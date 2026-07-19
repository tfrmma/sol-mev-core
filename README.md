# sol-mev-core

Solana MEV bot: triangular arbitrage, Kamino/Solend liquidations, sandwich detection. Geyser (Yellowstone gRPC) for account/tx streaming, Jito bundles for atomic execution.

## Why this stack

- **Yellowstone gRPC over polling RPC.** Polling `getProgramAccounts` on an interval means you're always trading on stale reserves. Geyser pushes account updates as they land, which is the difference between reacting to a price move and reacting to last block's price move.
- **Jito bundles over raw `sendTransaction`.** Bundles land atomically or not at all, so a failed frontrun leg can't leave you holding a half-executed sandwich. Tip payment also buys priority inclusion that plain fee bumping doesn't guarantee.
- **Bellman-Ford on a log-rate graph for arbitrage**, instead of just checking pairs. A negative cycle in `-ln(rate)` space is a profitable route regardless of how many hops it takes. Two-hop-only scanning misses most real opportunities.
- **solana-sdk 2.x, not 1.18.** 1.18's published crates hard-pin `spl-token-2022 = "1.0.0"`, which itself pins `solana-program` to `1.17.6` through `solana-zk-token-sdk`. That's baked into every 1.18.x release on crates.io and can't be patched away without vendoring a crate by hand. 2.x doesn't have this problem.

## Architecture

```
Yellowstone gRPC (Geyser)
  |  account updates: pool reserves, obligation health
  |  tx updates: pending swap intent (pre-confirmation)
  v
Monitor ---------------------- mpsc ----------------------> StrategyEngine
  - decodes Raydium pool layout                                - ArbitrageScanner (Bellman-Ford)
  - decodes Kamino/Solend obligations                           - LiquidationScanner (LTV monitor)
  - extracts pending swap intent (WIP, see limitations)         - SandwichDetector
                                                        mpsc
                                                        v
                                                   Executor
                                                   1. build DEX instructions
                                                   2. simulateTransaction (preflight)
                                                   3. ComputeBudget (CU limit + priority fee)
                                                   4. Jito bundle
                                                   5. submit, retry on stale blockhash
```

## Known limitations

Things that will silently do nothing, or worse, fail on-chain, if you run this as-is. Ordered by how much it matters.

- **Swap instruction builders (`executor.rs`) build empty account lists.** `raydium_ix`, `orca_ix`, `whirlpool_ix` construct the discriminant and data correctly but pass `accounts: vec![]`. Every one of these will fail on-chain right now. The plumbing from `PoolMeta` (vaults, authority, extra accounts) to `AccountMeta` isn't wired up yet.
- **`build_kamino_liquidation_ix` is missing accounts too** (obligation, reserve, oracle). Tracked as issue #47. Same story as above, different strategy.
- **Sandwich detection never fires.** `extract_pending_swap()` in `monitor.rs` always returns `None`. The decoder for pending swap intent from Geyser tx updates isn't implemented. `SandwichDetector` itself is complete and correct, it just never receives anything to evaluate.
- **Orca and Whirlpool pools are invisible to the arb graph.** `decode_pool()` only handles Raydium. Both programs are enabled in the default registry, but their account layouts aren't decoded, so they never become graph edges.
- **`registry.json` starts empty.** It auto-generates on first run with zero pools. You need to populate it with real pool addresses before the bot has anything to scan.
- **Kamino/Solend obligation offsets are unverified.** `owner` (`data[8..40]`) and `collateral_value` (`data[32..48]`) overlap in `decode_obligation`. Might be correct for a packed layout I haven't confirmed, might be a copied offset that's wrong. Verify against `solana account <obligation> --output json` before trusting any LTV number this produces.
- **No tests.** `state.rs` (quote math, LTV) and `risk.rs` (haircut, EWMA) don't touch RPC or the network and are the cheapest place to start.

## What's next

Roughly in the order I'm tackling it:

1. Confirm `solana-sdk` 2.x migration compiles clean end to end.
2. Pending swap extraction for sandwich detection, decoded against real Jupiter/Raydium instruction data.
3. Orca/Whirlpool pool decoding against the published Anchor IDL.
4. Wire real `AccountMeta` lists into the swap and liquidation instruction builders.
5. Verify Kamino/Solend obligation offsets against live account data.
6. Basic test coverage for the pure-math modules.

## Prerequisites

### Infrastructure

| Requirement | Why |
|---|---|
| Dedicated validator or colocation | Public RPCs add 50-200ms of latency. You want RTT under 10ms to the block leader. Colocate near where Solana stake actually concentrates (Tokyo, US-East, Frankfurt). |
| Yellowstone Geyser | Run the [plugin](https://github.com/rpcpool/yellowstone-grpc) on your own validator, or rent a Triton/Helius gRPC endpoint. This is how you see pending activity before it confirms. |
| Jito-Solana validator | Needed for block engine tip routing. See the [Jito-Solana fork](https://github.com/jito-foundation/jito-solana). |
| Rust >= 1.85 | solana-sdk 2.x pulls transitive deps that require `edition2024`. Older toolchains will fail to even resolve the dependency graph, not just compile. Use `rustup`, not your distro's packaged Rust. |
| Perl (Windows only) | `openssl` builds vendored from source on Windows (`solana-secp256r1-program` pulls it transitively, unavoidable). Needs Perl and a C compiler. `winget install StrawberryPerl.StrawberryPerl` if you don't have one. |

## Configuration

Copy `.env.example` to `.env`:

```bash
# RPC
RPC_URL=https://your-private-rpc.example.com
WS_URL=wss://your-private-rpc.example.com

# Yellowstone gRPC
GEYSER_URL=http://your-validator:10000
GEYSER_TOKEN=your-auth-token

# Jito
JITO_URL=https://mainnet.block-engine.jito.labs.io/api/v1/bundles
JITO_TIP_LAMPORTS=500000      # 0.0005 SOL floor, raise to 1-5M under contention

# keys
KEYPAIR_PATH=/secure/path/to/keypair.json

# strategy toggles
ENABLE_ARBITRAGE=true
ENABLE_LIQUIDATION=true
ENABLE_SANDWICH=false          # leave off until extract_pending_swap actually works

# risk
MIN_PROFIT_LAMPORTS=1000000    # 0.001 SOL minimum gross profit
MAX_TRADE_SOL=1.0
MAX_CU_PRICE=1000000           # 1M microlamports ceiling on priority fee
SIMULATE=true                  # keep this on, always
```

## Build

```bash
cargo build --release
RUST_LOG=info ./target/release/mev-bot
```

First build compiles OpenSSL from source on Windows (vendored feature), so it'll be slower than you expect. Subsequent builds are cached.

## Strategy notes

### Triangular arbitrage (`strategies/arbitrage.rs`)

A directed graph is built from every live `PoolState`. Each pool contributes two edges, A to B and B to A, weighted by `-ln(effective_rate)`. Bellman-Ford finds negative-weight cycles, which correspond to a product-of-rates greater than one, i.e. a profitable loop. Optimal input size comes from a binary search over the (concave, for a CPMM) profit function. Only cycles clearing `MIN_PROFIT_LAMPORTS` after estimated fees get executed.

Main risk: the pool snapshot has slot age. If someone else's swap lands between your simulation and your bundle confirming, reserves have moved and your transaction may revert. Keep `min_amount_out` tight and don't trust a stale snapshot.

### Liquidations (`strategies/liquidation.rs`)

Tracks Kamino/Solend/MarginFi obligations in a shared map. On every update, computes current LTV; crosses `liquidation_threshold` and it emits a signal. Kamino uses a dynamic close factor (up to 100% when LTV exceeds 100%, scaling down near the threshold), getting this wrong either burns capital overclosing or leaves profit on the table underclosing. Bonus varies by protocol: 5-15% tiered for Kamino, flat 5% for Solend.

### Sandwich (`strategies/sandwich.rs`)

Not enabled by default, and shouldn't be until pending swap extraction actually works (see limitations above). Filters for `slippage_tolerance >= 50bps`, skips anything with relative price impact over 0.5% of pool reserves (likely informed flow, not a naive retail swap), and sizes the frontrun to stay under `max_pool_impact_bps` so it doesn't push price past the victim's own `min_amount_out`.

## ComputeBudget

Every transaction gets two prepended instructions: a compute unit limit set to simulated usage plus 10% headroom, and a priority fee set to the 90th percentile of `getRecentPrioritizationFees` for the accounts involved. Pay for what you use, stay competitive without overpaying.

## Security checklist

- [ ] Keypair file on an encrypted volume, `chmod 600`.
- [ ] RPC endpoint behind a VPN or private network, never public.
- [ ] `MIN_PROFIT_LAMPORTS` set high enough to survive worst-case fee scenarios.
- [ ] `SIMULATE=true` in production, no exceptions.
- [ ] Wallet balance monitored with a circuit breaker that halts the bot below a threshold (protects against a runaway retry loop draining funds).
- [ ] Jito tip accounts verified against the [official list](https://jito-labs.gitbook.io/mev/searcher-resources/tip-payment-program).
