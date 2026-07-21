# sol-mev-core

Solana MEV bot: triangular arbitrage, Kamino/Solend liquidations, sandwich detection. Geyser (Yellowstone gRPC) for account/tx streaming, Jito bundles for atomic execution.

Status: **not production ready, but no longer a skeleton.** Every strategy has real logic behind it now. What's left is verification against live data and a couple of protocols (Solend offsets, Orca legacy, Lifinity/Meteora) that haven't gotten the same rigor as the rest. Read Known Limitations before trusting this with real capital.

## Why this stack

- **Yellowstone gRPC over polling RPC.** Polling `getProgramAccounts` on an interval means you're always trading on stale reserves. Geyser pushes account updates as they land, which is the difference between reacting to a price move and reacting to last block's price move.
- **Jito bundles over raw `sendTransaction`.** Bundles land atomically or not at all, so a failed frontrun leg can't leave you holding a half-executed sandwich. Tip payment also buys priority inclusion that plain fee bumping doesn't guarantee.
- **Bellman-Ford on a log-rate graph for arbitrage**, instead of just checking pairs. A negative cycle in `-ln(rate)` space is a profitable route regardless of how many hops it takes. Two-hop-only scanning misses most real opportunities.
- **solana-sdk 2.x, not 1.18.** 1.18's published crates hard-pin `spl-token-2022 = "1.0.0"`, which itself pins `solana-program` to `1.17.6` through `solana-zk-token-sdk`. That's baked into every 1.18.x release on crates.io and can't be patched away without vendoring a crate by hand. 2.x doesn't have this problem.
- **`klend-interface` over hand-rolled Kamino accounts.** Kamino publishes an official, maintained Rust crate for exactly this (no anchor-lang dependency, zero-copy account parsing, typed instruction builders). Rebuilding that by hand from an IDL is how you end up with subtly wrong obligation offsets, which is exactly the bug this replaced.

## Architecture

```
Yellowstone gRPC (Geyser)
  |  account updates: pool reserves, obligation health
  |  tx updates: pending swap intent
  v
Monitor ---------------------- mpsc ----------------------> StrategyEngine
  - decodes Raydium + Whirlpool pool layout                     - ArbitrageScanner (Bellman-Ford)
  - decodes Kamino obligations (klend-interface, zero-copy)      - LiquidationScanner (LTV monitor)
  - decodes Solend obligations (offsets unverified, see below)   - SandwichDetector
  - extracts pending swaps from top-level + inner (CPI) ixs
                                                        mpsc
                                                        v
                                                   Executor
                                                   1. build DEX instructions (Raydium, Whirlpool done;
                                                      Orca legacy, Lifinity, Meteora pending)
                                                   2. simulateTransaction (preflight)
                                                   3. ComputeBudget (CU limit + priority fee)
                                                   4. Jito bundle
                                                   5. submit, retry on stale blockhash
```

## Known limitations

Ordered by how much it matters. Shorter list than it used to be, read it anyway.

- **Orca legacy swap (Token Swap Program) isn't wired up.** `orca_ix` returns an error instead of a transaction. Whirlpool has mostly displaced its volume, so this is lower priority than it looks.
- **Lifinity and Meteora aren't supported.** `registry.rs` maps them to `Dex::Lifinity`/`Dex::Meteora`, but there's no ix builder for either.
- **CLMM quoting doesn't handle tick crossing.** `quote_clmm` in `state.rs` is correct for swaps that stay within the pool's currently active tick range. A trade big enough to cross into the next initialized tick will get worse execution than the quote predicted, real tick-array walking isn't implemented.
- **Liquidation risk-adjustment isn't computed.** `build_opportunity` in `liquidation.rs` used to (incorrectly) estimate exit slippage against a placeholder mint. It now just doesn't estimate it at all, since the real collateral mint isn't known without an extra RPC round trip this sync scanner doesn't make. Less precise than before, no longer silently wrong.
- **`registry.json` starts empty.** `scripts/populate_registry.py` fills it in by decoding real pool accounts directly over RPC (same byte offsets already trusted in `monitor.rs`), seeded with one verified pool. Add more pool addresses to the script's `POOL_IDS` list as you find them.

## What's next

1. Orca legacy swap instruction, verified against `solana-program-library`'s `token-swap` source the same way Raydium and Whirlpool were.
2. Tick-array-aware CLMM quoting, for trade sizes that matter enough to cross a tick boundary.
3. Lifinity and Meteora ix builders.
4. More test coverage as strategies stabilize.

## Prerequisites

### Infrastructure

| Requirement | Why |
|---|---|
| Dedicated validator or colocation | Public RPCs add 50-200ms of latency. You want RTT under 10ms to the block leader. Colocate near where Solana stake actually concentrates (Tokyo, US-East, Frankfurt). |
| Yellowstone Geyser | Run the [plugin](https://github.com/rpcpool/yellowstone-grpc) on your own validator, or rent a Triton/Helius gRPC endpoint. This is how you see pending activity before it confirms. |
| Jito-Solana validator | Needed for block engine tip routing. See the [Jito-Solana fork](https://github.com/jito-foundation/jito-solana). |
| Rust >= 1.85, via `rustup` | solana-sdk 2.x pulls transitive deps that require `edition2024`. Your distro's packaged Rust (`apt install cargo`) is almost certainly too old and will fail to even resolve the dependency graph, not just compile. |
| Linux or WSL2, not native Windows | `yellowstone-grpc-client` unconditionally imports `tokio::net::UnixStream`, which doesn't exist on native Windows and currently has no feature flag to disable. If you're on Windows, install WSL2 (`wsl --install`) and build inside it. |
| Perl + a C compiler (native Windows only) | Not needed on Linux/WSL2 if you have `libssl-dev` installed. Only relevant if you insist on native Windows: `openssl` builds vendored from source there since `solana-secp256r1-program` pulls it in transitively either way. |

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
ENABLE_SANDWICH=false          # extract_pending_swap works now, but this hasn't run against live traffic yet

# risk
MIN_PROFIT_LAMPORTS=1000000    # 0.001 SOL minimum gross profit
MAX_TRADE_SOL=1.0
MAX_CU_PRICE=1000000           # 1M microlamports ceiling on priority fee
SIMULATE=true                  # keep this on, always
```

### Populating `registry.json`

```bash
pip install -r scripts/requirements.txt
python3 scripts/populate_registry.py <YOUR_RPC_URL> > registry.json
```

Add more pool addresses to `POOL_IDS` in the script as you find them (Solscan, DexScreener, or the Raydium/Orca UI all show a pool's address directly). The script decodes accounts over RPC using the same offsets `monitor.rs` already trusts, no third-party API involved.

## Build

```bash
cargo build --release
RUST_LOG=info ./target/release/mev-bot
```

## Strategy notes

### Triangular arbitrage (`strategies/arbitrage.rs`)

A directed graph is built from every live `PoolState`. Each pool contributes two edges, A to B and B to A, weighted by `-ln(effective_rate)`. Bellman-Ford finds negative-weight cycles, which correspond to a product-of-rates greater than one, a profitable loop. The cycle is rotated to start at whatever mint you actually hold (SOL by default), a mathematically valid negative cycle that never touches it isn't executable and gets discarded. Optimal input size comes from a binary search over the profit function.

Main risk: the pool snapshot has slot age. If someone else's swap lands between your simulation and your bundle confirming, reserves have moved and your transaction may revert. Keep `min_amount_out` tight and don't trust a stale snapshot.

### Liquidations (`strategies/liquidation.rs`)

Tracks Kamino/Solend obligations in a shared map. Kamino obligations decode through `klend-interface`'s zero-copy `state::Obligation`, giving real deposit/borrow reserve pubkeys instead of guessed offsets. On a threshold cross, the scanner picks the largest deposit and largest borrow reserve (not necessarily optimal across multiple positions, a deliberate simplification) and hands off to `executor.rs`, which fetches live reserve data and calls `klend-interface`'s `ObligationContext::liquidate(...)` directly, refresh instructions included automatically.

Kamino uses a dynamic close factor (up to 100% when LTV exceeds 100%, scaling down near the threshold), getting this wrong either burns capital overclosing or leaves profit on the table underclosing.

### Sandwich (`strategies/sandwich.rs`)

Not enabled by default. `extract_pending_swap` in `monitor.rs` decodes Raydium `SwapBaseInV2` from both top-level and inner (CPI) instructions, since most retail volume routes through an aggregator that CPIs into the AMM rather than calling it directly. Direction is resolved by comparing the swapper's actual source token account against their derived ATA for each candidate mint, no extra RPC call needed. Note that "pending" here means Geyser's `processed`-commitment stream, not a literal mempool, Solana doesn't have one. This is fast-following within the same or next slot via Jito, not classic frontrunning.

Filters for `slippage_tolerance >= 50bps`, skips anything with relative price impact over 0.5% of pool reserves (likely informed flow), and sizes the frontrun to stay under `max_pool_impact_bps`.

## ComputeBudget

Every transaction gets two prepended instructions: a compute unit limit set to simulated usage plus 10% headroom, and a priority fee set to the 90th percentile of `getRecentPrioritizationFees` for the accounts involved.

## Security checklist

- [ ] Keypair file on an encrypted volume, `chmod 600`.
- [ ] RPC endpoint behind a VPN or private network, never public.
- [ ] `MIN_PROFIT_LAMPORTS` set high enough to survive worst-case fee scenarios.
- [ ] `SIMULATE=true` in production, no exceptions.
- [ ] Wallet balance monitored with a circuit breaker that halts the bot below a threshold.
- [ ] Jito tip accounts verified against the [official list](https://jito-labs.gitbook.io/mev/searcher-resources/tip-payment-program).
- [ ] `ENABLE_SANDWICH` left off until it's been run against live traffic and you've reviewed what it actually does.
