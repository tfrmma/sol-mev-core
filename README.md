# sol-mev-core

Solana MEV bot: triangular arbitrage, Kamino/Solend liquidations, sandwich detection. Geyser (Yellowstone gRPC) for account/tx streaming, Jito bundles for atomic execution.

Status: **engineering-complete on the core paths.** Every strategy has real, source-verified logic behind it, not stubs. Read Known Limitations for the specific gaps.

## Why this stack

- **Yellowstone gRPC over polling RPC.** Polling `getProgramAccounts` on an interval means you're always trading on stale reserves. Geyser pushes account updates as they land.
- **Jito bundles over raw `sendTransaction`.** Bundles land atomically or not at all. Tip payment buys priority inclusion that fee bumping alone doesn't guarantee.
- **Bellman-Ford on a log-rate graph for arbitrage**, instead of just checking pairs. A negative cycle in `-ln(rate)` space is a profitable route regardless of hop count.
- **solana-sdk 2.x, not 1.18.** 1.18's published crates hard-pin `spl-token-2022 = "1.0.0"`, which pins `solana-program` to `1.17.6` through `solana-zk-token-sdk`. Baked into every 1.18.x release on crates.io, unpatchable without vendoring. 2.x doesn't have this problem.
- **`klend-interface` over hand-rolled Kamino accounts.** Kamino publishes an official, maintained Rust crate for exactly this. Rebuilding it by hand from an IDL is how the obligation-decoding bug this replaced happened in the first place.

## What's actually verified, not assumed

Every account layout and instruction format below was checked against the protocol's own published source or, where a project has no public source, explicitly flagged as unverified rather than guessed:

| Component | Source |
|---|---|
| Raydium `SwapBaseInV2` (8 accounts, tag 16) | `raydium-io/raydium-amm/program/src/instruction.rs` |
| Raydium AMM v4 authority PDA | Raydium's own program-addresses reference |
| Orca Whirlpool account layout + swap (11 accounts) | `orca-so/whirlpools/programs/whirlpool/src/state/whirlpool.rs` + `instructions/swap.rs` |
| Whirlpool tick array / oracle PDAs | Same repo, `initialize_tick_array.rs` |
| Orca legacy Token Swap (13 accounts, tag 1) | `solana-labs/solana-program-library/token-swap/program/src/instruction.rs` |
| Kamino obligation decode + liquidation | `klend-interface` (Kamino's own crate), cross-checked against Kamino's docs |
| Solend obligation layout (variable-length deposits/borrows) | `solana-labs/solana-program-library/token-lending/program/src/state/obligation.rs` |

Where this table doesn't cover something (Lifinity, Meteora), it's not supported, not guessed at. See below.

## Architecture

```
Yellowstone gRPC (Geyser)
  |  account updates: pool reserves, obligation health
  |  tx updates: pending swap intent
  v
Monitor ---------------------- mpsc ----------------------> StrategyEngine
  - decodes Raydium + Whirlpool pool layout                     - ArbitrageScanner (Bellman-Ford)
  - decodes Kamino obligations (klend-interface, zero-copy)      - LiquidationScanner (reactive + 20s full sweep)
  - decodes Solend obligations (variable-length, verified)       - SandwichDetector
  - extracts pending swaps from top-level + inner (CPI) ixs
                                                        mpsc
                                                        v
                                                   Executor
                                                   1. build DEX instructions (Raydium, Whirlpool,
                                                      Orca legacy: done. Lifinity, Meteora: not supported)
                                                   2. simulateTransaction (preflight)
                                                   3. risk-adjust liquidation exits against live reserve data
                                                   4. ComputeBudget (CU limit + priority fee)
                                                   5. Jito bundle
                                                   6. submit, retry on stale blockhash
```

## Known limitations

- **CLMM quoting refuses trades that would exceed the currently visible tick range**, rather than mis-quoting them. `quote_clmm` caps price impact at 2% and returns 0 past that, since real tick-crossing needs decoding `TickArray` accounts this doesn't fetch. Correct behavior, just conservative: some real opportunities on thin Whirlpools will get skipped rather than sized wrong.
- **Lifinity isn't supported.** No official public source exists for its program; the only account-layout reference found was third-party transaction-parser inference, not held to the same bar as everything in the table above.
- **Meteora isn't supported.** It's three different pool models (DAMM v1, DAMM v2, DLMM) with genuinely different account structures, and `registry.rs` doesn't even distinguish which one a given `ProgramKind::AmmMeteora` entry refers to yet. DLMM's math specifically is complex enough that it's the subject of a dedicated from-scratch porting effort elsewhere in the ecosystem.
- **`registry.json` needs real pools.** `scripts/populate_registry.py` decodes real pool accounts over RPC and ships with one verified seed pool (Raydium SOL/USDC). Arbitrage and sandwich detection are only as good as what's in there.
- **Liquidation bonus sizing assumes a single dominant reserve per side.** `top_deposit_reserve`/`top_borrow_reserve` picks the largest position on each side of an obligation rather than optimizing across all of them. Correct for the common case, not optimal for obligations with several sizeable positions.

## What's next

1. Populate `registry.json` with the pools you actually want to trade.
2. Tick-array-aware CLMM quoting, if the 2% cap turns out to be leaving real money on the table.
3. Lifinity, if and when a real source (official repo or IDL) surfaces.
4. Meteora, starting with whichever of the three variants is actually needed.

## Prerequisites

### Infrastructure

| Requirement | Why |
|---|---|
| Dedicated validator or colocation | Public RPCs add 50-200ms of latency. Colocate near where stake concentrates (Tokyo, US-East, Frankfurt). |
| Yellowstone Geyser | Run the [plugin](https://github.com/rpcpool/yellowstone-grpc) on your own validator, or rent a Triton/Helius endpoint. |
| Jito-Solana validator | Needed for block engine tip routing. [Jito-Solana fork](https://github.com/jito-foundation/jito-solana). |
| Rust >= 1.85, via `rustup` | solana-sdk 2.x pulls transitive deps requiring `edition2024`. Your distro's packaged Rust is almost certainly too old. |
| Linux or WSL2, not native Windows | `yellowstone-grpc-client` unconditionally imports `tokio::net::UnixStream`, unavailable on native Windows with no feature flag to disable it. `wsl --install` if needed. |

## Configuration

Copy `.env.example` to `.env`:

```bash
RPC_URL=https://your-private-rpc.example.com
WS_URL=wss://your-private-rpc.example.com

GEYSER_URL=http://your-validator:10000
GEYSER_TOKEN=your-auth-token

JITO_URL=https://mainnet.block-engine.jito.labs.io/api/v1/bundles
JITO_TIP_LAMPORTS=500000

KEYPAIR_PATH=/secure/path/to/keypair.json

ENABLE_ARBITRAGE=true
ENABLE_LIQUIDATION=true
ENABLE_SANDWICH=false          # works end to end, hasn't run against live traffic yet

MIN_PROFIT_LAMPORTS=1000000
MAX_TRADE_SOL=1.0
MAX_CU_PRICE=1000000
SIMULATE=true                  # keep this on until you've watched a full session of correct behavior
```

### Populating `registry.json`

```bash
pip install -r scripts/requirements.txt
python3 scripts/populate_registry.py <YOUR_RPC_URL> > registry.json
```

Add pool addresses to `POOL_IDS` in the script as you find them. It decodes accounts directly over RPC using the same byte offsets this codebase already trusts, no third-party API involved.

## Build

```bash
cargo build --release
cargo test              # pure-math unit tests, no RPC needed
RUST_LOG=info ./target/release/mev-bot
```

## Strategy notes

### Triangular arbitrage (`strategies/arbitrage.rs`)

Bellman-Ford over a directed graph built from every live `PoolState`, weighted by `-ln(effective_rate)`. A negative cycle is a profitable loop regardless of hop count. Cycles get rotated to start at whatever mint you actually hold; a mathematically valid cycle that never touches it gets discarded, since you can't fund an arb with a currency you don't have. Stale pool snapshots are filtered before scoring. Optimal input size comes from a binary search over the profit function.

### Liquidations (`strategies/liquidation.rs` + `executor.rs`)

Two paths feed the same executor: reactive (fires on the specific obligation that just updated) and a 20-second full sweep (catches positions that were already underwater without a fresh update). Kamino obligations decode through `klend-interface`'s zero-copy parser, giving real deposit/borrow reserve pubkeys. At execution time, `executor.rs` fetches live reserve data, checks the exit trade's slippage against the actual pool you'd sell seized collateral into, and aborts rather than sending a transaction the risk engine can't justify.

### Sandwich (`strategies/sandwich.rs`)

Not enabled by default. `extract_pending_swap` decodes Raydium `SwapBaseInV2` from both top-level and inner (CPI) instructions, since most retail volume routes through an aggregator. Direction resolves by comparing the swapper's source token account against their derived ATA, no extra RPC call. "Pending" here means Geyser's `processed`-commitment stream, Solana doesn't have a literal mempool; this is fast-following within the same or next slot via Jito, not classic frontrunning.

## ComputeBudget

Every transaction gets a compute unit limit set to simulated usage plus 10% headroom, and a priority fee set to the 90th percentile of `getRecentPrioritizationFees` for the accounts involved.

## Security checklist

- [ ] Keypair file on an encrypted volume, `chmod 600`.
- [ ] RPC endpoint behind a VPN or private network, never public.
- [ ] `MIN_PROFIT_LAMPORTS` set high enough to survive worst-case fee scenarios.
- [ ] `SIMULATE=true` until you've watched a full session behave correctly with it on.
- [ ] Wallet balance monitored with a circuit breaker that halts the bot below a threshold.
- [ ] Jito tip accounts verified against the [official list](https://jito-labs.gitbook.io/mev/searcher-resources/tip-payment-program).
- [ ] `ENABLE_SANDWICH` stays off until it's run against live traffic and you've reviewed the output.
