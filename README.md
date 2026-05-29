# Solana MEV Bot — Pipeline

> **Rust · Jito Bundles · Yellowstone gRPC · Triangular Arbitrage · Liquidations · Sandwich Detection**

---

## Architecture

```
Yellowstone gRPC (Geyser)
  │  Account updates (pool reserves, obligation health)
  │  Pending transactions (pre-confirmation swap intents)
  ▼
Monitor ──────────────────── mpsc ─────────────────────► StrategyEngine
  • Decodes Raydium/Orca pool layouts                       • ArbitrageScanner
  • Decodes Kamino/Solend obligations                         Bellman-Ford on log-rate graph
  • Extracts pending swap intent                             • LiquidationScanner
                                                              LTV threshold monitor
                                                            • SandwichDetector
                                                              Slippage tolerance + toxic flow filter
                                                    mpsc
                                                    ▼
                                               Executor
                                               1. Build DEX instructions
                                               2. simulateTransaction (pre-flight)
                                               3. ComputeBudget (CU limit + priority fee)
                                               4. Jito Bundle (atomic, up to 5 txs)
                                               5. Submit → retry on blockhash stale
```

---

## Prerequisites

### Infrastructure (non-negotiable for production)

| Requirement | Why |
|---|---|
| **Dedicated validator / colocation** | Public RPCs add 50–200 ms latency. You need RTT < 10 ms to the block leader. Colocate in **Tokyo**, **US-East (Ashburn)**, or **Frankfurt** — where the majority of Solana stake is. |
| **Yellowstone Geyser** | Run the [Yellowstone gRPC plugin](https://github.com/rpcpool/yellowstone-grpc) on your validator or rent a Triton/Helius gRPC endpoint. This is how you see the "mempool". |
| **Jito-Solana client** | Your validator must run the [Jito-Solana fork](https://github.com/jito-foundation/jito-solana) to be eligible for block engine tip routing. |
| **QUIC transport** | Solana's default since 1.14. Ensure your firewall opens UDP on 8003/8004 and your RPC client uses QUIC (not HTTP fallback). |

---

## Configuration

Copy `.env.example` to `.env`:

```bash
# ── RPC ──────────────────────────────────────────────────────────────────────
RPC_URL=https://your-private-rpc.example.com
WS_URL=wss://your-private-rpc.example.com

# ── Yellowstone gRPC ─────────────────────────────────────────────────────────
GEYSER_URL=http://your-validator:10000
GEYSER_TOKEN=your-auth-token

# ── Jito ─────────────────────────────────────────────────────────────────────
JITO_URL=https://mainnet.block-engine.jito.labs.io/api/v1/bundles
JITO_TIP_LAMPORTS=500000      # 0.0005 SOL floor; raise to 1–5M under contention

# ── Keys ─────────────────────────────────────────────────────────────────────
KEYPAIR_PATH=/secure/path/to/keypair.json

# ── Strategy toggles ─────────────────────────────────────────────────────────
ENABLE_ARBITRAGE=true
ENABLE_LIQUIDATION=true
ENABLE_SANDWICH=false          # off by default — read the module docstring first

# ── Risk management ──────────────────────────────────────────────────────────
MIN_PROFIT_LAMPORTS=1000000   # 0.001 SOL minimum gross profit
MAX_TRADE_SOL=1.0
MAX_CU_PRICE=1000000          # 1 M micro-lamports ceiling on priority fee
SIMULATE=true                 # always simulate in production
```

---

## Build

```bash
# Release build with full LTO (takes ~2 min but produces fastest binary)
cargo build --release

# Run
RUST_LOG=info ./target/release/mev-bot
```

---

## Strategy Notes

### Triangular Arbitrage (`strategies/arbitrage.rs`)

1. A directed graph is built from all live `PoolState` entries.  Each pool yields two directed edges (A→B and B→A) weighted by `–ln(effective_rate)`.
2. Bellman-Ford detects negative-weight cycles, which correspond to product-of-rates > 1 (profit).
3. Optimal input size is found via ternary/binary search on the unimodal profit function `P(x) = route_output(x) – x`.
4. Only cycles with `net_profit > MIN_PROFIT_LAMPORTS` (after fee estimate) are executed.

**Key risk:** the pool state snapshot has a slot age.  If another bot's transaction settles between your simulation and your bundle landing, reserves shift and your transaction may revert.  Mitigate by (a) keeping your snapshot fresh (Geyser gives you sub-slot updates), and (b) always setting `min_amount_out` tightly.

### Liquidations (`strategies/liquidation.rs`)

- Tracks all Kamino/Solend/MarginFi obligations in `OBLIGATIONS` (DashMap).
- On every obligation update, computes current LTV.  If `LTV ≥ liquidation_threshold`, emit a signal.
- Kamino uses a *dynamic close factor* (100 % when LTV > 100 %, scaling down to 20 % near the threshold).  Getting the close factor right is critical — over-repaying wastes capital; under-repaying leaves profit on the table.
- Liquidation bonus varies: 5–15 % for Kamino (tier-dependent), 5 % for Solend.

### Sandwich (`strategies/sandwich.rs`)

⚠️ **Read the module docstring before enabling.**  This strategy degrades execution quality for ordinary users.  It is included here for completeness and research.

- Only targets swaps with `slippage_tolerance ≥ 50 bps`.
- Filters out large relative-impact swaps (> 0.5 % of pool reserves) as likely *toxic flow* (informed arbitrageurs who will move price adversely).
- Sizes the front-run to stay within `max_pool_impact_bps` (default 2 %) to avoid pushing price past the victim's `min_amount_out`.

---

## ComputeBudget Optimisation

Every transaction includes two prepended instructions:

```
SetComputeUnitLimit(simulated_units × 1.10)
SetComputeUnitPrice(P μLamports)
```

Where `P` is the 90th-percentile of `getRecentPrioritizationFees` for the accounts in the transaction.  This ensures:
- You pay only for the CUs you actually use (no over-allocation).
- Your fee is competitive without being wasteful.

---

## Backtesting with Geyser

Use the [Geyser plugin](https://docs.solanalabs.com/validator/geyser) to dump historical account/transaction data to PostgreSQL or a flat-file store, then replay it against your strategy engine:

```bash
# Clone historical slot range into parquet
solana-ledger-tool --ledger /mnt/ledger bigtable upload \
    --starting-slot 280000000 --ending-slot 280100000

# Point GEYSER_URL at your replay stream
GEYSER_URL=http://localhost:10000 REPLAY_MODE=true cargo run --release
```

---

## Security Checklist

- [ ] Keypair file is on an encrypted volume (LUKS / FileVault) with `chmod 600`.
- [ ] RPC endpoint is behind a VPN or private network (never expose publicly).
- [ ] `MIN_PROFIT_LAMPORTS` is set high enough to cover worst-case fee scenarios.
- [ ] `SIMULATE=true` in production — **never disable this**.
- [ ] Monitor your wallet balance; implement a circuit-breaker that halts the bot if balance drops below a threshold (protection against runaway retry loops).
- [ ] All Jito tip accounts are hardcoded and verified against the [official list](https://jito-labs.gitbook.io/mev/searcher-resources/tip-payment-program).
