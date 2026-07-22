// triangular arb via bellman-ford on the log-rate graph.
// negative cycle = profitable route. classic. works on any CPMM topology.
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use tracing::info;

use crate::config::BotConfig;
use crate::state::{PoolState, CURRENT_SLOT, POOLS};

const PROBE_UNIT: u64    = 1_000_000;  // 1 USDC-sized probe. rate is dimensionless anyway
const INPUT_FLOOR: u64   = 1_000_000;
const INPUT_CAP: u64     = 10_000_000_000; // 10k USDC ceiling. above this slippage eats you alive
const SIZE_STEP: u64     = 1_000;
// per-hop cost. 3 ixs + some overhead, rough but consistent
const FEE_PER_HOP: u64  = 15_000;

// `pool` duplicates `pool_state.pool_id`, kept as a quick accessor without going through
// the full PoolState when you just need the address (e.g. logging).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ArbEdge {
    pub pool:       Pubkey,
    pub from_mint:  Pubkey,
    pub to_mint:    Pubkey,
    pub rate:       f64,
    pub pool_state: PoolState,
}

// gross_output is diagnostic (pre-fee output, useful when comparing against net_profit_lamports
// in a log line), input_mint is redundant with edges.first().from_mint, see the debug_assert
// in executor.rs::build_arb_ixs that keeps the two in sync.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ArbPath {
    pub edges:               Vec<ArbEdge>,
    pub input_mint:          Pubkey,
    pub optimal_input:       u64,
    pub gross_output:        u64,
    pub net_profit_lamports: i64,
}

pub struct ArbitrageScanner {
    min_profit: u64,
    sol_mint:   Pubkey,
}

impl ArbitrageScanner {
    pub fn new(config: &BotConfig) -> Self {
        Self {
            min_profit: config.min_profit_lamports,
            sol_mint: "So11111111111111111111111111111111111111112".parse().unwrap(),
        }
    }

    pub fn scan(&self, _updated_pool: Pubkey) -> Option<ArbPath> {
        // TODO: only rerun edges connected to _updated_pool instead of full graph rebuild.
        //       right now we're doing O(pools) work on every update which is fine at <500 pools
        //       but will hurt at scale. incremental bellman-ford is annoying to implement correctly.
        let edges = self.build_edges();
        if edges.is_empty() { return None; }

        let cycles = self.bellman_ford(&edges);
        let best = cycles.into_iter()
            .filter_map(|c| self.evaluate_cycle(c))
            .filter(|p| p.net_profit_lamports > self.min_profit as i64)
            .max_by_key(|p| p.net_profit_lamports);

        if let Some(ref p) = best {
            info!("arb found: {} hops net={}", p.edges.len(), p.net_profit_lamports);
        }
        best
    }

    fn build_edges(&self) -> Vec<ArbEdge> {
        let mut edges = Vec::with_capacity(POOLS.len() * 2);
        let current_slot = CURRENT_SLOT.load(std::sync::atomic::Ordering::Relaxed);
        POOLS.for_each(|_, pool| {
            if pool.is_stale(current_slot) { return; } // was never checked before, stale reserves = bad quotes
            // reserve_a/reserve_b are 0 by design for CLMM pools (see ClmmState in state.rs),
            // that's not "no liquidity", check clmm.liquidity instead of the constant-product fields.
            let has_liquidity = match &pool.clmm {
                Some(c) => c.liquidity > 0,
                None    => pool.reserve_a != 0 && pool.reserve_b != 0,
            };
            if !has_liquidity { return; }
            let rate_ab = pool.quote_a_to_b(PROBE_UNIT) as f64 / PROBE_UNIT as f64;
            let rate_ba = pool.quote_b_to_a(PROBE_UNIT) as f64 / PROBE_UNIT as f64;
            edges.push(ArbEdge { pool: pool.pool_id, from_mint: pool.token_a_mint, to_mint: pool.token_b_mint, rate: rate_ab, pool_state: pool.clone() });
            edges.push(ArbEdge { pool: pool.pool_id, from_mint: pool.token_b_mint, to_mint: pool.token_a_mint, rate: rate_ba, pool_state: pool.clone() });
        });
        edges
    }

    // standard bellman-ford over -ln(rate) weights.
    // negative cycle ↔ product-of-rates > 1 ↔ free money (modulo fees and latency).
    fn bellman_ford(&self, edges: &[ArbEdge]) -> Vec<Vec<ArbEdge>> {
        let mints: Vec<Pubkey> = {
            let mut v: Vec<Pubkey> = edges.iter().flat_map(|e| [e.from_mint, e.to_mint]).collect();
            v.sort_unstable();
            v.dedup();
            v
        };
        let n   = mints.len();
        let idx: HashMap<Pubkey, usize> = mints.iter().enumerate().map(|(i, p)| (*p, i)).collect();

        // multiple pools can connect the same pair (e.g. two raydium pools for the same mints).
        // keep only the best-rate edge per (u, v), that's the one bellman-ford would actually pick,
        // and it keeps the pred[] walk unambiguous during cycle reconstruction below.
        let mut best_edge: HashMap<(usize, usize), &ArbEdge> = HashMap::new();
        for e in edges {
            let (u, v) = (idx[&e.from_mint], idx[&e.to_mint]);
            let w = -(e.rate.max(1e-15).ln());
            best_edge.entry((u, v))
                .and_modify(|cur| if w < -(cur.rate.max(1e-15).ln()) { *cur = e })
                .or_insert(e);
        }

        let mut dist = vec![f64::INFINITY; n];
        let mut pred: Vec<Option<usize>> = vec![None; n];
        let start = idx.get(&self.sol_mint).copied().unwrap_or(0);
        dist[start] = 0.0;

        for _ in 0..n.saturating_sub(1) {
            for (&(u, v), e) in &best_edge {
                if dist[u].is_infinite() { continue; }
                let w = -(e.rate.max(1e-15).ln());
                if dist[u] + w < dist[v] {
                    dist[v] = dist[u] + w;
                    pred[v] = Some(u);
                }
            }
        }

        // nth pass: anything that still relaxes is inside (or downstream of) a negative cycle.
        // TODO: this grabs the first one it finds and stops. there can be several live negative
        //       cycles at once on a busy graph, worth returning all of them eventually, but one
        //       profitable route per scan is enough for now and keeps this simple.
        let mut relaxed_node = None;
        for (&(u, v), e) in &best_edge {
            if dist[u].is_infinite() { continue; }
            let w = -(e.rate.max(1e-15).ln());
            if dist[u] + w < dist[v] - 1e-12 {
                pred[v] = Some(u);
                relaxed_node = Some(v);
                break;
            }
        }

        let Some(mut node) = relaxed_node else { return vec![] };

        // walk back n steps first, guarantees we land *inside* the cycle instead of somewhere
        // upstream of it that merely feeds into it.
        for _ in 0..n {
            node = match pred[node] {
                Some(p) => p,
                None => return vec![],
            };
        }

        // now trace the actual loop back to itself
        let cycle_head = node;
        let mut nodes = vec![cycle_head];
        let mut cur = match pred[cycle_head] { Some(p) => p, None => return vec![] };
        let mut guard = 0;
        while cur != cycle_head {
            nodes.push(cur);
            cur = match pred[cur] { Some(p) => p, None => return vec![] };
            guard += 1;
            if guard > n { return vec![]; } // defensive, shouldn't happen if pred[] is a real cycle
        }
        // pred[] points backward, so the collected order is reversed relative to actual swap order
        nodes.reverse();

        // stitch node indices back into the real edges (and pools) that connect them
        let mut cycle_edges = Vec::with_capacity(nodes.len());
        for w in nodes.windows(2) {
            match best_edge.get(&(w[0], w[1])) {
                Some(e) => cycle_edges.push((*e).clone()),
                None => return vec![],
            }
        }
        let (last, first) = (*nodes.last().unwrap(), nodes[0]);
        match best_edge.get(&(last, first)) {
            Some(e) => cycle_edges.push((*e).clone()),
            None => return vec![],
        }

        // rotate so the cycle starts at sol_mint, we can only actually fund the arb with
        // a currency we hold, a cycle that never touches it isn't executable today.
        let Some(rotate_at) = cycle_edges.iter().position(|e| e.from_mint == self.sol_mint) else {
            return vec![]; // valid negative cycle, but doesn't route through anything we hold
        };
        cycle_edges.rotate_left(rotate_at);

        vec![cycle_edges]
    }

    fn evaluate_cycle(&self, cycle: Vec<ArbEdge>) -> Option<ArbPath> {
        if cycle.len() < 2 { return None; }
        let input_mint = cycle.first()?.from_mint;

        let optimal = self.binary_search_optimal_input(&cycle);
        let gross   = self.route_output(&cycle, optimal);
        let fees    = cycle.len() as u64 * FEE_PER_HOP;
        let net     = gross as i64 - optimal as i64 - fees as i64;

        if net <= 0 { return None; }
        Some(ArbPath { edges: cycle, input_mint, optimal_input: optimal, gross_output: gross, net_profit_lamports: net })
    }

    // profit function is unimodal for CPMMs (concave), so bisect works fine.
    // ternary search would be more correct but binary is fast enough and the
    // difference in found optimum is negligible at SIZE_STEP resolution.
    fn binary_search_optimal_input(&self, cycle: &[ArbEdge]) -> u64 {
        let (mut lo, mut hi) = (INPUT_FLOOR, INPUT_CAP);
        while hi - lo > SIZE_STEP {
            let mid = (lo + hi) / 2;
            if self.route_output(cycle, mid + SIZE_STEP) > self.route_output(cycle, mid) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        (lo + hi) / 2
    }

    fn route_output(&self, cycle: &[ArbEdge], amount_in: u64) -> u64 {
        let mut amount = amount_in;
        for edge in cycle {
            amount = if edge.from_mint == edge.pool_state.token_a_mint {
                edge.pool_state.quote_a_to_b(amount)
            } else {
                edge.pool_state.quote_b_to_a(amount)
            };
            if amount == 0 { return 0; }
        }
        amount
    }
}
