// toxic flow filter. classifies wallets that are likely informed / arb bots.
// we don't want to sandwich these — they'll eat us alive.
//
// scoring is multi-signal: success rate, cross-pool activity, CU usage, timing, arb program hits.
// thresholds are hand-tuned on historical data. don't change them without backtesting.
use ahash::AHashMap;
use solana_sdk::pubkey::Pubkey;
use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tracing::debug;

const PROFILE_WINDOW: Duration = Duration::from_secs(60); // rolling 60s window
const MAX_PROFILES: usize      = 50_000;
const SM_THRESHOLD: f64        = 0.65;
const MIN_OBS: u32             = 3; // need at least 3 txs before trusting the score

// jupiter v4 + v6. expand this list if new aggregators start showing up.
const ARB_PROGRAMS: &[&str] = &[
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
];

#[derive(Debug, Clone)]
struct Profile {
    address:       Pubkey,
    tx_count:      u32,
    success_count: u32,
    unique_pools:  u32,
    total_cu:      u64,
    first_mover:   u32,
    arb_hits:      u32,
    last_seen:     Instant,
    seen_pools:    Vec<Pubkey>, // small enough that linear scan beats hashset
}

impl Profile {
    fn new(address: Pubkey) -> Self {
        Self {
            address, tx_count: 0, success_count: 0, unique_pools: 0,
            total_cu: 0, first_mover: 0, arb_hits: 0,
            last_seen: Instant::now(), seen_pools: Vec::with_capacity(8),
        }
    }

    fn record(&mut self, success: bool, pool: Pubkey, cu: u32, is_first: bool, is_arb: bool) {
        self.tx_count += 1;
        self.total_cu += cu as u64;
        self.last_seen = Instant::now();
        if success  { self.success_count += 1; }
        if is_first { self.first_mover   += 1; }
        if is_arb   { self.arb_hits      += 1; }
        if !self.seen_pools.contains(&pool) {
            self.seen_pools.push(pool);
            self.unique_pools += 1;
        }
    }

    // weighted scoring. weights were derived empirically from labeled flow data.
    // if you touch these and break the F1, that's on you.
    fn score(&self) -> f64 {
        if self.tx_count == 0 { return 0.0; }

        let success_rate     = self.success_count as f64 / self.tx_count as f64;
        let avg_cu           = self.total_cu as f64 / self.tx_count as f64;
        let first_mover_rate = self.first_mover as f64 / self.tx_count as f64;
        let arb_rate         = self.arb_hits as f64 / self.tx_count as f64;

        // high success rate = knows what they're doing
        let s_success    = if success_rate >= 0.95 { 0.30 } else if success_rate >= 0.80 { 0.15 } else { 0.0 };
        // cross-pool activity = arb bot signature
        let s_cross_pool = if self.unique_pools >= 4 { 0.25 } else if self.unique_pools >= 2 { 0.12 } else { 0.0 };
        // optimized CU usage. retail wallets are sloppy with compute budgets.
        let s_cu         = if avg_cu < 120_000.0 && avg_cu > 10_000.0 { 0.20 } else if avg_cu < 180_000.0 { 0.08 } else { 0.0 };
        let s_timing     = if first_mover_rate >= 0.30 { 0.15 } else if first_mover_rate >= 0.10 { 0.07 } else { 0.0 };
        let s_arb        = if arb_rate >= 0.50 { 0.10 } else { 0.0 };

        s_success + s_cross_pool + s_cu + s_timing + s_arb
    }

    fn is_stale(&self) -> bool { self.last_seen.elapsed() > PROFILE_WINDOW }
}

pub struct SmartMoneyClassifier {
    profiles:     Arc<RwLock<AHashMap<Pubkey, Profile>>>,
    arb_programs: Vec<Pubkey>,
}

impl SmartMoneyClassifier {
    pub fn new() -> Arc<Self> {
        let arb_programs = ARB_PROGRAMS.iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        Arc::new(Self {
            profiles:    Arc::new(RwLock::new(AHashMap::with_capacity(1024))),
            arb_programs,
        })
    }

    pub fn observe_tx(&self, signer: Pubkey, success: bool, pool: Pubkey, cu: u32, is_first: bool, program_ids: &[Pubkey]) {
        let is_arb = program_ids.iter().any(|p| self.arb_programs.contains(p));
        let mut map = self.profiles.write().unwrap();

        // evict stale entries when we're getting big. could be smarter but this is fine at 50k.
        if map.len() >= MAX_PROFILES {
            map.retain(|_, v| !v.is_stale());
        }

        map.entry(signer)
            .or_insert_with(|| Profile::new(signer))
            .record(success, pool, cu, is_first, is_arb);
    }

    pub fn is_smart_money(&self, address: &Pubkey) -> bool {
        let map = self.profiles.read().unwrap();
        match map.get(address) {
            None => false,
            Some(p) if p.is_stale() => false,
            Some(p) if p.tx_count < MIN_OBS => {
                // raise the bar when we don't have enough data. benefit of the doubt costs money.
                let is_sm = p.score() > SM_THRESHOLD + 0.15;
                if is_sm { debug!("SM (low-obs) {address} score={:.3}", p.score()); }
                is_sm
            }
            Some(p) => {
                let is_sm = p.score() > SM_THRESHOLD;
                if is_sm {
                    debug!("SM {address} score={:.3} success={}/{} pools={}",
                           p.score(), p.success_count, p.tx_count, p.unique_pools);
                }
                is_sm
            }
        }
    }

    pub fn score_of(&self, address: &Pubkey) -> Option<f64> {
        self.profiles.read().unwrap()
            .get(address)
            .filter(|p| !p.is_stale())
            .map(|p| p.score())
    }

    pub fn top_smart_money(&self, n: usize) -> Vec<(Pubkey, f64)> {
        let map = self.profiles.read().unwrap();
        let mut scored: Vec<_> = map.values()
            .filter(|p| !p.is_stale() && p.tx_count >= MIN_OBS)
            .map(|p| (p.address, p.score()))
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        scored.truncate(n);
        scored
    }
}
