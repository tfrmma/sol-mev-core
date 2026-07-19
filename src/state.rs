// sharded map because dashmap's rwlock contention on hot paths was making me sad.
// 64 shards, power-of-two so we can mask. don't overthink it.
//
// GC note: Solana spawns hundreds of pump.fun pools per hour. without a TTL sweep
// this map will bloat, hash collisions creep up and you start paying for cache misses
// you shouldn't be paying for. run gc_stale() from a background task every ~30s.
use crossbeam_utils::CachePadded;
use once_cell::sync::Lazy;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, sync::{Arc, RwLock}};

const NUM_SHARDS: usize = 64;
const SHARD_MASK: u64   = (NUM_SHARDS as u64) - 1;

// pre-allocate enough per shard to avoid reallocs at normal pool counts.
// 64 shards × 128 = 8192 entries before first rehash. enough for mainnet steady-state.
const SHARD_INIT_CAP: usize = 128;

pub const STALE_SLOT_THRESHOLD: u64 = 8;   // ~3.2s. if your data is older than this, it's garbage.
pub const GC_SLOT_THRESHOLD: u64    = 1500; // ~10min @ 400ms/slot. pools dead this long get evicted.

struct Shard<V>(CachePadded<RwLock<HashMap<Pubkey, V>>>);

impl<V> Shard<V> {
    fn new() -> Self {
        Self(CachePadded::new(RwLock::new(HashMap::with_capacity(SHARD_INIT_CAP))))
    }
}

pub struct ShardedTable<V> {
    shards: Vec<Shard<V>>,
}

impl<V: Clone + Send + Sync + 'static> ShardedTable<V> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { shards: (0..NUM_SHARDS).map(|_| Shard::new()).collect() })
    }

    #[inline(always)]
    fn shard_idx(key: &Pubkey) -> usize {
        // low 6 bits of the first 8 bytes. good enough distribution across base58 keys.
        let lo = u64::from_le_bytes(key.as_ref()[0..8].try_into().unwrap());
        (lo & SHARD_MASK) as usize
    }

    pub fn insert(&self, key: Pubkey, value: V) {
        let idx = Self::shard_idx(&key);
        // spin briefly before blocking. contention is rare; when it happens we want low latency.
        for _ in 0..3 {
            if let Ok(mut g) = self.shards[idx].0.try_write() {
                g.insert(key, value);
                return;
            }
            std::hint::spin_loop();
        }
        self.shards[idx].0.write().unwrap().insert(key, value);
    }

    pub fn get_cloned(&self, key: &Pubkey) -> Option<V> {
        let idx = Self::shard_idx(key);
        self.shards[idx].0.read().unwrap().get(key).cloned()
    }

    pub fn for_each<F: FnMut(&Pubkey, &V)>(&self, mut f: F) {
        for shard in &self.shards {
            let g = shard.0.read().unwrap();
            for (k, v) in g.iter() { f(k, v); }
        }
    }

    pub fn collect_all(&self) -> Vec<V> {
        let mut out = Vec::with_capacity(NUM_SHARDS * SHARD_INIT_CAP / 4);
        for shard in &self.shards {
            out.extend(shard.0.read().unwrap().values().cloned());
        }
        out
    }

    pub fn remove(&self, key: &Pubkey) {
        let idx = Self::shard_idx(key);
        self.shards[idx].0.write().unwrap().remove(key);
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.0.read().unwrap().len()).sum()
    }
}

// GC trait — lets callers drive eviction without knowing the value type internals.
pub trait HasSlot { fn slot(&self) -> u64; }

impl<V: Clone + Send + Sync + HasSlot + 'static> ShardedTable<V> {
    // sweeps entries whose slot age exceeds `max_age_slots`. run from a background task.
    // returns number of entries evicted — useful for metrics.
    pub fn gc_stale(&self, current_slot: u64, max_age_slots: u64) -> usize {
        let mut evicted = 0usize;
        for shard in &self.shards {
            let mut g = shard.0.write().unwrap();
            let before = g.len();
            g.retain(|_, v| current_slot.saturating_sub(v.slot()) <= max_age_slots);
            evicted += before - g.len();
        }
        evicted
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Dex { Raydium, Orca, OrcaWhirlpool, Lifinity, Meteora }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LendingProtocol { Kamino, Solend, MarginFi }

#[derive(Debug, Clone)]
pub struct PoolState {
    pub pool_id:      Pubkey,
    pub dex:          Dex,
    pub token_a_mint: Pubkey,
    pub token_b_mint: Pubkey,
    pub reserve_a:    u64,
    pub reserve_b:    u64,
    pub fee_bps:      u16,
    pub slot:         u64,
}

impl HasSlot for PoolState      { fn slot(&self) -> u64 { self.slot } }
impl HasSlot for ObligationState { fn slot(&self) -> u64 { self.slot } }

impl PoolState {
    #[inline(always)]
    pub fn price_a_in_b(&self) -> Option<f64> {
        if self.reserve_a == 0 { return None; }
        Some(self.reserve_b as f64 / self.reserve_a as f64)
    }

    // constant product. boring but correct. don't try to be clever here.
    #[inline(always)]
    pub fn quote_a_to_b(&self, amount_in: u64) -> u64 {
        let fee_num     = 10_000u128 - self.fee_bps as u128;
        let in_with_fee = amount_in as u128 * fee_num;
        let num         = in_with_fee * self.reserve_b as u128;
        let den         = (self.reserve_a as u128 * 10_000) + in_with_fee;
        if den == 0 { return 0; }
        (num / den) as u64
    }

    #[inline(always)]
    pub fn quote_b_to_a(&self, amount_in: u64) -> u64 {
        let fee_num     = 10_000u128 - self.fee_bps as u128;
        let in_with_fee = amount_in as u128 * fee_num;
        let num         = in_with_fee * self.reserve_a as u128;
        let den         = (self.reserve_b as u128 * 10_000) + in_with_fee;
        if den == 0 { return 0; }
        (num / den) as u64
    }

    #[inline(always)]
    pub fn is_stale(&self, current_slot: u64) -> bool {
        current_slot.saturating_sub(self.slot) > STALE_SLOT_THRESHOLD
    }
}

#[derive(Debug, Clone)]
pub struct ObligationState {
    pub obligation_pubkey:         Pubkey,
    pub owner:                     Pubkey,
    pub protocol:                  LendingProtocol,
    pub collateral_value:          u128,
    pub borrow_value:              u128,
    pub liquidation_threshold_bps: u16,
    pub slot:                      u64,
}

impl ObligationState {
    #[inline(always)]
    pub fn ltv_bps(&self) -> u16 {
        if self.collateral_value == 0 { return u16::MAX; }
        let bps = self.borrow_value * 10_000 / self.collateral_value;
        bps.min(u16::MAX as u128) as u16
    }

    #[inline(always)]
    pub fn is_underwater(&self) -> bool {
        self.ltv_bps() >= self.liquidation_threshold_bps
    }

    #[inline(always)]
    pub fn health_factor(&self) -> f64 {
        if self.borrow_value == 0 { return f64::MAX; }
        (self.collateral_value as f64 * self.liquidation_threshold_bps as f64)
            / (self.borrow_value as f64 * 10_000.0)
    }
}

// globals. yes i know. it's fine. they're sharded and lock-free enough.
pub static POOLS: Lazy<Arc<ShardedTable<PoolState>>>            = Lazy::new(ShardedTable::new);
pub static OBLIGATIONS: Lazy<Arc<ShardedTable<ObligationState>>> = Lazy::new(ShardedTable::new);
pub static CURRENT_SLOT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
