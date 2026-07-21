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

// GC trait, lets callers drive eviction without knowing the value type internals.
pub trait HasSlot { fn slot(&self) -> u64; }

impl<V: Clone + Send + Sync + HasSlot + 'static> ShardedTable<V> {
    // sweeps entries whose slot age exceeds `max_age_slots`. run from a background task.
    // returns number of entries evicted, useful for metrics.
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

// concentrated liquidity pool state, sqrt_price and liquidity as tracked on-chain.
// sqrt_price is Q64.64 fixed point, verified against orca-so/whirlpools state/whirlpool.rs.
#[derive(Debug, Clone)]
pub struct ClmmState {
    pub sqrt_price:  u128, // Q64.64
    pub liquidity:   u128,
    pub tick_current: i32,
    pub tick_spacing: u16,
}

const Q64: u128 = 1u128 << 64;

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
    // Some() for CLMM dexes (whirlpool), None for constant-product ones (raydium, orca legacy).
    // reserve_a/reserve_b are left at 0 for CLMM pools, don't use them, use clmm instead.
    pub clmm:         Option<ClmmState>,
}

impl HasSlot for PoolState      { fn slot(&self) -> u64 { self.slot } }
impl HasSlot for ObligationState { fn slot(&self) -> u64 { self.slot } }

impl PoolState {
    #[inline(always)]
    pub fn price_a_in_b(&self) -> Option<f64> {
        if let Some(c) = &self.clmm {
            // price = (sqrt_price / 2^64)^2, how many B per 1 A. exact, no approximation here.
            let sp = c.sqrt_price as f64 / Q64 as f64;
            return Some(sp * sp);
        }
        if self.reserve_a == 0 { return None; }
        Some(self.reserve_b as f64 / self.reserve_a as f64)
    }

    // constant product. boring but correct. don't try to be clever here.
    #[inline(always)]
    pub fn quote_a_to_b(&self, amount_in: u64) -> u64 {
        if let Some(c) = &self.clmm { return self.quote_clmm(c, amount_in, true); }
        let fee_num     = 10_000u128 - self.fee_bps as u128;
        let in_with_fee = amount_in as u128 * fee_num;
        let num         = in_with_fee * self.reserve_b as u128;
        let den         = (self.reserve_a as u128 * 10_000) + in_with_fee;
        if den == 0 { return 0; }
        (num / den) as u64
    }

    #[inline(always)]
    pub fn quote_b_to_a(&self, amount_in: u64) -> u64 {
        if let Some(c) = &self.clmm { return self.quote_clmm(c, amount_in, false); }
        let fee_num     = 10_000u128 - self.fee_bps as u128;
        let in_with_fee = amount_in as u128 * fee_num;
        let num         = in_with_fee * self.reserve_a as u128;
        let den         = (self.reserve_b as u128 * 10_000) + in_with_fee;
        if den == 0 { return 0; }
        (num / den) as u64
    }

    // single-tick-range swap quote (no tick crossing), CLMM math verified against
    // orca-so/whirlpools/programs/whirlpool/src/math/{token_math,swap_math}.rs.
    //
    // IMPORTANT CAVEAT: this only holds while the swap stays within the pool's currently
    // active tick range. once the trade is big enough to cross into the next initialized
    // tick, real liquidity changes and this formula silently undercounts slippage, i.e. it
    // will quote a BETTER output than you'd actually get on-chain. fine for opportunity
    // scanning on reasonably-sized trades, not safe yet for sizing large trades via the
    // binary search in arbitrage.rs without a size cap. needs real tick-array walking and
    // unit tests against known pool snapshots before this is fully trustworthy, tracked
    // as a follow-up.
    fn quote_clmm(&self, c: &ClmmState, amount_in: u64, a_to_b: bool) -> u64 {
        if c.liquidity == 0 || c.sqrt_price == 0 { return 0; }
        let fee_num     = 10_000u128 - self.fee_bps as u128;
        let in_with_fee = (amount_in as u128 * fee_num) / 10_000;
        if in_with_fee == 0 { return 0; }

        if a_to_b {
            // adding token A moves price down. Δ(sqrt_price) via token0-style delta:
            // new_sqrt_price = L * sqrt_price / (L + amount_in * sqrt_price / 2^64)
            let Some(product) = in_with_fee.checked_mul(c.sqrt_price) else { return 0 };
            let denom = c.liquidity + (product / Q64);
            if denom == 0 { return 0; }
            let Some(num) = c.liquidity.checked_mul(c.sqrt_price) else { return 0 };
            let new_sqrt_price = num / denom;
            if new_sqrt_price >= c.sqrt_price { return 0; } // no movement, or overflowed into garbage
            // amount_out (token B) = L * (sqrt_price - new_sqrt_price) / 2^64
            let Some(delta) = c.liquidity.checked_mul(c.sqrt_price - new_sqrt_price) else { return 0 };
            (delta / Q64).min(u64::MAX as u128) as u64
        } else {
            // adding token B moves price up linearly: new_sqrt_price = sqrt_price + amount*2^64/L
            let Some(scaled) = in_with_fee.checked_mul(Q64) else { return 0 };
            let new_sqrt_price = c.sqrt_price + (scaled / c.liquidity);
            // amount_out (token A) = L * (new_sqrt_price - sqrt_price) / (sqrt_price * new_sqrt_price) * 2^64
            let Some(num) = c.liquidity.checked_mul(new_sqrt_price - c.sqrt_price) else { return 0 };
            let Some(num) = num.checked_mul(Q64) else { return 0 };
            let Some(den) = c.sqrt_price.checked_mul(new_sqrt_price) else { return 0 };
            if den == 0 { return 0; }
            (num / den).min(u64::MAX as u128) as u64
        }
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
    // largest deposit/borrow reserve on this obligation, by value. simplification: a real
    // optimal liquidation might split across multiple reserves, we just take the biggest one
    // on each side. good enough for a first cut, not optimal for obligations with several
    // sizeable positions.
    pub top_deposit_reserve:       Pubkey,
    pub top_borrow_reserve:        Pubkey,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(reserve_a: u64, reserve_b: u64, fee_bps: u16) -> PoolState {
        PoolState {
            pool_id: Pubkey::default(), dex: Dex::Raydium,
            token_a_mint: Pubkey::default(), token_b_mint: Pubkey::default(),
            reserve_a, reserve_b, fee_bps, slot: 0, clmm: None,
        }
    }

    #[test]
    fn constant_product_quote_respects_fee() {
        // 1000/1000 pool, 30bps fee, swap 100 in. output should be less than a fee-less
        // constant-product swap would give (100 * 1000 / 1100 = 90.9...).
        let p = pool(1_000, 1_000, 30);
        let out = p.quote_a_to_b(100);
        assert!(out < 91, "expected fee to eat into output, got {out}");
        assert!(out > 85, "fee is only 30bps, shouldn't eat this much, got {out}");
    }

    #[test]
    fn constant_product_zero_reserve_returns_zero_not_panic() {
        let p = pool(0, 1_000, 30);
        assert_eq!(p.quote_a_to_b(100), 0);
    }

    #[test]
    fn constant_product_is_symmetric_at_parity() {
        // equal reserves, equal fee both directions, quoting a->b then the reverse amount
        // b->a should land close to the original input (not exact, that's what the fee is for).
        let p = pool(500_000, 500_000, 25);
        let out_b = p.quote_a_to_b(10_000);
        let back_a = p.quote_b_to_a(out_b);
        assert!(back_a < 10_000, "round trip should lose value to fees, got {back_a}");
        assert!(back_a > 9_900, "shouldn't lose more than ~2x the fee on a round trip, got {back_a}");
    }

    #[test]
    fn ltv_bps_saturates_instead_of_wrapping() {
        // this is the bug that was here before: (huge_ratio as u16) truncates mod 65536
        // before the .min() ever runs, so a catastrophic LTV could read back as healthy.
        let obl = ObligationState {
            obligation_pubkey: Pubkey::default(), owner: Pubkey::default(),
            protocol: LendingProtocol::Kamino,
            collateral_value: 1, // tiny collateral
            borrow_value: u128::MAX / 2, // huge debt relative to collateral
            liquidation_threshold_bps: 8_000,
            top_deposit_reserve: Pubkey::default(), top_borrow_reserve: Pubkey::default(),
            slot: 0,
        };
        assert_eq!(obl.ltv_bps(), u16::MAX, "should saturate, not wrap to a small number");
    }

    #[test]
    fn ltv_bps_zero_collateral_is_max_not_divide_by_zero() {
        let obl = ObligationState {
            obligation_pubkey: Pubkey::default(), owner: Pubkey::default(),
            protocol: LendingProtocol::Solend,
            collateral_value: 0,
            borrow_value: 100,
            liquidation_threshold_bps: 8_000,
            top_deposit_reserve: Pubkey::default(), top_borrow_reserve: Pubkey::default(),
            slot: 0,
        };
        assert_eq!(obl.ltv_bps(), u16::MAX);
    }

    #[test]
    fn clmm_price_matches_sqrt_price_squared() {
        // sqrt_price for a 1:1 price pool is exactly 2^64 (Q64.64 representation of 1.0)
        let sqrt_price_one: u128 = 1u128 << 64;
        let p = PoolState {
            pool_id: Pubkey::default(), dex: Dex::OrcaWhirlpool,
            token_a_mint: Pubkey::default(), token_b_mint: Pubkey::default(),
            reserve_a: 0, reserve_b: 0, fee_bps: 30, slot: 0,
            clmm: Some(ClmmState { sqrt_price: sqrt_price_one, liquidity: 1_000_000, tick_current: 0, tick_spacing: 64 }),
        };
        let price = p.price_a_in_b().unwrap();
        assert!((price - 1.0).abs() < 1e-9, "sqrt_price of 2^64 should be exactly price 1.0, got {price}");
    }

    #[test]
    fn clmm_quote_moves_price_in_the_right_direction() {
        let sqrt_price_one: u128 = 1u128 << 64;
        let p = PoolState {
            pool_id: Pubkey::default(), dex: Dex::OrcaWhirlpool,
            token_a_mint: Pubkey::default(), token_b_mint: Pubkey::default(),
            reserve_a: 0, reserve_b: 0, fee_bps: 0, slot: 0,
            clmm: Some(ClmmState { sqrt_price: sqrt_price_one, liquidity: 1_000_000_000_000, tick_current: 0, tick_spacing: 64 }),
        };
        // small trade relative to liquidity, output should be close to input at ~1:1 price
        let out = p.quote_a_to_b(1_000);
        assert!(out > 0, "should quote a nonzero amount for a small trade against real liquidity");
        assert!(out <= 1_000, "can't get more out than in at a 1:1 price with zero fee");
        assert!(out > 900, "small trade against large liquidity shouldn't slip this much, got {out}");
    }
}
