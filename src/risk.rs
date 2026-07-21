// EWMA volatility + circuit breaker + profit adjustment for liquidations.
// nothing fancy, RiskMetrics-style, lambda=0.94 because that's what everyone uses
// and it works well enough on crypto (high kurtosis be damned).
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tracing::{debug, warn};

use crate::state::PoolState;

const EWMA_LAMBDA: f64           = 0.94;
const CIRCUIT_BREAKER_SIGMA: f64 = 0.08;  // 8% per-update sigma. if we're here, something is wrong.
const SLIPPAGE_SAFETY_MULT: f64  = 1.5;   // conservative. we'd rather pass than blow up.
const EXECUTION_SLOTS: u64       = 2;     // ~800ms. optimistic but Jito bundles land fast.

struct AssetVol {
    last_price:    f64,
    ewma_variance: f64,
    last_update:   std::time::Instant,
}

impl AssetVol {
    fn new(price: f64) -> Self {
        Self { last_price: price, ewma_variance: 0.0, last_update: std::time::Instant::now() }
    }

    fn update(&mut self, price: f64) {
        if self.last_price <= 0.0 || price <= 0.0 { return; }
        let r = (price / self.last_price).ln();
        self.ewma_variance = EWMA_LAMBDA * self.ewma_variance + (1.0 - EWMA_LAMBDA) * r * r;
        self.last_price    = price;
        self.last_update   = std::time::Instant::now();
    }

    fn sigma(&self) -> f64 { self.ewma_variance.sqrt() }

    // 2σ haircut scaled by sqrt(execution window). standard microstructure stuff.
    // cap at 25% because beyond that our quote is garbage anyway and we should just skip.
    fn price_haircut(&self, slots: u64) -> f64 {
        (2.0 * self.sigma() * (slots as f64).sqrt()).min(0.25)
    }
}

// how much of the exit will we lose to price impact. multiply by safety factor because
// we usually can't exit instantly and the market moves against us.
fn exit_slippage(pool: &PoolState, sell_amount: u64, sell_is_a: bool) -> f64 {
    let reserve = if sell_is_a { pool.reserve_a } else { pool.reserve_b };
    if reserve == 0 { return 1.0; }
    (sell_amount as f64 / (reserve as f64 + sell_amount as f64)) * SLIPPAGE_SAFETY_MULT
}

pub struct RiskEngine {
    vol:              Arc<RwLock<HashMap<Pubkey, AssetVol>>>,
    fee_p95_lamports: std::sync::atomic::AtomicU64,
}

impl RiskEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            vol:              Arc::new(RwLock::new(HashMap::with_capacity(128))),
            fee_p95_lamports: std::sync::atomic::AtomicU64::new(25_000),
        })
    }

    // called on every pool account update from geyser
    pub fn on_pool_update(&self, pool: &PoolState) {
        if let Some(price) = pool.price_a_in_b() {
            self.update_vol(pool.token_a_mint, price);
            if price > 0.0 { self.update_vol(pool.token_b_mint, 1.0 / price); }
        }
    }

    pub fn update_fee_p95(&self, lamports: u64) {
        self.fee_p95_lamports.store(lamports, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn sigma_for(&self, mint: Pubkey) -> f64 {
        // default 1% if we haven't seen this mint yet. not great, not terrible.
        self.vol.read().unwrap().get(&mint).map(|v| v.sigma()).unwrap_or(0.01)
    }

    pub fn circuit_breaker_active(&self, mint: Pubkey) -> bool {
        let sigma = self.sigma_for(mint);
        if sigma > CIRCUIT_BREAKER_SIGMA {
            warn!("circuit breaker: σ={sigma:.4} for {mint} > {CIRCUIT_BREAKER_SIGMA:.4}");
            return true;
        }
        false
    }

    // net profit after haircut for price vol, exit slippage, and fees.
    // returns None if the circuit breaker is tripped or net goes negative.
    pub fn adjusted_profit(
        &self,
        gross:           i64,
        collateral_mint: Pubkey,
        exit_pool:       &PoolState,
        exit_amount:     u64,
        exit_is_a:       bool,
    ) -> Option<i64> {
        if self.circuit_breaker_active(collateral_mint) { return None; }

        let haircut = self.vol.read().unwrap()
            .get(&collateral_mint)
            .map(|v| v.price_haircut(EXECUTION_SLOTS))
            .unwrap_or(0.02); // 2% default when blind

        let slippage_cost = exit_slippage(exit_pool, exit_amount, exit_is_a) * exit_amount as f64;
        let fee           = self.fee_p95_lamports.load(std::sync::atomic::Ordering::Relaxed) as f64;
        let net           = gross as f64 * (1.0 - haircut) - slippage_cost - fee;

        debug!("risk: gross={gross} haircut={haircut:.3} slip={slippage_cost:.0} fee={fee:.0} → net={net:.0}");

        if net > 0.0 { Some(net as i64) } else { None }
    }

    pub fn volatility_report(&self) -> Vec<(Pubkey, f64)> {
        self.vol.read().unwrap().iter().map(|(k, v)| (*k, v.sigma())).collect()
    }

    fn update_vol(&self, mint: Pubkey, price: f64) {
        self.vol.write().unwrap()
            .entry(mint)
            .or_insert_with(|| AssetVol::new(price))
            .update(price);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Dex;

    fn pool(reserve_a: u64, reserve_b: u64) -> PoolState {
        PoolState {
            pool_id: Pubkey::default(), dex: Dex::Raydium,
            token_a_mint: Pubkey::default(), token_b_mint: Pubkey::default(),
            reserve_a, reserve_b, fee_bps: 25, slot: 0, clmm: None,
        }
    }

    #[test]
    fn exit_slippage_zero_reserve_is_total_loss() {
        let p = pool(0, 1_000);
        assert_eq!(exit_slippage(&p, 100, true), 1.0);
    }

    #[test]
    fn exit_slippage_scales_with_size_and_safety_mult() {
        let p = pool(1_000_000, 1_000_000);
        // 1000 / (1_000_000 + 1000) * 1.5, small trade against deep liquidity should be tiny
        let small = exit_slippage(&p, 1_000, true);
        let large = exit_slippage(&p, 500_000, true);
        assert!(small < large, "bigger trade should slip more");
        assert!(small < 0.01, "1000 against a 1M pool shouldn't slip more than 1%, got {small}");
    }

    #[test]
    fn asset_vol_sigma_starts_at_zero() {
        let v = AssetVol::new(100.0);
        assert_eq!(v.sigma(), 0.0, "no price history yet, no variance");
    }

    #[test]
    fn asset_vol_sigma_rises_after_a_price_jump() {
        let mut v = AssetVol::new(100.0);
        v.update(150.0); // 50% jump, should register real variance
        assert!(v.sigma() > 0.0, "sigma should be nonzero after a price move");
    }

    #[test]
    fn asset_vol_ignores_nonpositive_prices() {
        // a zero or negative price is bad data (decode bug, empty pool, whatever), not a real move.
        // update() should just no-op instead of feeding ln(negative) or ln(0) into the EWMA.
        let mut v = AssetVol::new(100.0);
        v.update(0.0);
        v.update(-5.0);
        assert_eq!(v.sigma(), 0.0);
        assert_eq!(v.last_price, 100.0);
    }

    #[test]
    fn adjusted_profit_none_when_net_negative() {
        let risk = RiskEngine::new();
        let mint = Pubkey::new_unique();
        let exit_pool = pool(1_000, 1_000); // tiny pool, exiting into it is expensive
        // gross profit smaller than what fees + slippage will eat
        let result = risk.adjusted_profit(1_000, mint, &exit_pool, 500, true);
        assert!(result.is_none(), "should reject a trade where costs exceed gross profit");
    }

    #[test]
    fn adjusted_profit_some_when_comfortably_net_positive() {
        let risk = RiskEngine::new();
        let mint = Pubkey::new_unique();
        let exit_pool = pool(10_000_000_000, 10_000_000_000); // deep pool, minimal slippage
        let result = risk.adjusted_profit(1_000_000_000, mint, &exit_pool, 1_000, true);
        assert!(result.is_some(), "large gross profit against deep liquidity should clear costs");
        assert!(result.unwrap() < 1_000_000_000, "adjusted profit should still be less than gross");
    }
}
