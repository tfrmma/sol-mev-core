// sandwich detection. yes it's in here. no, we don't run it by default.
// read the README disclaimer before you even think about flipping that flag.
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::BotConfig;
use crate::monitor::PendingSwap;
use crate::risk::RiskEngine;
use crate::smart_money::SmartMoneyClassifier;
use crate::state::{PoolState, POOLS};

// 50bps minimum slippage tolerance on the victim. anything tighter and
// you're playing with fire, the frontrun will push them past min_amount_out.
const MIN_VICTIM_SLIP_BPS: u16   = 50;
const MAX_POOL_IMPACT_BPS: u16   = 200;  // don't nuke the pool, 2% is already aggressive
const MAX_VICTIM_IMPACT_BPS: u16 = 100;  // if the victim is moving price 1%+ they're probably not retail
const SIZE_STEP: u64             = 1_000;
const FRONTRUN_FLOOR: u64        = 100_000;
// 3 ixs * ~5k each. rough but close enough
const FEE_ESTIMATE: i64          = 15_000;

#[derive(Debug, Clone)]
pub struct SandwichOpportunity {
    pub victim_sig:              String,
    pub victim_pool:             Pubkey,
    pub pool_state:              PoolState,
    pub frontrun_amount:         u64,
    pub frontrun_output:         u64,
    pub estimated_profit:        i64,
    pub victim_price_impact_bps: u16,
    pub victim_sm_score:         f64,
}

pub struct SandwichDetector {
    min_profit:  u64,
    risk:        Arc<RiskEngine>,
    smart_money: Arc<SmartMoneyClassifier>,
}

impl SandwichDetector {
    pub fn new(config: &BotConfig, risk: Arc<RiskEngine>, smart_money: Arc<SmartMoneyClassifier>) -> Self {
        Self { min_profit: config.min_profit_lamports, risk, smart_money }
    }

    pub fn evaluate(&self, swap: &PendingSwap) -> Option<SandwichOpportunity> {
        let pool = POOLS.get_cloned(&swap.pool)?;

        let fair_out = self.quote(&pool, swap.input_mint, swap.amount_in);
        let slip_bps = self.slippage_bps(fair_out, swap.min_amount_out);
        let sm_score = self.smart_money.score_of(&swap.user).unwrap_or(0.0);

        self.check_preconditions(swap, &pool, slip_bps, sm_score)?;

        let vol_scale     = self.vol_scale(pool.token_a_mint);
        let max_frontrun  = (pool.reserve_a * MAX_POOL_IMPACT_BPS as u64 / 10_000) as f64 * vol_scale;
        let frontrun_amount = self.optimal_frontrun_size(&pool, swap, max_frontrun as u64)?;
        let frontrun_output = self.quote(&pool, swap.input_mint, frontrun_amount);

        let profit = self.simulate_profit(&pool, swap, frontrun_amount, frontrun_output);
        if profit < self.min_profit as i64 { return None; }

        // victim's impact on pool. just an approximation, good enough
        let victim_impact_bps = (swap.amount_in.saturating_mul(10_000) / pool.reserve_a.max(1)) as u16;

        info!("sandwich: victim={} profit={profit} slip={slip_bps}bps sm={sm_score:.3} vol_scale={vol_scale:.2}",
              swap.signature);

        Some(SandwichOpportunity {
            victim_sig: swap.signature.clone(),
            victim_pool: swap.pool,
            pool_state: pool,
            frontrun_amount,
            frontrun_output,
            estimated_profit: profit,
            victim_price_impact_bps: victim_impact_bps,
            victim_sm_score: sm_score,
        })
    }

    fn check_preconditions(&self, swap: &PendingSwap, pool: &PoolState, slip_bps: u16, sm_score: f64) -> Option<()> {
        if slip_bps < MIN_VICTIM_SLIP_BPS {
            debug!("slip too tight: {slip_bps}bps");
            return None;
        }
        // if they know what they're doing, skip it. getting sandwiched by a sandwich bot is bad karma
        if self.smart_money.is_smart_money(&swap.user) {
            warn!("toxic flow: {} sm={sm_score:.3}", swap.user);
            return None;
        }
        let reserve    = if swap.input_mint == pool.token_a_mint { pool.reserve_a } else { pool.reserve_b };
        let impact_bps = (swap.amount_in.saturating_mul(10_000) / reserve.max(1)) as u16;
        if impact_bps > MAX_VICTIM_IMPACT_BPS {
            debug!("victim impact too large: {impact_bps}bps");
            return None;
        }
        Some(())
    }

    fn simulate_profit(&self, pool: &PoolState, swap: &PendingSwap, frontrun: u64, frontrun_out: u64) -> i64 {
        let post_front  = self.apply_swap(pool, swap.input_mint, frontrun);
        let post_victim = self.apply_swap(&post_front, swap.input_mint, swap.amount_in);
        // backrun: we're selling what we bought
        let back_out = if swap.input_mint == post_victim.token_a_mint {
            post_victim.quote_b_to_a(frontrun_out)
        } else {
            post_victim.quote_a_to_b(frontrun_out)
        };
        back_out as i64 - frontrun as i64 - FEE_ESTIMATE
    }

    // binary search the largest frontrun that won't revert the victim.
    // this is the ugly but it flies approach, no closed form, just bisect.
    fn optimal_frontrun_size(&self, pool: &PoolState, swap: &PendingSwap, max: u64) -> Option<u64> {
        let (mut lo, mut hi) = (FRONTRUN_FLOOR, max);
        if lo > hi { return None; }
        while hi - lo > SIZE_STEP {
            let mid        = (lo + hi) / 2;
            let after      = self.apply_swap(pool, swap.input_mint, mid);
            let victim_out = self.quote(&after, swap.input_mint, swap.amount_in);
            if victim_out >= swap.min_amount_out { lo = mid; } else { hi = mid; }
        }
        if lo < FRONTRUN_FLOOR { None } else { Some(lo) }
    }

    fn apply_swap(&self, pool: &PoolState, input_mint: Pubkey, amount_in: u64) -> PoolState {
        let mut p       = pool.clone();
        let fee_factor  = (10_000 - pool.fee_bps as u128) as u128;
        let in_with_fee = amount_in as u128 * fee_factor;
        if input_mint == pool.token_a_mint {
            let out = (in_with_fee * pool.reserve_b as u128) / (pool.reserve_a as u128 * 10_000 + in_with_fee);
            p.reserve_a = pool.reserve_a.saturating_add(amount_in);
            p.reserve_b = pool.reserve_b.saturating_sub(out as u64);
        } else {
            let out = (in_with_fee * pool.reserve_a as u128) / (pool.reserve_b as u128 * 10_000 + in_with_fee);
            p.reserve_b = pool.reserve_b.saturating_add(amount_in);
            p.reserve_a = pool.reserve_a.saturating_sub(out as u64);
        }
        p
    }

    fn quote(&self, pool: &PoolState, input_mint: Pubkey, amount: u64) -> u64 {
        if input_mint == pool.token_a_mint { pool.quote_a_to_b(amount) } else { pool.quote_b_to_a(amount) }
    }

    fn slippage_bps(&self, fair_out: u64, min_out: u64) -> u16 {
        if fair_out == 0 { return 0; }
        (fair_out.saturating_sub(min_out).saturating_mul(10_000) / fair_out) as u16
    }

    // scale down frontrun size when volatility is high. simple linear scaling,
    // nothing fancy. sigma*10 gives us something in [0, 0.7] range roughly.
    fn vol_scale(&self, mint: Pubkey) -> f64 {
        (1.0 - (self.risk.sigma_for(mint) * 10.0).min(0.70)).max(0.30)
    }
}
