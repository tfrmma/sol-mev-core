use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tracing::{debug, info};

use crate::config::BotConfig;
use crate::risk::RiskEngine;
use crate::state::{LendingProtocol, ObligationState, OBLIGATIONS};

// protocol/owner/health_factor aren't consumed by executor.rs today (it reads obligation,
// repay_reserve, withdraw_reserve, repay_amount), kept for logging and for when liquidation
// support extends beyond Kamino.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LiqOpportunity {
    pub obligation:               Pubkey,
    pub protocol:                 LendingProtocol,
    pub owner:                    Pubkey,
    pub repay_amount:             u64,
    pub repay_reserve:            Pubkey,
    pub withdraw_reserve:         Pubkey,
    pub gross_profit_lamports:    i64,
    pub adjusted_profit_lamports: Option<i64>, // None means risk engine vetoed it, or couldn't be computed
    pub health_factor:            f64,
}

impl LiqOpportunity {
    pub fn effective_profit(&self) -> i64 {
        self.adjusted_profit_lamports.unwrap_or(self.gross_profit_lamports)
    }
}

pub struct LiquidationScanner {
    min_profit: u64,
    // not read right now either, the actual liquidator pubkey used when building the tx
    // comes from executor.rs's own signer. kept for the same reason as `risk` above.
    #[allow(dead_code)]
    liquidator: Pubkey,
    // not read right now, see the note in build_opportunity: risk-adjustment needs the
    // collateral mint, which this sync scanner doesn't have without an extra RPC call.
    // kept in the constructor signature since callers already pass it and it's the natural
    // place to restore adjusted_profit_lamports if that RPC round trip gets added later.
    #[allow(dead_code)]
    risk: Arc<RiskEngine>,
}

impl LiquidationScanner {
    pub fn new(config: &BotConfig, liquidator: Pubkey, risk: Arc<RiskEngine>) -> Self {
        Self { min_profit: config.min_profit_lamports, liquidator, risk }
    }

    pub fn evaluate(&self, obligation_key: Pubkey) -> Option<LiqOpportunity> {
        let obl = OBLIGATIONS.get_cloned(&obligation_key)?;
        if !obl.is_underwater() { return None; }

        info!("underwater {} hf={:.4} ltv={}/{} protocol={:?}",
              obligation_key, obl.health_factor(), obl.ltv_bps(),
              obl.liquidation_threshold_bps, obl.protocol);

        let opp = self.build_opportunity(&obl)?;
        if opp.effective_profit() < self.min_profit as i64 {
            debug!("profit too low: {} (adjusted={:?})", opp.gross_profit_lamports, opp.adjusted_profit_lamports);
            return None;
        }
        info!("liquidation: obligation={} gross={} adjusted={:?}",
              obligation_key, opp.gross_profit_lamports, opp.adjusted_profit_lamports);
        Some(opp)
    }

    // full sweep over every tracked obligation. slower than evaluate() (O(n)), driven
    // periodically from main.rs (LIQ_SWEEP_INTERVAL) rather than per-update.
    pub fn scan_all(&self) -> Vec<LiqOpportunity> {
        let mut opps: Vec<_> = OBLIGATIONS.collect_all().into_iter()
            .filter(|o| o.is_underwater())
            .filter_map(|o| {
                let opp = self.build_opportunity(&o)?;
                (opp.effective_profit() >= self.min_profit as i64).then_some(opp)
            })
            .collect();
        opps.sort_by(|a, b| b.effective_profit().cmp(&a.effective_profit()));
        opps
    }

    // liquidation ix building lives in executor.rs::execute_liquidation now, using the
    // official klend-interface crate (ObligationContext::liquidate) with real reserve data
    // fetched over RPC. this scanner stays sync and only decides *whether* and *how much*
    // to liquidate, not how to build the transaction.

    fn build_opportunity(&self, obl: &ObligationState) -> Option<LiqOpportunity> {
        let close_factor = self.close_factor(obl);
        let repay_value  = obl.borrow_value * close_factor as u128 / 10_000;
        let repay_lamps  = (repay_value / 1_000_000_000) as u64;
        let bonus_bps    = self.bonus_bps(obl);
        let collateral   = repay_lamps + repay_lamps * bonus_bps / 10_000;
        let gross        = collateral as i64 - repay_lamps as i64;

        // real reserve pubkeys now (from klend_interface's zero-copy Obligation parse in
        // monitor.rs), not the owner-as-placeholder hack this used to be.
        let repay_reserve    = obl.top_borrow_reserve;
        let withdraw_reserve = obl.top_deposit_reserve;

        // NOTE: no risk.adjusted_profit() call here anymore. that needs the collateral mint
        // to look up an exit pool, and we only have the reserve pubkey at this point, not its
        // mint, without an extra RPC round trip this sync scanner doesn't have. executor.rs
        // fetches the real Reserve account anyway before executing, that's the right place
        // to re-check slippage against gross_profit_lamports if this turns out to matter.
        let adjusted = None;

        Some(LiqOpportunity {
            obligation: obl.obligation_pubkey,
            protocol: obl.protocol.clone(),
            owner: obl.owner,
            repay_amount: repay_lamps,
            repay_reserve,
            withdraw_reserve,
            gross_profit_lamports: gross,
            adjusted_profit_lamports: adjusted,
            health_factor: obl.health_factor(),
        })
    }

    fn close_factor(&self, obl: &ObligationState) -> u64 {
        match obl.protocol {
            LendingProtocol::Kamino => self.kamino_close_factor(obl),
            // solend and marginfi both use 50% flat. boring but fine.
            LendingProtocol::Solend | LendingProtocol::MarginFi => 5_000,
        }
    }

    fn bonus_bps(&self, obl: &ObligationState) -> u64 {
        match obl.protocol {
            LendingProtocol::Kamino => {
                // tiered bonus based on how far underwater they are
                let excess = obl.ltv_bps().saturating_sub(obl.liquidation_threshold_bps);
                if excess > 1000 { 1500 } else if excess > 500 { 1000 } else { 700 }
            }
            _ => 500, // 5% flat for everyone else
        }
    }

    // kamino's dynamic close factor: slides from ~20% at threshold up to 100% at full insolvency.
    // getting this wrong is expensive, overclosing burns capital, underclosing leaves profit on table.
    fn kamino_close_factor(&self, obl: &ObligationState) -> u64 {
        let ltv       = obl.ltv_bps() as u64;
        let threshold = obl.liquidation_threshold_bps as u64;
        if ltv >= 10_000 { return 10_000; }
        let excess = ltv.saturating_sub(threshold);
        let range  = 10_000u64.saturating_sub(threshold).max(1);
        (2_000 + excess * 8_000 / range).min(10_000)
    }
}
