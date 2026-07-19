use solana_sdk::{instruction::{AccountMeta, Instruction}, pubkey::Pubkey};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::BotConfig;
use crate::risk::RiskEngine;
use crate::state::{LendingProtocol, ObligationState, PoolState, OBLIGATIONS, POOLS};

#[derive(Debug, Clone)]
pub struct LiqOpportunity {
    pub obligation:               Pubkey,
    pub protocol:                 LendingProtocol,
    pub owner:                    Pubkey,
    pub repay_amount:             u64,
    pub repay_mint:               Pubkey,
    pub collateral_mint:          Pubkey,
    pub gross_profit_lamports:    i64,
    pub adjusted_profit_lamports: Option<i64>, // None means risk engine vetoed it
    pub health_factor:            f64,
}

impl LiqOpportunity {
    pub fn effective_profit(&self) -> i64 {
        self.adjusted_profit_lamports.unwrap_or(self.gross_profit_lamports)
    }
}

pub struct LiquidationScanner {
    min_profit: u64,
    liquidator: Pubkey,
    risk:       Arc<RiskEngine>,
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

    // scan everything. called less frequently than evaluate(), don't be ashamed of the O(n).
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

    pub fn build_kamino_liquidation_ix(&self, opp: &LiqOpportunity) -> Instruction {
        let program_id: Pubkey = "KLend2g3cP87fffoy8q1mQqGKjrL1AyGGFsDGJr5J6Z".parse().unwrap();
        // discriminant from idl. if kamino redeploys and changes this we'll find out the hard way.
        let disc: [u8; 8] = [0xb5, 0xe9, 0x4c, 0xbb, 0x68, 0x91, 0x24, 0x1d];
        let mut data = disc.to_vec();
        data.extend_from_slice(&opp.repay_amount.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes()); // min_acceptable_received_collateral_amount
        Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(self.liquidator, true),
                AccountMeta::new(opp.obligation, false),
                // TODO: add remaining accounts from obligation collateral+reserve metadata.
                //       right now this ix will fail on-chain. need to parse the full obligation
                //       layout to pull vault/reserve/oracle pubkeys. tracked in #47.
            ],
            data,
        }
    }

    fn build_opportunity(&self, obl: &ObligationState) -> Option<LiqOpportunity> {
        let close_factor = self.close_factor(obl);
        let repay_value  = obl.borrow_value * close_factor as u128 / 10_000;
        let repay_lamps  = (repay_value / 1_000_000_000) as u64;
        let bonus_bps    = self.bonus_bps(obl);
        let collateral   = repay_lamps + repay_lamps * bonus_bps / 10_000;
        let gross        = collateral as i64 - repay_lamps as i64;

        // FIXME: both mints are wrong, need to decode from the obligation's deposit/borrow lists.
        //        using owner as placeholder so it at least compiles. don't ship this.
        let collateral_mint = obl.owner;
        let repay_mint      = obl.owner;

        let adjusted = self.best_exit_pool(collateral_mint).and_then(|pool| {
            let exit_is_a = pool.token_a_mint == collateral_mint;
            self.risk.adjusted_profit(gross, collateral_mint, &pool, collateral, exit_is_a)
        });

        Some(LiqOpportunity {
            obligation: obl.obligation_pubkey,
            protocol: obl.protocol.clone(),
            owner: obl.owner,
            repay_amount: repay_lamps,
            repay_mint,
            collateral_mint,
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

    // find deepest pool for this collateral. deep = less slippage on exit.
    fn best_exit_pool(&self, mint: Pubkey) -> Option<PoolState> {
        let mut best: Option<PoolState> = None;
        let mut best_liq = 0u64;
        POOLS.for_each(|_, pool| {
            if pool.token_a_mint == mint || pool.token_b_mint == mint {
                let liq = pool.reserve_a.min(pool.reserve_b);
                if liq > best_liq { best_liq = liq; best = Some(pool.clone()); }
            }
        });
        best
    }
}
