pub mod arbitrage;
pub mod liquidation;
pub mod sandwich;

use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

use crate::config::BotConfig;
use crate::monitor::Opportunity;
use crate::risk::RiskEngine;
use crate::smart_money::SmartMoneyClassifier;
use crate::strategies::{
    arbitrage::ArbitrageScanner,
    liquidation::LiquidationScanner,
    sandwich::SandwichDetector,
};

#[derive(Debug)]
pub enum TradingSignal {
    Arb(arbitrage::ArbPath),
    Liquidation(liquidation::LiqOpportunity),
    Sandwich(sandwich::SandwichOpportunity),
}

pub struct StrategyEngine {
    arb:      Option<ArbitrageScanner>,
    liq:      Option<LiquidationScanner>,
    sandwich: Option<SandwichDetector>,
}

impl StrategyEngine {
    pub fn new(
        config:      &BotConfig,
        signer:      Pubkey,
        risk:        Arc<RiskEngine>,
        smart_money: Arc<SmartMoneyClassifier>,
    ) -> Self {
        Self {
            arb:      config.enable_arbitrage  .then(|| ArbitrageScanner::new(config)),
            liq:      config.enable_liquidation.then(|| LiquidationScanner::new(config, signer, risk.clone())),
            sandwich: config.enable_sandwich   .then(|| SandwichDetector::new(config, risk, smart_money)),
        }
    }

    pub fn process(&self, opp: Opportunity) -> Option<TradingSignal> {
        match opp {
            Opportunity::PoolUpdated(pk)       => self.arb     .as_ref()?.scan(pk)       .map(TradingSignal::Arb),
            Opportunity::ObligationUpdated(pk) => self.liq     .as_ref()?.evaluate(pk)   .map(TradingSignal::Liquidation),
            Opportunity::PendingSwap(swap)     => self.sandwich.as_ref()?.evaluate(&swap) .map(TradingSignal::Sandwich),
        }
    }

    // full sweep over every tracked obligation, not just the one that triggered an update.
    // catches positions that were already underwater but haven't had a fresh account update
    // land recently, evaluate() alone can't see those.
    pub fn sweep_liquidations(&self) -> Vec<TradingSignal> {
        self.liq.as_ref()
            .map(|liq| liq.scan_all().into_iter().map(TradingSignal::Liquidation).collect())
            .unwrap_or_default()
    }
}
