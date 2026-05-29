mod config;
mod executor;
mod jito;
mod monitor;
mod registry;
mod risk;
mod simulator;
mod smart_money;
mod state;
mod strategies;

use anyhow::Result;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::{
    config::BotConfig,
    executor::Executor,
    monitor::{Monitor, Opportunity},
    registry::Registry,
    risk::RiskEngine,
    simulator::{AccountCache, RpcSimulator, Simulator},
    smart_money::SmartMoneyClassifier,
    strategies::{StrategyEngine, TradingSignal},
};

const OPP_CHAN: usize        = 512;
const SIGNAL_CHAN: usize     = 128;
const REGISTRY_TTL: Duration = Duration::from_secs(300); // 5min refresh. slow enough to not hammer disk.

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,solana_mev_bot=debug")),
        )
        .with_target(true)
        .with_thread_ids(true)
        .init();

    let config    = BotConfig::from_env()?;
    let signer    = config.load_keypair()?;
    let signer_pk = signer.try_pubkey()?;

    info!("signer={signer_pk} rpc={} arb={} liq={} sandwich={}",
          config.rpc_url, config.enable_arbitrage, config.enable_liquidation, config.enable_sandwich);

    let registry = Registry::load(Path::new("registry.json"))?;
    let risk     = RiskEngine::new();
    let sm       = SmartMoneyClassifier::new();
    let cache    = AccountCache::new();

    {
        info!("warming account cache…");
        let rpc_sim   = RpcSimulator::new(&config.rpc_url);
        let pool_keys = registry.active_program_ids();
        match rpc_sim.warm_cache(&pool_keys, &cache).await {
            Ok(n)  => info!("cache warm: {n} accounts"),
            Err(e) => tracing::warn!("partial cache warm: {e:#}"), // non-fatal, local sim will miss and fall back
        }
    }

    let simulator = Arc::new(Simulator::new(&config.rpc_url, cache.clone()));
    let (opp_tx, opp_rx)     = mpsc::channel::<Opportunity>(OPP_CHAN);
    let (sig_tx, mut sig_rx) = mpsc::channel::<TradingSignal>(SIGNAL_CHAN);

    // monitor: geyser stream → opportunity channel
    {
        let (risk, sm, cache, reg) = (risk.clone(), sm.clone(), cache.clone(), registry.clone());
        let (url, token, tx)       = (config.geyser_url.clone(), config.geyser_token.clone(), opp_tx);
        tokio::spawn(async move {
            let mon = Monitor::new_with_hooks(&url, &token, tx, risk, sm, cache, reg);
            loop {
                if let Err(e) = mon.run().await { error!("monitor: {e:#}"); }
            }
        });
    }

    // strategy engine: opportunity → signal
    {
        let (cfg, risk, sm) = (config.clone(), risk.clone(), sm.clone());
        let sig_tx          = sig_tx.clone();
        let mut opp_rx      = opp_rx;
        tokio::spawn(async move {
            let engine = StrategyEngine::new(&cfg, signer_pk, risk, sm);
            while let Some(opp) = opp_rx.recv().await {
                if let Some(sig) = engine.process(opp) {
                    if sig_tx.send(sig).await.is_err() { break; }
                }
            }
        });
    }

    // executor: signal → jito bundle
    {
        let (cfg, sim) = (config.clone(), simulator.clone());
        tokio::spawn(async move {
            let signer   = cfg.load_keypair().expect("keypair");
            let executor = Executor::new(cfg, signer, sim);
            while let Some(sig) = sig_rx.recv().await {
                if let Err(e) = executor.execute(sig).await { error!("executor: {e:#}"); }
            }
        });
    }

    // registry heartbeat — just logs pool count for now. add reload logic here if needed.
    {
        let reg = registry.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(REGISTRY_TTL);
            loop {
                ticker.tick().await;
                info!("registry: {} pools active", reg.pool_count());
            }
        });
    }

    // diagnostics: top smart money wallets + highest vol asset every minute
    {
        let (risk, sm) = (risk.clone(), sm.clone());
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            loop {
                ticker.tick().await;
                for (addr, score) in sm.top_smart_money(5) {
                    info!("smart money {addr}  score={score:.3}");
                }
                let mut vols = risk.volatility_report();
                vols.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                if let Some((mint, sigma)) = vols.first() {
                    if *sigma > 0.01 { info!("highest vol {mint}  σ={sigma:.4}"); }
                }
            }
        });
    }

    info!("running — ctrl+c to stop");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
