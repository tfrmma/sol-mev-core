// takes a trading signal, builds ixs, sims, sets compute budget, fires off a jito bundle.
// each strategy has its own ix builder. they're all stubs right now except the skeleton — 
// real account metas need to be plumbed through from registry.
use anyhow::{Context, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    message::{v0, VersionedMessage},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::VersionedTransaction,
};
use std::sync::Arc;
use tracing::{info, warn};

use crate::{
    config::BotConfig,
    jito::{JitoBundle, JitoClient},
    simulator::Simulator,
    state::Dex,
    strategies::{
        arbitrage::ArbPath,
        liquidation::{LiqOpportunity, LiquidationScanner},
        sandwich::SandwichOpportunity,
        TradingSignal,
    },
};

pub struct Executor {
    rpc:       RpcClient,
    jito:      JitoClient,
    simulator: Arc<Simulator>,
    signer:    Keypair,
    config:    BotConfig,
}

impl Executor {
    pub fn new(config: BotConfig, signer: Keypair, simulator: Arc<Simulator>) -> Self {
        let rpc  = RpcClient::new_with_commitment(config.rpc_url.clone(), CommitmentConfig::confirmed());
        // wire spam endpoints from simulator into jito client.
        // executor owns the jito client so this is the natural place to plumb them together.
        let jito = JitoClient::new(&config.jito_url, config.max_retries)
            .with_spam_endpoints(simulator.spam_endpoints.clone());
        Self { rpc, jito, simulator, signer, config }
    }

    pub async fn execute(&self, signal: TradingSignal) -> Result<()> {
        match signal {
            TradingSignal::Arb(path)        => self.execute_arb(path).await,
            TradingSignal::Liquidation(opp) => self.execute_liquidation(opp).await,
            TradingSignal::Sandwich(opp)    => self.execute_sandwich(opp).await,
        }
    }

    async fn execute_arb(&self, path: ArbPath) -> Result<()> {
        info!("arb: {} hops profit={}", path.edges.len(), path.net_profit_lamports);
        let ixs = self.build_arb_ixs(&path)?;
        self.sim_and_send(ixs).await
    }

    async fn execute_liquidation(&self, opp: LiqOpportunity) -> Result<()> {
        info!("liq: obligation={} profit={:?}", opp.obligation, opp.adjusted_profit_lamports);
        // RiskEngine not needed for ix building here, pass a fresh no-op instance
        let scanner = LiquidationScanner::new(
            &self.config,
            self.signer.pubkey(),
            crate::risk::RiskEngine::new(),
        );
        let ix = scanner.build_kamino_liquidation_ix(&opp);
        self.sim_and_send(vec![ix]).await
    }

    async fn execute_sandwich(&self, opp: SandwichOpportunity) -> Result<()> {
        info!("sandwich: victim={} profit={}", opp.victim_sig, opp.estimated_profit);
        let pool = &opp.pool_state;
//! @file executor.rs
//! @author Taha - Algorithmic Trader
//! @brief Institutional-grade sol-mev-core
//! 
//! @note This is a public structural showcase. For full production-grade 
//!       deployment, architecture consulting, or recruitment inquiries:
//!       Contact: email: fadilrezokt@gmail.com / linkedin.com/in/tahaotc
        // 1% slippage tolerance on the frontrun output. don't be too tight or we revert.
        let front_ix = self.swap_ix(
            pool.pool_id, &pool.dex, pool.token_a_mint, pool.token_b_mint,
            opp.frontrun_amount, opp.frontrun_output * 990 / 1000,
        )?;
        let back_ix = self.swap_ix(
            pool.pool_id, &pool.dex, pool.token_b_mint, pool.token_a_mint,
            opp.frontrun_output, opp.frontrun_amount,
        )?;

        let blockhash = self.rpc.get_latest_blockhash().await?;
        let front_tx  = self.sign_tx(vec![front_ix], blockhash).await?;
        let back_tx   = self.sign_tx(vec![back_ix],  blockhash).await?;

        let bundle = JitoBundle {
            transactions: vec![front_tx, back_tx],
            tip_lamports: self.config.jito_tip_lamports,
        }.attach_tip(&self.signer, blockhash);

        let uuid = self.jito.send_bundle(&bundle).await?;
        info!("sandwich bundle {uuid}");
        Ok(())
    }

    async fn sim_and_send(&self, ixs: Vec<Instruction>) -> Result<()> {
        if !self.config.simulate_before_send {
            return self.bundle_and_send(ixs, 100_000).await;
        }

        let sim = self.simulator.simulate(&self.signer, ixs.clone()).await?;
        if !sim.success {
            warn!("preflight failed: {:?}", sim.error);
            return Err(anyhow::anyhow!("simulation failed — aborting"));
        }

        // deduplicate accounts for priority fee query
        let accounts: Vec<Pubkey> = ixs.iter()
            .flat_map(|ix| ix.accounts.iter().map(|m| m.pubkey))
            .collect();
        let fee = self.simulator.suggest_priority_fee(&accounts).await
            .min(self.config.max_cu_price_microlamports);

        let final_ixs = Simulator::wrap_with_compute_budget(ixs, sim.units_consumed, fee);
        self.bundle_and_send(final_ixs, fee).await
    }

    async fn bundle_and_send(&self, ixs: Vec<Instruction>, _fee: u64) -> Result<()> {
        let blockhash = self.rpc.get_latest_blockhash().await?;
        let tx        = self.sign_tx(ixs, blockhash).await?;
        let bundle    = JitoBundle {
            transactions: vec![tx],
            tip_lamports: self.config.jito_tip_lamports,
        }.attach_tip(&self.signer, blockhash);
        let uuid = self.jito.send_bundle(&bundle).await?;
        info!("bundle {uuid}");
        Ok(())
    }

    async fn sign_tx(&self, ixs: Vec<Instruction>, blockhash: solana_sdk::hash::Hash) -> Result<VersionedTransaction> {
        let msg = v0::Message::try_compile(&self.signer.pubkey(), &ixs, &[], blockhash)
            .context("compile v0 message")?;
        VersionedTransaction::try_new(VersionedMessage::V0(msg), &[&self.signer])
            .context("sign transaction")
    }

    fn build_arb_ixs(&self, path: &ArbPath) -> Result<Vec<Instruction>> {
        let mut ixs    = Vec::new();
        let mut amount = path.optimal_input;
        for edge in &path.edges {
            // 0.5% slippage per hop. tight enough to be safe, loose enough not to clip on variance
            let expected = if edge.from_mint == edge.pool_state.token_a_mint {
                edge.pool_state.quote_a_to_b(amount)
            } else {
                edge.pool_state.quote_b_to_a(amount)
            };
            let min_out = (expected * 995 / 1000).max(1);
            ixs.push(self.swap_ix(
                edge.pool_state.pool_id, &edge.pool_state.dex,
                edge.from_mint, edge.to_mint, amount, min_out,
            )?);
            amount = expected;
        }
        Ok(ixs)
    }

    fn swap_ix(&self, pool: Pubkey, dex: &Dex, input: Pubkey, output: Pubkey, amount_in: u64, min_out: u64) -> Result<Instruction> {
        match dex {
            Dex::Raydium       => self.raydium_ix(pool, input, output, amount_in, min_out),
            Dex::Orca          => self.orca_ix(pool, input, output, amount_in, min_out),
            Dex::OrcaWhirlpool => self.whirlpool_ix(pool, input, output, amount_in, min_out),
            // TODO: Lifinity and Meteora. meteora is a pain because of the dynamic fee model.
            _                  => Err(anyhow::anyhow!("{dex:?} not implemented")),
        }
    }

    // NOTE: all account metas are empty stubs. you need to fill these in from
    //       registry PoolMeta (vaults, authority, token program, etc) before this works on-chain.
    fn raydium_ix(&self, _pool: Pubkey, _input: Pubkey, _output: Pubkey, amount_in: u64, min_out: u64) -> Result<Instruction> {
        let pid: Pubkey = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".parse()?;
        let mut data = vec![9u8]; // SwapBaseIn discriminant
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());
        Ok(Instruction { program_id: pid, accounts: vec![], data })
    }

    fn orca_ix(&self, _pool: Pubkey, _input: Pubkey, _output: Pubkey, amount_in: u64, min_out: u64) -> Result<Instruction> {
        let pid: Pubkey = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP".parse()?;
        let mut data = vec![1u8];
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());
        data.push(1); // swap direction flag
        Ok(Instruction { program_id: pid, accounts: vec![], data })
    }

    fn whirlpool_ix(&self, _pool: Pubkey, _input: Pubkey, _output: Pubkey, amount_in: u64, min_out: u64) -> Result<Instruction> {
        let pid: Pubkey  = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".parse()?;
        let disc: [u8;8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8]; // swap anchor discriminant
        let mut data = disc.to_vec();
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());
        data.push(1);                              // a_to_b
        data.extend_from_slice(&0u128.to_le_bytes()); // sqrtPriceLimit = 0 (no limit)
        Ok(Instruction { program_id: pid, accounts: vec![], data })
    }
}
