// takes a trading signal, builds ixs, sims, sets compute budget, fires off a jito bundle.
// each strategy has its own ix builder. they're all stubs right now except the skeleton, 
// real account metas need to be plumbed through from registry.
use anyhow::{Context, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::{AccountMeta, Instruction},
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
    registry::{PoolMeta, Registry},
    simulator::Simulator,
    state::Dex,
    strategies::{
        arbitrage::ArbPath,
        liquidation::{LiqOpportunity, LiquidationScanner},
        sandwich::SandwichOpportunity,
        TradingSignal,
    },
};

// well-known, program-independent constants. these never change per-pool.
const SPL_TOKEN_PROGRAM_ID:        &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
// single global authority shared by every raydium AMM v4 pool, not per-pool.
// verified against docs.raydium.io/reference/program-addresses.
const RAYDIUM_AMM_V4_AUTHORITY: &str = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1";

// derives a wallet's associated token account without pulling in spl-associated-token-account,
// which is what dragged us into the whole solana-zk-token-sdk version hell on 1.18. this is just
// the standard ATA PDA, seeds = [wallet, token_program, mint], nothing exotic.
fn derive_ata(wallet: &Pubkey, mint: &Pubkey) -> Result<Pubkey> {
    let token_program: Pubkey = SPL_TOKEN_PROGRAM_ID.parse()?;
    let ata_program:   Pubkey = ASSOCIATED_TOKEN_PROGRAM_ID.parse()?;
    let seeds = [wallet.as_ref(), token_program.as_ref(), mint.as_ref()];
    Ok(Pubkey::find_program_address(&seeds, &ata_program).0)
}

pub struct Executor {
    rpc:       RpcClient,
    jito:      JitoClient,
    simulator: Arc<Simulator>,
    signer:    Keypair,
    config:    BotConfig,
    registry:  Registry,
}

impl Executor {
    pub fn new(config: BotConfig, signer: Keypair, simulator: Arc<Simulator>, registry: Registry) -> Self {
        let rpc  = RpcClient::new_with_commitment(config.rpc_url.clone(), CommitmentConfig::confirmed());
        // wire spam endpoints from simulator into jito client.
        // executor owns the jito client so this is the natural place to plumb them together.
        let jito = JitoClient::new(&config.jito_url, config.max_retries)
            .with_spam_endpoints(simulator.spam_endpoints.clone());
        Self { rpc, jito, simulator, signer, config, registry }
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
            return Err(anyhow::anyhow!("simulation failed, aborting"));
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
        let meta = self.registry.pool_meta(&pool)
            .with_context(|| format!("no registry entry for pool {pool}, can't build accounts"))?;
        match dex {
            Dex::Raydium       => self.raydium_ix(&meta, input, output, amount_in, min_out),
            Dex::Orca          => self.orca_ix(&meta, input, output, amount_in, min_out),
            Dex::OrcaWhirlpool => self.whirlpool_ix(&meta, input, output, amount_in, min_out),
            // TODO: Lifinity and Meteora. meteora is a pain because of the dynamic fee model.
            _                  => Err(anyhow::anyhow!("{dex:?} not implemented")),
        }
    }

    // SwapBaseInV2 (tag 16), verified against raydium-io/raydium-amm program/src/instruction.rs.
    // migrated off the 18-account OpenBook-era layout in sept 2025, this is the current path.
    //
    // assumption baked into this: registry.token_a_vault is raydium's "coin" vault and
    // token_b_vault is "pc" vault, matching whatever order token_a/token_b got assigned when
    // the pool was registered. if swaps start reverting with a mint mismatch, check that first.
    fn raydium_ix(&self, meta: &PoolMeta, input: Pubkey, output: Pubkey, amount_in: u64, min_out: u64) -> Result<Instruction> {
        let program_id = meta.program_pubkey().context("bad program_id in registry")?;
        let pool_id     = meta.pool_pubkey().context("bad pool_id in registry")?;
        let coin_vault  = meta.vault_a_pk().context("bad token_a_vault in registry")?;
        let pc_vault    = meta.vault_b_pk().context("bad token_b_vault in registry")?;
        let authority: Pubkey     = RAYDIUM_AMM_V4_AUTHORITY.parse()?;
        let token_program: Pubkey = SPL_TOKEN_PROGRAM_ID.parse()?;
        let owner = self.signer.pubkey();

        let mut data = vec![16u8]; // SwapBaseInV2 discriminant
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());

        let accounts = vec![
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new(pool_id, false),
            AccountMeta::new_readonly(authority, false),
            AccountMeta::new(coin_vault, false),
            AccountMeta::new(pc_vault, false),
            AccountMeta::new(derive_ata(&owner, &input)?, false),
            AccountMeta::new(derive_ata(&owner, &output)?, false),
            AccountMeta::new_readonly(owner, true),
        ];

        Ok(Instruction { program_id, accounts, data })
    }

    // TODO(#4): orca legacy token-swap accounts, needs a source-verified pass same as raydium above.
    //           not doing it from memory, spl-token-swap's account order has to be checked against
    //           solana-labs/solana-program-library/docs/src/token-swap.md before this ships.
    fn orca_ix(&self, _meta: &PoolMeta, _input: Pubkey, _output: Pubkey, _amount_in: u64, _min_out: u64) -> Result<Instruction> {
        Err(anyhow::anyhow!("orca ix builder not implemented yet, see TODO(#4)"))
    }

    // TODO(#4): whirlpool needs tick array accounts computed from the pool's current tick index
    //           and tick spacing, which decode_pool() doesn't extract yet (monitor.rs only handles
    //           raydium today). building this ix before that lands means guessing tick arrays,
    //           which just fails on-chain. blocked on the whirlpool decoder, not on this function.
    fn whirlpool_ix(&self, _meta: &PoolMeta, _input: Pubkey, _output: Pubkey, _amount_in: u64, _min_out: u64) -> Result<Instruction> {
        Err(anyhow::anyhow!("whirlpool ix builder blocked on whirlpool pool decoding, see TODO(#4)"))
    }
}
