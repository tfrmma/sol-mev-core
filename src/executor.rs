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
    risk::RiskEngine,
    simulator::Simulator,
    state::{Dex, PoolState, POOLS},
    strategies::{
        arbitrage::ArbPath,
        liquidation::LiqOpportunity,
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
    risk:      Arc<RiskEngine>,
}

impl Executor {
    pub fn new(config: BotConfig, signer: Keypair, simulator: Arc<Simulator>, registry: Registry, risk: Arc<RiskEngine>) -> Self {
        let rpc  = RpcClient::new_with_commitment(config.rpc_url.clone(), CommitmentConfig::confirmed());
        // wire spam endpoints from simulator into jito client.
        // executor owns the jito client so this is the natural place to plumb them together.
        let jito = JitoClient::new(&config.jito_url, config.max_retries)
            .with_spam_endpoints(simulator.spam_endpoints.clone());
        Self { rpc, jito, simulator, signer, config, registry, risk }
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

        use klend_interface::ObligationContext;

        let owner = self.signer.pubkey();

        // 1. fetch the obligation, discover every reserve it references
        let obligation_account = self.rpc.get_account(&opp.obligation).await
            .context("fetch obligation account")?;
        let reserve_addrs = ObligationContext::reserve_addresses_for_obligation(&obligation_account.data)
            .map_err(|e| anyhow::anyhow!("parse obligation reserve list: {e:?}"))?;

        // 2. fetch all of them in one batched RPC call
        let reserve_accounts = self.rpc.get_multiple_accounts(&reserve_addrs).await
            .context("fetch obligation reserves")?;
        let reserve_data: Vec<(Pubkey, &[u8])> = reserve_addrs.iter()
            .zip(reserve_accounts.iter())
            .filter_map(|(addr, acc)| acc.as_ref().map(|a| (*addr, a.data.as_slice())))
            .collect();

        // 3. build the context, then the liquidate instructions (refreshes prepended automatically)
        let ctx = ObligationContext::from_account_data(opp.obligation, &obligation_account.data, &reserve_data)
            .map_err(|e| anyhow::anyhow!("build obligation context: {e:?}"))?;

        // need the repay reserve's liquidity mint to derive our own source ATA, and the
        // withdraw reserve's collateral mint for the destination. the mint comes straight
        // out of the reserve account we already fetched, the collateral mint is a PDA off
        // the reserve pubkey alone, no need to parse the reserve for that part.
        let repay_mint = ctx.reserve_info(&opp.repay_reserve)
            .context("repay reserve not present in fetched obligation context")?
            .liquidity_mint;
        let withdraw_reserve_info = ctx.reserve_info(&opp.withdraw_reserve)
            .context("withdraw reserve not present in fetched obligation context")?;
        // liquidate_and_redeem gives us the withdraw reserve's real underlying asset directly,
        // not a cToken we'd need a separate redeem step for. that's the mint we actually end up
        // holding, and the one that matters for exit-slippage risk below.
        let withdraw_liquidity_mint = withdraw_reserve_info.liquidity_mint;
        let (collateral_mint, _) = klend_interface::pda::reserve_collateral_mint(
            &klend_interface::KLEND_PROGRAM_ID, &opp.withdraw_reserve,
        );

        // risk-adjustment, moved here from liquidation.rs's sync scanner (see the comment that
        // used to be there): now that we have the real withdraw mint from a live reserve fetch,
        // we can actually look up an exit pool and estimate slippage instead of skipping this.
        let collateral_amount = (opp.gross_profit_lamports + opp.repay_amount as i64).max(0) as u64;
        let exit_pool = {
            let mut best: Option<PoolState> = None;
            let mut best_liq = 0u64;
            POOLS.for_each(|_, pool| {
                if pool.token_a_mint == withdraw_liquidity_mint || pool.token_b_mint == withdraw_liquidity_mint {
                    let liq = pool.reserve_a.min(pool.reserve_b);
                    if liq > best_liq { best_liq = liq; best = Some(pool.clone()); }
                }
            });
            best
        };
        if let Some(pool) = &exit_pool {
            let exit_is_a = pool.token_a_mint == withdraw_liquidity_mint;
            match self.risk.adjusted_profit(opp.gross_profit_lamports, withdraw_liquidity_mint, pool, collateral_amount, exit_is_a) {
                Some(adjusted) => info!("liq: risk-adjusted profit {adjusted} lamports (gross {})", opp.gross_profit_lamports),
                None => return Err(anyhow::anyhow!(
                    "liq: risk engine vetoed obligation {} (circuit breaker or unprofitable after haircut/slippage/fee)",
                    opp.obligation
                )),
            }
        } else {
            // no tracked pool for this mint, can't estimate exit cost. proceed on gross profit
            // alone rather than block every liquidation just because registry.json is thin,
            // but this is exactly the blind spot risk-adjustment exists to catch, so make noise.
            warn!("liq: no exit pool found for {withdraw_liquidity_mint}, proceeding on unadjusted gross profit");
        }

        let user_source_liquidity      = derive_ata(&owner, &repay_mint)?;
        let user_destination_collateral = derive_ata(&owner, &collateral_mint)?;
        // the redeemed underlying asset lands in withdraw_liquidity_mint, NOT repay_mint,
        // those only coincide for a same-asset liquidation. got this wrong on the first pass.
        let user_destination_liquidity  = derive_ata(&owner, &withdraw_liquidity_mint)?;

        // min_received left at 1: this is a liquidation, not a swap, we want it to land even
        // on a thin bonus rather than revert. real slippage protection belongs in whether we
        // decided to liquidate at all (see gross_profit_lamports upstream), not here.
        let ixs = ctx.liquidate(
            owner, &opp.repay_reserve, &opp.withdraw_reserve,
            user_source_liquidity, user_destination_collateral, user_destination_liquidity,
            opp.repay_amount, 1, 0,
        ).map_err(|e| anyhow::anyhow!("build liquidate instructions: {e:?}"))?;

        self.sim_and_send(ixs).await
    }

    async fn execute_sandwich(&self, opp: SandwichOpportunity) -> Result<()> {
        info!("sandwich: victim={} profit={}", opp.victim_sig, opp.estimated_profit);
        let pool = &opp.pool_state;
        // 1% slippage tolerance on the frontrun output. don't be too tight or we revert.
        let front_ix = self.swap_ix(
            pool, pool.token_a_mint, pool.token_b_mint,
            opp.frontrun_amount, opp.frontrun_output * 990 / 1000,
        )?;
        let back_ix = self.swap_ix(
            pool, pool.token_b_mint, pool.token_a_mint,
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
        self.risk.update_fee_p95(fee); // keep the risk engine's profitability estimate current instead of stuck at its startup default

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
        debug_assert_eq!(
            path.edges.first().map(|e| e.from_mint), Some(path.input_mint),
            "ArbPath.input_mint out of sync with its own first edge, scanner bug upstream"
        );
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
                &edge.pool_state,
                edge.from_mint, edge.to_mint, amount, min_out,
            )?);
            amount = expected;
        }
        Ok(ixs)
    }

    fn swap_ix(&self, pool: &PoolState, input: Pubkey, output: Pubkey, amount_in: u64, min_out: u64) -> Result<Instruction> {
        let meta = self.registry.pool_meta(&pool.pool_id)
            .with_context(|| format!("no registry entry for pool {}, can't build accounts", pool.pool_id))?;
        match &pool.dex {
            Dex::Raydium       => self.raydium_ix(&meta, input, output, amount_in, min_out),
            Dex::Orca          => self.orca_ix(&meta, input, output, amount_in, min_out),
            Dex::OrcaWhirlpool => self.whirlpool_ix(&meta, pool, input, output, amount_in, min_out),
            Dex::Meteora       => self.meteora_ix(&meta, input, output, amount_in, min_out),
            // Lifinity: no official public source found anywhere (checked Lifinity Labs' own
            // github org, 5 repos, none of them the swap program itself). only third-party
            // transaction-parser inference exists (solparser). not implementing from a guess.
            dex                => Err(anyhow::anyhow!("{dex:?} not implemented, see notes above")),
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

    // legacy SPL token-swap program (orca's original AMM, pre-whirlpool). verified against
    // solana-labs/solana-program-library/token-swap/program/src/instruction.rs: tag=1 (Swap),
    // data = tag(1) + amount_in(8) + minimum_amount_out(8) = 17 bytes, 13 accounts (+1 optional
    // host_fee, omitted here).
    //
    // needs pool_mint and pool_fee_account, which PoolMeta doesn't have dedicated fields for.
    // convention: extra_accounts[0] = pool_mint, extra_accounts[1] = pool_fee_account. document
    // this in registry.json if you're populating orca legacy pools by hand.
    fn orca_ix(&self, meta: &PoolMeta, input: Pubkey, output: Pubkey, amount_in: u64, min_out: u64) -> Result<Instruction> {
        let program_id = meta.program_pubkey().context("bad program_id in registry")?;
        let swap_pubkey = meta.pool_pubkey().context("bad pool_id in registry")?;
        let vault_a     = meta.vault_a_pk().context("bad token_a_vault in registry")?;
        let vault_b     = meta.vault_b_pk().context("bad token_b_vault in registry")?;
        let mint_a      = meta.token_a_mint_pk().context("bad token_a_mint in registry")?;
        let _mint_b     = meta.token_b_mint_pk().context("bad token_b_mint in registry")?; // validated, not otherwise needed here
        let extra = meta.extra_pubkeys();
        let pool_mint = *extra.first().context("orca legacy pool missing extra_accounts[0]=pool_mint")?;
        let pool_fee  = *extra.get(1).context("orca legacy pool missing extra_accounts[1]=pool_fee_account")?;
        let token_program: Pubkey = SPL_TOKEN_PROGRAM_ID.parse()?;
        let owner = self.signer.pubkey();

        // authority is a PDA seeded on just the swap account, the on-chain nonce was generated
        // the same way at pool init time, find_program_address reproduces it deterministically.
        let (authority, _bump) = Pubkey::find_program_address(&[swap_pubkey.as_ref()], &program_id);

        let a_to_b = input == mint_a;
        let (swap_source, swap_dest) = if a_to_b { (vault_a, vault_b) } else { (vault_b, vault_a) };

        let mut data = vec![1u8]; // SwapInstruction::Swap
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());

        let accounts = vec![
            AccountMeta::new_readonly(swap_pubkey, false),
            AccountMeta::new_readonly(authority, false),
            AccountMeta::new_readonly(owner, true), // user_transfer_authority: self-signing, no separate delegate
            AccountMeta::new(derive_ata(&owner, &input)?, false),
            AccountMeta::new(swap_source, false),
            AccountMeta::new(swap_dest, false),
            AccountMeta::new(derive_ata(&owner, &output)?, false),
            AccountMeta::new(pool_mint, false),
            AccountMeta::new(pool_fee, false),
            AccountMeta::new_readonly(input, false),
            AccountMeta::new_readonly(output, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(token_program, false),
        ];

        Ok(Instruction { program_id, accounts, data })
    }

    // meteora DAMM v2 (cp-amm) swap, verified against the real SwapCtx account struct (source
    // provided directly, not searched: programs/cp-amm/src/instructions/ix_swap.rs). 14 accounts:
    // the 12 named in #[derive(Accounts)] SwapCtx, plus event_authority + program that the
    // #[event_cpi] macro appends automatically (standard anchor-lang convention, same on every
    // program using that macro, not meteora-specific).
    //
    // ONE PLACEHOLDER LEFT: pool_authority is a single global PDA (`address =
    // const_pda::pool_authority::ID` in their source, not derived per-pool), but I don't have
    // its actual value confirmed, the source excerpt I got doesn't include const_pda.rs. DO NOT
    // use this until that's filled in, everything else here is real.
    //
    // known limitations even once pool_authority is filled in:
    //   - token_a_program/token_b_program default to classic SPL Token below. token2022 pools
    //     need these overridden (the vault's actual owner program), not detected here.
    //   - referral_token_account: per validate_p_accounts, "no referral" means passing the
    //     program's own ID as this account, not omitting it. done below.
    //   - rate-limiter-mode pools additionally need SYSVAR_INSTRUCTIONS in remaining accounts,
    //     not handled, this will fail on those specific pools until it is.
    //
    // ALSO: even once pool_authority is filled in, this is unreachable in practice today.
    // monitor.rs has no decode_meteora_pool, so no PoolState ever gets built with Dex::Meteora,
    // and this dispatch arm never fires. that decoder is the next real piece, not this function.
    fn meteora_ix(&self, meta: &PoolMeta, input: Pubkey, output: Pubkey, amount_in: u64, min_out: u64) -> Result<Instruction> {
        // NOT REAL, deliberately an invalid pubkey string so this fails loudly and immediately
        // on the .parse() below rather than silently building a wrong, guaranteed-to-revert
        // transaction with a placeholder that happens to parse. replace with the real
        // pool_authority PDA (const_pda::pool_authority::ID in Meteora's source) before this
        // can run. everything else in this function is verified and ready.
        const POOL_AUTHORITY_NOT_YET_FILLED_IN: &str = "REPLACE_WITH_REAL_pool_authority_PDA";
        let pool_authority: Pubkey = POOL_AUTHORITY_NOT_YET_FILLED_IN.parse()
            .context("meteora_ix: pool_authority PDA not filled in yet, see comment above meteora_ix")?;

        let program_id = meta.program_pubkey().context("bad program_id in registry")?;
        let pool       = meta.pool_pubkey().context("bad pool_id in registry")?;
        let vault_a    = meta.vault_a_pk().context("bad token_a_vault in registry")?;
        let vault_b    = meta.vault_b_pk().context("bad token_b_vault in registry")?;
        let mint_a     = meta.token_a_mint_pk().context("bad token_a_mint in registry")?;
        let mint_b     = meta.token_b_mint_pk().context("bad token_b_mint in registry")?;
        let token_program: Pubkey = SPL_TOKEN_PROGRAM_ID.parse()?; // see token2022 caveat above
        let owner = self.signer.pubkey();

        let (event_authority, _) = Pubkey::find_program_address(&[b"__event_authority"], &program_id);

        const SWAP_DISCRIMINATOR: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8]; // sha256("global:swap")[..8]
        let mut data = SWAP_DISCRIMINATOR.to_vec();
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());

        let accounts = vec![
            AccountMeta::new_readonly(pool_authority, false),
            AccountMeta::new(pool, false),
            AccountMeta::new(derive_ata(&owner, &input)?, false),
            AccountMeta::new(derive_ata(&owner, &output)?, false),
            AccountMeta::new(vault_a, false),
            AccountMeta::new(vault_b, false),
            AccountMeta::new_readonly(mint_a, false),
            AccountMeta::new_readonly(mint_b, false),
            AccountMeta::new_readonly(owner, true),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new(program_id, false), // referral_token_account: "none" sentinel per validate_p_accounts
            AccountMeta::new_readonly(event_authority, false),
            AccountMeta::new_readonly(program_id, false),
        ];

        Ok(Instruction { program_id, accounts, data })
    }

    // whirlpool swap, verified against orca-so/whirlpools/programs/whirlpool/src/instructions/swap.rs
    // (main branch). 11 accounts, discriminator computed as sha256("global:swap")[..8] rather than
    // copied from memory, since that's the one part of an anchor ix nobody should be typing by hand.
    //
    // single caveat carried over from quote_clmm in state.rs: this only walks the tick arrays that
    // contain the pool's *current* tick. if the trade is big enough to cross into the next array,
    // the swap will still succeed on-chain (the program crosses ticks correctly, it doesn't need our
    // help there) but our quote_a_to_b sizing upstream in arbitrage.rs was computed assuming no
    // crossing, so on a big trade the real output may come in worse than what we sized against.
    fn whirlpool_ix(&self, meta: &PoolMeta, pool: &PoolState, input: Pubkey, output: Pubkey, amount_in: u64, min_out: u64) -> Result<Instruction> {
        let clmm = pool.clmm.as_ref().context("whirlpool pool_state missing clmm data, decode didn't run?")?;
        let program_id = meta.program_pubkey().context("bad program_id in registry")?;
        let whirlpool   = meta.pool_pubkey().context("bad pool_id in registry")?;
        let vault_a     = meta.vault_a_pk().context("bad token_a_vault in registry")?;
        let vault_b     = meta.vault_b_pk().context("bad token_b_vault in registry")?;
        let mint_a      = meta.token_a_mint_pk().context("bad token_a_mint in registry")?;
        let mint_b      = meta.token_b_mint_pk().context("bad token_b_mint in registry")?;
        let token_program: Pubkey = SPL_TOKEN_PROGRAM_ID.parse()?;
        let owner = self.signer.pubkey();
        let a_to_b = input == mint_a;

        // three tick arrays around the current tick, seeds verified against
        // instructions/initialize_tick_array.rs: [b"tick_array", whirlpool, start_tick.to_string()].
        // note the seed is the ascii decimal string of the tick, not raw bytes, an orca quirk.
        const TICK_ARRAY_SIZE: i32 = 88;
        let ticks_per_array = TICK_ARRAY_SIZE * clmm.tick_spacing as i32;
        let base_start = clmm.tick_current.div_euclid(ticks_per_array) * ticks_per_array;
        let tick_array_pda = |start: i32| -> Result<Pubkey> {
            let start_str = start.to_string();
            let seeds = [b"tick_array".as_ref(), whirlpool.as_ref(), start_str.as_bytes()];
            Ok(Pubkey::find_program_address(&seeds, &program_id).0)
        };
        // walk outward in the swap direction, same convention the SDK uses for tick_array_1/2.
        let (start_1, start_2) = if a_to_b {
            (base_start - ticks_per_array, base_start - 2 * ticks_per_array)
        } else {
            (base_start + ticks_per_array, base_start + 2 * ticks_per_array)
        };
        let tick_array_0 = tick_array_pda(base_start)?;
        let tick_array_1 = tick_array_pda(start_1)?;
        let tick_array_2 = tick_array_pda(start_2)?;

        let oracle_seeds = [b"oracle".as_ref(), whirlpool.as_ref()];
        let oracle = Pubkey::find_program_address(&oracle_seeds, &program_id).0;

        // bounds verified against orca-so/whirlpools/programs/whirlpool/src/math/tick_math.rs.
        // passing 0 here isn't "no limit", it's out of bounds and the program rejects it outright.
        const MIN_SQRT_PRICE_X64: u128 = 4_295_048_016;
        const MAX_SQRT_PRICE_X64: u128 = 79_226_673_515_401_279_992_447_579_055;
        let sqrt_price_limit = if a_to_b { MIN_SQRT_PRICE_X64 } else { MAX_SQRT_PRICE_X64 };

        const SWAP_DISCRIMINATOR: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];
        let mut data = SWAP_DISCRIMINATOR.to_vec();
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());
        data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
        data.push(1); // amount_specified_is_input: true, amount_in is exact
        data.push(a_to_b as u8);

        let accounts = vec![
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(owner, true),
            AccountMeta::new(whirlpool, false),
            AccountMeta::new(derive_ata(&owner, &mint_a)?, false),
            AccountMeta::new(vault_a, false),
            AccountMeta::new(derive_ata(&owner, &mint_b)?, false),
            AccountMeta::new(vault_b, false),
            AccountMeta::new(tick_array_0, false),
            AccountMeta::new(tick_array_1, false),
            AccountMeta::new(tick_array_2, false),
            AccountMeta::new_readonly(oracle, false),
        ];

        let _ = output; // direction comes from a_to_b / meta.token_a_mint, not from comparing to `output`

        Ok(Instruction { program_id, accounts, data })
    }
}
