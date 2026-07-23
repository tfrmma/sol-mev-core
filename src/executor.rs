// geyser subscriber. account updates → pool/obligation state.
// tx updates → smart money classification + pending swap extraction.
// reconnects forever on error because the stream drops periodically and that's normal.
//
// FILTERING STRATEGY:
//   1. server-side: geyser `owner` filter drops everything not owned by our program IDs.
//      this is free, the validator does it before the packet hits the network.
//   2. client-side first gate: check account data length before touching any offsets.
//   3. discriminant check: first 8 bytes must match the expected account type.
//      bails out before any field parsing on garbage/unrelated accounts.
use anyhow::Result;
use futures::StreamExt;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterTransactions,
};

use crate::{
    registry::Registry,
    risk::RiskEngine,
    simulator::AccountCache,
    smart_money::SmartMoneyClassifier,
    state::{ClmmState, Dex, LendingProtocol, ObligationState, PoolState, CURRENT_SLOT, OBLIGATIONS, POOLS},
};

// program IDs, keep in sync with registry.rs defaults
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_SWAP_V2:   &str = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const KAMINO_LENDING: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"; // verified against Kamino-Finance/klend README
const SOLEND_PROGRAM: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";
const METEORA_CPAMM:  &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"; // DAMM v2, verified against Meteora's own docs

// parsed once at startup, account updates come in fast enough that we don't want
// to allocate a String and compare text for every single one.
static RAYDIUM_AMM_V4_PK: once_cell::sync::Lazy<Pubkey> = once_cell::sync::Lazy::new(|| RAYDIUM_AMM_V4.parse().unwrap());
static ORCA_SWAP_V2_PK:   once_cell::sync::Lazy<Pubkey> = once_cell::sync::Lazy::new(|| ORCA_SWAP_V2.parse().unwrap());
static ORCA_WHIRLPOOL_PK: once_cell::sync::Lazy<Pubkey> = once_cell::sync::Lazy::new(|| ORCA_WHIRLPOOL.parse().unwrap());
static KAMINO_LENDING_PK: once_cell::sync::Lazy<Pubkey> = once_cell::sync::Lazy::new(|| KAMINO_LENDING.parse().unwrap());
static SOLEND_PROGRAM_PK: once_cell::sync::Lazy<Pubkey> = once_cell::sync::Lazy::new(|| SOLEND_PROGRAM.parse().unwrap());
static METEORA_CPAMM_PK:  once_cell::sync::Lazy<Pubkey> = once_cell::sync::Lazy::new(|| METEORA_CPAMM.parse().unwrap());

// Raydium AMM v4 account discriminant, first 8 bytes of the on-chain layout.
// verify with: solana account <pool> --output json | head. if this changes, raydium redeployed.
const RAYDIUM_POOL_DISC: [u8; 8] = [0x01, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00];
// minimum sane data sizes, saves us from indexing into garbage buffers
const RAYDIUM_POOL_MIN_LEN: usize  = 0x1A0;

// dex/output_mint/slot aren't read by SandwichDetector::evaluate today (it derives what it
// needs from swap.pool + the registry), kept for logging and for when sandwich detection
// supports more than one dex at once.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PendingSwap {
    pub signature:      String,
    pub user:           Pubkey,
    pub pool:           Pubkey,
    pub dex:            Dex,
    pub input_mint:     Pubkey,
    pub output_mint:    Pubkey,
    pub amount_in:      u64,
    pub min_amount_out: u64,
    pub slot:           u64,
}

#[derive(Debug)]
pub enum Opportunity {
    PoolUpdated(Pubkey),
    ObligationUpdated(Pubkey),
    PendingSwap(PendingSwap),
}

pub struct Monitor {
    endpoint:      String,
    token:         String,
    tx:            mpsc::Sender<Opportunity>,
    risk:          Option<Arc<RiskEngine>>,
    smart_money:   Option<Arc<SmartMoneyClassifier>>,
    account_cache: Option<Arc<AccountCache>>,
    registry:      Option<Registry>,
}

impl Monitor {
    // bare-bones constructor, no strategy hooks wired in. main.rs uses new_with_hooks instead,
    // this is here for anything that just wants raw account/tx streaming without the rest.
    #[allow(dead_code)]
    pub fn new(endpoint: &str, token: &str, tx: mpsc::Sender<Opportunity>) -> Self {
        Self {
            endpoint: endpoint.to_string(), token: token.to_string(), tx,
            risk: None, smart_money: None, account_cache: None, registry: None,
        }
    }

    pub fn new_with_hooks(
        endpoint:      &str,
        token:         &str,
        tx:            mpsc::Sender<Opportunity>,
        risk:          Arc<RiskEngine>,
        smart_money:   Arc<SmartMoneyClassifier>,
        account_cache: Arc<AccountCache>,
        registry:      Registry,
    ) -> Self {
        Self {
            endpoint: endpoint.to_string(), token: token.to_string(), tx,
            risk: Some(risk), smart_money: Some(smart_money),
            account_cache: Some(account_cache), registry: Some(registry),
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("connecting to geyser {}", self.endpoint);
        loop {
            match self.stream_loop().await {
                Ok(_)  => info!("geyser stream closed, reconnecting"),
                Err(e) => warn!("geyser error: {e}, reconnecting in 500ms"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    async fn stream_loop(&self) -> Result<()> {
        let mut client = GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
            .x_token(Some(self.token.clone()))?
            .tls_config(yellowstone_grpc_client::ClientTlsConfig::new().with_native_roots())?
            .connect()
            .await?;

        let program_ids = self.registry
            .as_ref()
            .map(|r| r.active_program_id_strings())
            .unwrap_or_else(|| vec![
                // fallback hardcoded defaults. ugly but fine for dev.
                RAYDIUM_AMM_V4.to_string(), ORCA_SWAP_V2.to_string(),
                ORCA_WHIRLPOOL.to_string(), KAMINO_LENDING.to_string(),
                SOLEND_PROGRAM.to_string(),
            ]);

        // server-side filter: only accounts owned by our AMM/lending programs.
        // this is the biggest lever, geyser drops the rest before sending anything over gRPC.
        //
        // NOTE: memcmp discriminant filters (offset=0, 8-byte match) would further reduce
        // traffic but the exact proto path varies between yellowstone-grpc versions.
        // TODO: add memcmp once we pin the yellowstone version and verify the generated types:
        //   subscribe_request_filter_accounts_filter::Filter::Memcmp with
        //   SubscribeRequestFilterAccountsFilterMemcmp { offset: 0, data: RAYDIUM_POOL_DISC }
        let mut accounts_filter = HashMap::new();

        accounts_filter.insert("raydium_pools".to_string(), SubscribeRequestFilterAccounts {
            account: vec![],
            owner:   vec![RAYDIUM_AMM_V4.to_string()],
            filters: vec![],
            ..Default::default()
        });

        accounts_filter.insert("orca_pools".to_string(), SubscribeRequestFilterAccounts {
            account: vec![], owner: vec![ORCA_SWAP_V2.to_string(), ORCA_WHIRLPOOL.to_string()],
            filters: vec![], ..Default::default()
        });

        accounts_filter.insert("obligations".to_string(), SubscribeRequestFilterAccounts {
            account: vec![], owner: vec![KAMINO_LENDING.to_string(), SOLEND_PROGRAM.to_string()],
            filters: vec![], ..Default::default()
        });

        let mut tx_filter = HashMap::new();
        tx_filter.insert("swap_txs".to_string(), SubscribeRequestFilterTransactions {
            vote:             Some(false),
            failed:           Some(false),
            account_include:  program_ids,
            account_exclude:  vec![],
            account_required: vec![],
            ..Default::default()
        });

        let request = SubscribeRequest {
            accounts:    accounts_filter,
            transactions: tx_filter,
            commitment:  Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        };

        let (_, mut stream) = client.subscribe_with_request(Some(request)).await?;
        info!("geyser stream active");

        while let Some(msg) = stream.next().await {
            match msg {
                Ok(update) => {
                    if let Some(u) = update.update_oneof { self.dispatch(u).await; }
                }
                Err(e) => {
                    error!("stream error: {e}");
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    async fn dispatch(&self, update: yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof) {
        use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
        match update {
            UpdateOneof::Account(u)     => if let Some(acc) = u.account { self.on_account(u.slot, acc).await; }
            UpdateOneof::Transaction(u) => if let Some(tx)  = u.transaction { self.on_transaction(u.slot, tx).await; }
            UpdateOneof::Slot(u)        => { CURRENT_SLOT.store(u.slot, std::sync::atomic::Ordering::Relaxed); }
            _ => {}
        }
    }

    async fn on_account(&self, slot: u64, acc: yellowstone_grpc_proto::prelude::SubscribeUpdateAccountInfo) {
        let Ok(pubkey) = Pubkey::try_from(acc.pubkey.as_slice()) else { return };
        let Ok(owner)  = Pubkey::try_from(acc.owner.as_slice())  else { return };

        // apply delta to account cache regardless of account type.
        // this is the path for state-delta updates: geyser sends the full account data
        // on every write, so upsert here keeps the sim cache current without a separate fetch.
        if let Some(ref cache) = self.account_cache {
            cache.apply_delta(pubkey, acc.lamports, acc.data.clone(), owner);
        }

        if is_amm_owner(&owner) {
            if let Some(pool) = decode_pool(pubkey, &owner, &acc.data, slot) {
                if let Some(ref risk) = self.risk { risk.on_pool_update(&pool); }
                POOLS.insert(pubkey, pool);
                let _ = self.tx.try_send(Opportunity::PoolUpdated(pubkey));
            }
        } else if is_lending_owner(&owner) {
            if let Some(obl) = decode_obligation(pubkey, &owner, &acc.data, slot) {
                if obl.is_underwater() {
                    info!("liquidation candidate {} ltv={}", pubkey, obl.ltv_bps());
                }
                OBLIGATIONS.insert(pubkey, obl);
                let _ = self.tx.try_send(Opportunity::ObligationUpdated(pubkey));
            }
        }
    }

    async fn on_transaction(
        &self,
        slot: u64,
        tx:   yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo,
    ) {
        if let Some(ref sm) = self.smart_money {
            self.feed_smart_money_classifier(&tx, sm);
        }
        if let Some(swap) = extract_pending_swap(slot, &tx, self.registry.as_ref()) {
            debug!("pending swap pool={}", swap.pool);
            let _ = self.tx.try_send(Opportunity::PendingSwap(swap));
        }
    }

    fn feed_smart_money_classifier(
        &self,
        tx: &yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo,
        sm: &SmartMoneyClassifier,
    ) {
        let Some(inner) = tx.transaction.as_ref() else { return };
        let Some(msg)   = inner.message.as_ref()  else { return };
        let Some(bytes) = msg.account_keys.first() else { return };
        let Ok(signer)  = Pubkey::try_from(bytes.as_slice()) else { return };

        let success  = tx.meta.as_ref().map(|m| m.err.is_none()).unwrap_or(false);
        let cu_used  = tx.meta.as_ref().and_then(|m| m.compute_units_consumed).unwrap_or(0) as u32;
        let programs: Vec<Pubkey> = msg.account_keys.iter()
            .filter_map(|b| Pubkey::try_from(b.as_slice()).ok())
            .collect();

        sm.observe_tx(signer, success, Pubkey::default(), cu_used, false, &programs);
    }
}

fn is_amm_owner(owner: &Pubkey) -> bool {
    owner == &*RAYDIUM_AMM_V4_PK || owner == &*ORCA_SWAP_V2_PK || owner == &*ORCA_WHIRLPOOL_PK
        || owner == &*METEORA_CPAMM_PK
}

fn is_lending_owner(owner: &Pubkey) -> bool {
    owner == &*KAMINO_LENDING_PK || owner == &*SOLEND_PROGRAM_PK
}

// unified pool decoder, dispatches by owner program.
// discriminant check is the first thing we do. cheap comparison before touching any field offsets.
fn decode_pool(pubkey: Pubkey, owner: &Pubkey, data: &[u8], slot: u64) -> Option<PoolState> {
    if owner == &*RAYDIUM_AMM_V4_PK {
        decode_raydium_pool(pubkey, data, slot)
    } else if owner == &*ORCA_WHIRLPOOL_PK {
        decode_whirlpool_pool(pubkey, data, slot)
    } else if owner == &*METEORA_CPAMM_PK {
        decode_meteora_pool(pubkey, data, slot)
    } else {
        // orca legacy (token-swap program): TODO, same story as raydium below but for the
        // spl-token-swap layout. lower priority, whirlpool has mostly eaten its volume.
        None
    }
}

// hardcoded raydium AMM v4 layout. offsets verified against on-chain IDL, not the docs.
fn decode_raydium_pool(pubkey: Pubkey, data: &[u8], slot: u64) -> Option<PoolState> {
    // discriminant check first, bails before any offset math on wrong account types.
    // this catches fee collector accounts, config accounts, etc that pass the owner filter.
    if data.len() < RAYDIUM_POOL_MIN_LEN { return None; }
    if data[..8] != RAYDIUM_POOL_DISC    { return None; }

    let coin_mint = Pubkey::try_from(&data[0xB8..0xD8]).ok()?;
    let pc_mint   = Pubkey::try_from(&data[0xD8..0xF8]).ok()?;
    let reserve_a = u64::from_le_bytes(data[0x190..0x198].try_into().ok()?);
    let reserve_b = u64::from_le_bytes(data[0x198..0x1A0].try_into().ok()?);

    // skip pools with zero reserves, nothing to trade against and they'll spew NaN into the arb graph
    if reserve_a == 0 || reserve_b == 0 { return None; }

    Some(PoolState {
        pool_id: pubkey, dex: Dex::Raydium,
        token_a_mint: coin_mint, token_b_mint: pc_mint,
        reserve_a, reserve_b, fee_bps: 25, slot,
        clmm: None,
    })
}

// whirlpool account layout, byte offsets verified against
// orca-so/whirlpools/programs/whirlpool/src/state/whirlpool.rs (anchor, borsh-packed, no padding).
// discriminant is sha256("account:Whirlpool")[..8], confirmed against the repo's own test.
const WHIRLPOOL_DISC: [u8; 8] = [0x3f, 0x95, 0xd1, 0x0c, 0xe1, 0x80, 0x63, 0x09];
const WHIRLPOOL_MIN_LEN: usize = 261; // through fee_growth_global_b, ignoring reward_infos tail

fn decode_whirlpool_pool(pubkey: Pubkey, data: &[u8], slot: u64) -> Option<PoolState> {
    if data.len() < WHIRLPOOL_MIN_LEN { return None; }
    if data[..8] != WHIRLPOOL_DISC     { return None; }

    let tick_spacing     = u16::from_le_bytes(data[41..43].try_into().ok()?);
    let fee_rate         = u16::from_le_bytes(data[45..47].try_into().ok()?);
    let liquidity        = u128::from_le_bytes(data[49..65].try_into().ok()?);
    let sqrt_price        = u128::from_le_bytes(data[65..81].try_into().ok()?);
    let tick_current      = i32::from_le_bytes(data[81..85].try_into().ok()?);
    let token_mint_a      = Pubkey::try_from(&data[101..133]).ok()?;
    let token_mint_b      = Pubkey::try_from(&data[181..213]).ok()?;

    // fee_rate is hundredths of a basis point (u16::MAX ~= 6.5%), our fee_bps field is plain bps.
    let fee_bps = (fee_rate / 100).min(u16::MAX);

    if liquidity == 0 || sqrt_price == 0 { return None; } // no liquidity, nothing to quote against

    Some(PoolState {
        pool_id: pubkey, dex: Dex::OrcaWhirlpool,
        token_a_mint: token_mint_a, token_b_mint: token_mint_b,
        reserve_a: 0, reserve_b: 0, // not meaningful for CLMM, see clmm field instead
        fee_bps, slot,
        clmm: Some(ClmmState { sqrt_price, liquidity, tick_current, tick_spacing }),
    })
}

// meteora DAMM v2 (cp-amm) Pool account layout, verified against the real
// programs/cp-amm/src/state/pool.rs source (not searched, provided directly). offsets below
// cross-checked against the source's own const_assert_eq!(Pool::INIT_SPACE, 1104): computing
// every field's offset independently landed on exactly 1104 bytes of body after the 8-byte
// anchor discriminator, matching their assertion exactly.
//
// two liquidity models share this one account, picked by collect_fee_mode:
//   - Compounding (2): CompoundingLiquidity, a real constant-product pool over token_a_amount/
//     token_b_amount. quotable with our existing constant-product math (reserve_a/reserve_b),
//     no CLMM needed.
//   - BothToken (0) / OnlyB (1): ConcentratedLiquidity, ONE bounded price range per pool
//     [sqrt_min_price, sqrt_max_price] with a single liquidity value across the whole range
//     (unlike whirlpool, there's no per-tick liquidity change to miss). our existing quote_clmm
//     formula is exact here, not an approximation, we just don't enforce the pool's own
//     min/max price bounds, a trade that would push past them just reverts on-chain (protected
//     by min_amount_out) rather than silently mis-quoting.
const METEORA_POOL_DISC: [u8; 8] = [0xf1, 0x9a, 0x6d, 0x04, 0x11, 0xb1, 0x6d, 0xbc]; // sha256("account:Pool")[..8]
const METEORA_POOL_LEN:  usize = 1112; // 8-byte disc + 1104-byte body, matches Pool::INIT_SPACE exactly

fn decode_meteora_pool(pubkey: Pubkey, data: &[u8], slot: u64) -> Option<PoolState> {
    if data.len() < METEORA_POOL_LEN { return None; }
    if data[..8] != METEORA_POOL_DISC { return None; }

    let token_a_mint  = Pubkey::try_from(&data[168..200]).ok()?;
    let token_b_mint  = Pubkey::try_from(&data[200..232]).ok()?;
    let liquidity     = u128::from_le_bytes(data[360..376].try_into().ok()?);
    let sqrt_min_price = u128::from_le_bytes(data[424..440].try_into().ok()?);
    let sqrt_max_price = u128::from_le_bytes(data[440..456].try_into().ok()?);
    let sqrt_price     = u128::from_le_bytes(data[456..472].try_into().ok()?);
    let pool_status    = data[481];
    let collect_fee_mode = data[484];
    let token_a_amount = u64::from_le_bytes(data[680..688].try_into().ok()?);
    let token_b_amount = u64::from_le_bytes(data[688..696].try_into().ok()?);

    if pool_status != 0 { return None; } // 0 = Enable, 1 = Disable, don't quote a disabled pool
    let _ = (sqrt_min_price, sqrt_max_price); // not enforced yet, see comment above

    // fee isn't a single flat value here (base fee scheduler + dynamic fee on top), and reading
    // it right means decoding the nested PoolFeesStruct/BaseFeeStruct properly. rough constant
    // for now rather than parsing that whole nested structure just for an estimate.
    let fee_bps: u16 = 30;

    if collect_fee_mode == 2 {
        // Compounding: real constant-product reserves, quotable directly.
        if token_a_amount == 0 || token_b_amount == 0 { return None; }
        Some(PoolState {
            pool_id: pubkey, dex: Dex::Meteora,
            token_a_mint, token_b_mint,
            reserve_a: token_a_amount, reserve_b: token_b_amount,
            fee_bps, slot, clmm: None,
        })
    } else {
        // BothToken / OnlyB: single-range concentrated liquidity.
        if liquidity == 0 || sqrt_price == 0 { return None; }
        Some(PoolState {
            pool_id: pubkey, dex: Dex::Meteora,
            token_a_mint, token_b_mint,
            reserve_a: 0, reserve_b: 0, // not meaningful here, see clmm field
            fee_bps, slot,
            clmm: Some(ClmmState { sqrt_price, liquidity, tick_current: 0, tick_spacing: 1 }),
        })
    }
}

// minimal obligation decode. collateral/borrow at fixed offsets, works for kamino v1 and solend.
// marginfi has a different layout; add it when we actually need it.
fn decode_obligation(pubkey: Pubkey, program: &Pubkey, data: &[u8], slot: u64) -> Option<ObligationState> {
    if program == &*KAMINO_LENDING_PK {
        return decode_kamino_obligation(pubkey, data, slot);
    }
    decode_solend_obligation(pubkey, data, slot)
}

// solend obligation layout, verified byte-for-byte against Pack::pack_into_slice /
// unpack_from_slice in solana-labs/solana-program-library, token-lending/program/src/state/obligation.rs.
// this is a pre-anchor program (Pack trait, not anchor), no 8-byte discriminant, version is
// a plain u8 at offset 0. deposits/borrows are variable-length (deposits_len/borrows_len
// bytes just before the data), not fixed arrays like kamino's, so we walk them instead of
// indexing straight in.
const SOLEND_OBLIGATION_LEN: usize = 916; // OBLIGATION_LEN in the source, fixed regardless of actual deposits/borrows count
const SOLEND_COLLATERAL_LEN: usize = 56;  // 32 (pubkey) + 8 (u64) + 16 (Decimal)
const SOLEND_LIQUIDITY_LEN:  usize = 80;  // 32 (pubkey) + 16 + 16 + 16 (3 Decimals)

fn decode_solend_obligation(pubkey: Pubkey, data: &[u8], slot: u64) -> Option<ObligationState> {
    if data.len() < SOLEND_OBLIGATION_LEN { return None; }
    let version = data[0];
    if version == 0 { return None; } // UNINITIALIZED_VERSION

    let owner              = Pubkey::try_from(&data[42..74]).ok()?;
    // Decimal fields are WAD-scaled u128 (the field is literally named e.g. "borrowed_amount_wads"
    // in the source). we don't need to know the exact WAD constant: every ratio below divides
    // one Decimal by another of the same scale, so the scale cancels out.
    let deposited_value        = u128::from_le_bytes(data[74..90].try_into().ok()?);
    let borrowed_value         = u128::from_le_bytes(data[90..106].try_into().ok()?);
    let unhealthy_borrow_value = u128::from_le_bytes(data[122..138].try_into().ok()?);
    let deposits_len = data[138] as usize;
    let borrows_len   = data[139] as usize;

    // real per-obligation threshold instead of a flat guess: unhealthy_borrow_value is defined
    // in the source as "the dangerous borrow value at the weighted average liquidation
    // threshold", i.e. unhealthy_borrow_value = deposited_value * liquidation_threshold.
    let liquidation_threshold_bps = if deposited_value == 0 {
        8500 // no deposits yet, arbitrary default, is_underwater() will be false regardless (0/0 handled in ltv_bps)
    } else {
        (unhealthy_borrow_value * 10_000 / deposited_value).min(u16::MAX as u128) as u16
    };

    // walk the variable-length deposits, then borrows, right after the fixed header.
    let mut offset = 140;
    let mut top_deposit_reserve = Pubkey::default();
    let mut top_deposit_amount  = 0u64;
    for _ in 0..deposits_len {
        if offset + SOLEND_COLLATERAL_LEN > data.len() { return None; }
        let reserve = Pubkey::try_from(&data[offset..offset + 32]).ok()?;
        let amount  = u64::from_le_bytes(data[offset + 32..offset + 40].try_into().ok()?);
        if amount > top_deposit_amount { top_deposit_amount = amount; top_deposit_reserve = reserve; }
        offset += SOLEND_COLLATERAL_LEN;
    }
    let mut top_borrow_reserve = Pubkey::default();
    let mut top_borrow_amount  = 0u128;
    for _ in 0..borrows_len {
        if offset + SOLEND_LIQUIDITY_LEN > data.len() { return None; }
        let reserve = Pubkey::try_from(&data[offset..offset + 32]).ok()?;
        let borrowed_amount_wads = u128::from_le_bytes(data[offset + 48..offset + 64].try_into().ok()?);
        if borrowed_amount_wads > top_borrow_amount { top_borrow_amount = borrowed_amount_wads; top_borrow_reserve = reserve; }
        offset += SOLEND_LIQUIDITY_LEN;
    }

    if top_deposit_reserve == Pubkey::default() || top_borrow_reserve == Pubkey::default() {
        return None; // nothing to liquidate against
    }

    Some(ObligationState {
        obligation_pubkey: pubkey, owner, protocol: LendingProtocol::Solend,
        collateral_value: deposited_value, borrow_value: borrowed_value,
        liquidation_threshold_bps,
        top_deposit_reserve, top_borrow_reserve,
        slot,
    })
}

// zero-copy parse via klend-interface, verified against docs.rs/klend-interface (Obligation
// struct, .deposited_value()/.borrow_factor_adjusted_debt_value()/.is_liquidatable() methods).
// no more hand-picked byte offsets for kamino, this replaces the old overlapping-offset bug
// (item #9) entirely.
fn decode_kamino_obligation(pubkey: Pubkey, data: &[u8], slot: u64) -> Option<ObligationState> {
    let obl = klend_interface::from_account_data::<klend_interface::state::Obligation>(data).ok()?;

    // largest deposit/borrow by value, see the comment on ObligationState for why we
    // simplify to a single reserve per side instead of optimizing across all of them.
    let top_deposit_reserve = obl.deposits.iter()
        .filter(|d| d.deposit_reserve != Pubkey::default())
        .max_by_key(|d| d.deposited_amount)
        .map(|d| d.deposit_reserve)
        .unwrap_or_default();
    let top_borrow_reserve = obl.borrows.iter()
        .filter(|b| b.borrow_reserve != Pubkey::default())
        .max_by_key(|b| b.borrowed_amount())
        .map(|b| b.borrow_reserve)
        .unwrap_or_default();

    if top_deposit_reserve == Pubkey::default() || top_borrow_reserve == Pubkey::default() {
        return None; // nothing to liquidate against
    }

    // _sf fields are Q68.60 fixed point, klend_interface::Fraction handles the conversion.
    // scale to lamports-equivalent (u128) the same way the rest of state.rs expects.
    let collateral_value: f64 = klend_interface::Fraction::from_bits(obl.deposited_value()).to_num();
    let borrow_value: f64     = klend_interface::Fraction::from_bits(obl.borrow_factor_adjusted_debt_value()).to_num();

    Some(ObligationState {
        obligation_pubkey: pubkey, owner: obl.owner, protocol: LendingProtocol::Kamino,
        collateral_value: (collateral_value * 1e9) as u128,
        borrow_value: (borrow_value * 1e9) as u128,
        liquidation_threshold_bps: 8500, // TODO: read the real per-reserve threshold instead of a flat default
        top_deposit_reserve, top_borrow_reserve,
        slot,
    })
}

// derives a wallet's associated token account, same PDA logic as executor.rs::derive_ata.
// duplicated here on purpose rather than importing across modules, it's six lines and monitor.rs
// shouldn't depend on executor.rs. worth pulling into a shared util module if a third copy shows up.
fn derive_ata(wallet: &Pubkey, mint: &Pubkey) -> Option<Pubkey> {
    const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    const ATA_PROGRAM:   &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
    let token_program: Pubkey = TOKEN_PROGRAM.parse().ok()?;
    let ata_program:   Pubkey = ATA_PROGRAM.parse().ok()?;
    let seeds = [wallet.as_ref(), token_program.as_ref(), mint.as_ref()];
    Some(Pubkey::find_program_address(&seeds, &ata_program).0)
}

// scans both top-level and inner (CPI) instructions for a raydium SwapBaseInV2. inner instructions
// matter because most retail volume routes through an aggregator (jupiter) that CPIs into raydium,
// the top-level instruction is the aggregator's own, not raydium's.
//
// note on "pending": solana doesn't have a real mempool, geyser streams transactions at whatever
// commitment level you subscribed at (processed here, see registry.rs subscribe config). by the
// time we see this the tx has already landed in a block, just not confirmed yet. sandwiching here
// means fast-following in the same or next slot via jito bundle, not classic mempool frontrunning.
//
// account layout assumed below (SwapBaseInV2, tag 16): verified against raydium-amm's own
// instruction.rs doc comments, same 8-account layout used in executor.rs::raydium_ix.
//   0 token program, 1 amm, 2 authority, 3 coin vault, 4 pc vault,
//   5 user source token account, 6 user destination token account, 7 user wallet (signer)
fn extract_pending_swap(
    slot: u64,
    tx:       &yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo,
    registry: Option<&Registry>,
) -> Option<PendingSwap> {
    let registry = registry?; // no registry, no way to resolve pool -> mints, nothing to do
    let inner    = tx.transaction.as_ref()?;
    let sig      = bs58::encode(&tx.signature).into_string();
    let msg      = inner.message.as_ref()?;
    let meta     = tx.meta.as_ref();

    // failed txs didn't move the pool, nothing to sandwich
    if meta.map(|m| m.err.is_some()).unwrap_or(false) { return None; }

    // full account list needs the dynamically-loaded lookup table addresses appended, static
    // account_keys alone is incomplete for v0 transactions since yellowstone-grpc v3+.
    let mut keys: Vec<Pubkey> = msg.account_keys.iter()
        .filter_map(|b| Pubkey::try_from(b.as_slice()).ok())
        .collect();
    if let Some(m) = meta {
        keys.extend(m.loaded_writable_addresses.iter().filter_map(|b| Pubkey::try_from(b.as_slice()).ok()));
        keys.extend(m.loaded_readonly_addresses.iter().filter_map(|b| Pubkey::try_from(b.as_slice()).ok()));
    }

    // (program_id_index, accounts, data) tuples from both top-level and inner instructions
    let top_level = msg.instructions.iter().map(|ix| (ix.program_id_index, &ix.accounts, &ix.data));
    let inner_ixs = meta.into_iter()
        .flat_map(|m| m.inner_instructions.iter())
        .flat_map(|group| group.instructions.iter())
        .map(|ix| (ix.program_id_index, &ix.accounts, &ix.data));

    for (program_id_index, accounts, data) in top_level.chain(inner_ixs) {
        let Some(&program_id) = keys.get(program_id_index as usize) else { continue };
        if program_id != *RAYDIUM_AMM_V4_PK { continue }
        if data.len() < 17 || data[0] != 16 { continue } // SwapBaseInV2 discriminant
        if accounts.len() < 8 { continue }

        let acc = |i: usize| accounts.get(i).and_then(|&idx| keys.get(idx as usize)).copied();
        let (Some(pool), Some(user)) = (acc(1), acc(7)) else { continue };
        let Some(user_source) = acc(5) else { continue };

        let Some(meta) = registry.pool_meta(&pool) else { continue }; // unregistered pool, can't resolve mints
        let (Some(mint_a), Some(mint_b)) = (meta.token_a_mint_pk(), meta.token_b_mint_pk()) else { continue };

        // direction: whichever candidate mint's ATA matches the user's actual source account.
        // works without an extra RPC round-trip, assumes the swapper used their standard ATA
        // (true for the overwhelming majority of swaps, aggregators included).
        let (input_mint, output_mint) = if derive_ata(&user, &mint_a) == Some(user_source) {
            (mint_a, mint_b)
        } else if derive_ata(&user, &mint_b) == Some(user_source) {
            (mint_b, mint_a)
        } else {
            continue; // non-standard source account, can't determine direction cheaply, skip
        };

        let amount_in      = u64::from_le_bytes(data[1..9].try_into().ok()?);
        let min_amount_out = u64::from_le_bytes(data[9..17].try_into().ok()?);

        return Some(PendingSwap {
            signature: sig, user, pool, dex: Dex::Raydium,
            input_mint, output_mint, amount_in, min_amount_out, slot,
        });
    }

    None
}
