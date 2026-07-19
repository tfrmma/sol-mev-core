// geyser subscriber. account updates → pool/obligation state.
// tx updates → smart money classification + pending swap extraction.
// reconnects forever on error because the stream drops periodically and that's normal.
//
// FILTERING STRATEGY:
//   1. server-side: geyser `owner` filter drops everything not owned by our program IDs.
//      this is free — the validator does it before the packet hits the network.
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
    state::{Dex, LendingProtocol, ObligationState, PoolState, CURRENT_SLOT, OBLIGATIONS, POOLS},
};

// program IDs — keep in sync with registry.rs defaults
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_SWAP_V2:   &str = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const KAMINO_LENDING: &str = "KLend2g3cP87fffoy8q1mQqGKjrL1AyGGFsDGJr5J6Z";
const SOLEND_PROGRAM: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";

// Raydium AMM v4 account discriminant — first 8 bytes of the on-chain layout.
// verify with: solana account <pool> --output json | head. if this changes, raydium redeployed.
const RAYDIUM_POOL_DISC: [u8; 8] = [0x01, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00];
// Kamino obligation anchor discriminant (sha256("account:Obligation")[..8])
const KAMINO_OBLIGATION_DISC: [u8; 8] = [0xca, 0x5d, 0x0c, 0x6b, 0x7e, 0x3d, 0x41, 0x72];
// minimum sane data sizes — saves us from indexing into garbage buffers
const RAYDIUM_POOL_MIN_LEN: usize  = 0x1A0;
const OBLIGATION_MIN_LEN:   usize  = 200;

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
                Ok(_)  => info!("geyser stream closed — reconnecting"),
                Err(e) => warn!("geyser error: {e} — reconnecting in 500ms"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    async fn stream_loop(&self) -> Result<()> {
        let mut client = GeyserGrpcClient::connect(
            self.endpoint.clone(), Some(self.token.clone()), None,
        ).await?;

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
        // this is the biggest lever — geyser drops the rest before sending anything over gRPC.
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
        if let Some(swap) = extract_pending_swap(slot, &tx) {
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
    let s = owner.to_string();
    s == RAYDIUM_AMM_V4 || s == ORCA_SWAP_V2 || s == ORCA_WHIRLPOOL
}

fn is_lending_owner(owner: &Pubkey) -> bool {
    let s = owner.to_string();
    s == KAMINO_LENDING || s == SOLEND_PROGRAM
}

// unified pool decoder — dispatches by owner program.
// discriminant check is the first thing we do. cheap comparison before touching any field offsets.
fn decode_pool(pubkey: Pubkey, owner: &Pubkey, data: &[u8], slot: u64) -> Option<PoolState> {
    let owner_str = owner.to_string();
    if owner_str == RAYDIUM_AMM_V4 {
        decode_raydium_pool(pubkey, data, slot)
    } else {
        // orca/whirlpool: TODO — layout differs per pool version.
        // at minimum check discriminant before returning None to avoid log spam.
        None
    }
}

// hardcoded raydium AMM v4 layout. offsets verified against on-chain IDL, not the docs.
fn decode_raydium_pool(pubkey: Pubkey, data: &[u8], slot: u64) -> Option<PoolState> {
    // discriminant check first — bails before any offset math on wrong account types.
    // this catches fee collector accounts, config accounts, etc that pass the owner filter.
    if data.len() < RAYDIUM_POOL_MIN_LEN { return None; }
    if data[..8] != RAYDIUM_POOL_DISC    { return None; }

    let coin_mint = Pubkey::try_from(&data[0xB8..0xD8]).ok()?;
    let pc_mint   = Pubkey::try_from(&data[0xD8..0xF8]).ok()?;
    let reserve_a = u64::from_le_bytes(data[0x190..0x198].try_into().ok()?);
    let reserve_b = u64::from_le_bytes(data[0x198..0x1A0].try_into().ok()?);

    // skip pools with zero reserves — nothing to trade against and they'll spew NaN into the arb graph
    if reserve_a == 0 || reserve_b == 0 { return None; }

    Some(PoolState {
        pool_id: pubkey, dex: Dex::Raydium,
        token_a_mint: coin_mint, token_b_mint: pc_mint,
        reserve_a, reserve_b, fee_bps: 25, slot,
    })
}

// minimal obligation decode. collateral/borrow at fixed offsets — works for kamino v1 and solend.
// marginfi has a different layout; add it when we actually need it.
fn decode_obligation(pubkey: Pubkey, program: &Pubkey, data: &[u8], slot: u64) -> Option<ObligationState> {
    if data.len() < OBLIGATION_MIN_LEN { return None; }

    // kamino uses anchor discriminants; check before parsing fields
    let protocol = if program.to_string() == KAMINO_LENDING {
        if data[..8] != KAMINO_OBLIGATION_DISC { return None; }
        LendingProtocol::Kamino
    } else {
        LendingProtocol::Solend // solend doesn't use anchor discriminants
    };

    let collateral_value = u128::from_le_bytes(data[32..48].try_into().ok()?);
    let borrow_value     = u128::from_le_bytes(data[48..64].try_into().ok()?);
    let owner            = Pubkey::from(<[u8; 32]>::try_from(&data[8..40]).ok()?);

    Some(ObligationState {
        obligation_pubkey: pubkey, owner, protocol,
        collateral_value, borrow_value,
        liquidation_threshold_bps: 8500, // 85% LTV. most markets, not all. good enough for now.
        slot,
    })
}

// TODO: decode swap instructions from tx data (#52).
// per-protocol discriminant matching + argument parsing needed.
// without this, sandwich detection can never fire.
fn extract_pending_swap(
    _slot: u64,
    _tx:   &yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo,
) -> Option<PendingSwap> {
    None
}
