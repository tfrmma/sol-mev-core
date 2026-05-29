// geyser subscriber. account updates → pool/obligation state.
// tx updates → smart money classification + pending swap extraction.
// reconnects forever on error because the stream will drop periodically and that's normal.
use anyhow::Result;
use futures::StreamExt;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterTransactions, SubscribeUpdateAccountInfo,
};

use crate::{
    registry::Registry,
    risk::RiskEngine,
    simulator::AccountCache,
    smart_money::SmartMoneyClassifier,
    state::{Dex, LendingProtocol, ObligationState, PoolState, CURRENT_SLOT, OBLIGATIONS, POOLS},
};

// program IDs — keep these in sync with registry.rs defaults
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_SWAP_V2:   &str = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const KAMINO_LENDING: &str = "KLend2g3cP87fffoy8q1mQqGKjrL1AyGGFsDGJr5J6Z";
const SOLEND_PROGRAM: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";

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
                // fallback to hardcoded defaults if no registry. ugly but fine for dev.
                RAYDIUM_AMM_V4.to_string(), ORCA_SWAP_V2.to_string(),
                ORCA_WHIRLPOOL.to_string(), KAMINO_LENDING.to_string(),
                SOLEND_PROGRAM.to_string(),
            ]);

        let mut accounts_filter = HashMap::new();
        accounts_filter.insert("amm_pools".to_string(), SubscribeRequestFilterAccounts {
            account: vec![], owner: program_ids.clone(), filters: vec![],
            ..Default::default()
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
            accounts:   accounts_filter,
            transactions: tx_filter,
            commitment: Some(CommitmentLevel::Processed as i32),
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

        if let Some(ref cache) = self.account_cache {
            cache.upsert(pubkey, acc.lamports, acc.data.clone(), owner, false);
        }

        if is_amm_owner(&owner) {
            if let Some(pool) = decode_raydium_pool(pubkey, &acc.data, slot) {
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

// hardcoded raydium AMM v4 layout offsets. if raydium deploys a v5 we'll need to revisit.
// offsets verified against the on-chain IDL, not the docs (docs are sometimes wrong).
fn decode_raydium_pool(pubkey: Pubkey, data: &[u8], slot: u64) -> Option<PoolState> {
    if data.len() < 0x1A0 { return None; }
    let coin_mint = Pubkey::try_from(&data[0xB8..0xD8]).ok()?;
    let pc_mint   = Pubkey::try_from(&data[0xD8..0xF8]).ok()?;
    let reserve_a = u64::from_le_bytes(data[0x190..0x198].try_into().ok()?);
    let reserve_b = u64::from_le_bytes(data[0x198..0x1A0].try_into().ok()?);
    Some(PoolState {
        pool_id: pubkey, dex: Dex::Raydium,
        token_a_mint: coin_mint, token_b_mint: pc_mint,
        reserve_a, reserve_b, fee_bps: 25, slot,
    })
}

// minimal obligation decode. collateral/borrow at fixed offsets — works for kamino v1 and solend.
// marginfi has a completely different layout; add it when we care.
fn decode_obligation(pubkey: Pubkey, program: &Pubkey, data: &[u8], slot: u64) -> Option<ObligationState> {
    if data.len() < 200 { return None; }
    let protocol = if program.to_string() == KAMINO_LENDING {
        LendingProtocol::Kamino
    } else {
        LendingProtocol::Solend
    };
    let collateral_value = u128::from_le_bytes(data[32..48].try_into().ok()?);
    let borrow_value     = u128::from_le_bytes(data[48..64].try_into().ok()?);
    let owner            = Pubkey::from(<[u8; 32]>::try_from(&data[8..40]).ok()?);
    Some(ObligationState {
        obligation_pubkey: pubkey, owner, protocol,
        collateral_value, borrow_value,
        liquidation_threshold_bps: 8500, // 85% LTV. could vary per market but this is the common case
        slot,
    })
}

// TODO: actually decode swap instructions here.
// need per-protocol discriminant matching + argument parsing.
// until this is implemented, sandwich detection can never fire. tracked in #52.
fn extract_pending_swap(
    _slot: u64,
    _tx:   &yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo,
) -> Option<PendingSwap> {
    None
}
