// simulation via RPC. we had a local LiteSVM path (~200µs) but it conflicted
// with solana-sdk 1.18 via the zeroize version resolution. pulled it for now.
// TODO: revisit litesvm when they stabilize on solana 1.18 or we bump the whole SDK to 2.x.
use anyhow::{Context, Result};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcSimulateTransactionConfig,
};
use solana_sdk::{
    account::Account, commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction, message::{v0, VersionedMessage},
    pubkey::Pubkey, signature::Keypair, signer::Signer,
    transaction::VersionedTransaction,
};
use std::{collections::HashMap, sync::{Arc, RwLock}, time::Instant};
use tracing::debug;

#[derive(Debug)]
// logs/elapsed_us are diagnostic, useful when a sim failure needs a closer look but not
// consumed by any current caller.
#[allow(dead_code)]
pub struct SimResult {
    pub units_consumed: u64,
    pub success:        bool,
    pub error:          Option<String>,
    pub logs:           Vec<String>,
    pub elapsed_us:     u64,
}

// flat account cache. used for warming at startup and keeping state current
// via apply_delta() on every geyser account write.
pub struct AccountCache(RwLock<HashMap<Pubkey, Account>>);

impl AccountCache {
    pub fn new() -> Arc<Self> {
        // 4096 initial cap. avoids rehash thrashing during warm startup.
        Arc::new(Self(RwLock::new(HashMap::with_capacity(4096))))
    }

    // full upsert, used during warm cache at startup
    pub fn upsert(&self, pubkey: Pubkey, lamports: u64, data: Vec<u8>, owner: Pubkey, executable: bool) {
        self.0.write().unwrap().insert(pubkey, Account {
            lamports, data, owner, executable, rent_epoch: u64::MAX,
        });
    }

    // targeted delta from a geyser account write notification.
    // patches lamports + data in place, avoids reallocating the Account struct on every update.
    pub fn apply_delta(&self, pubkey: Pubkey, lamports: u64, data: Vec<u8>, owner: Pubkey) {
        let mut map = self.0.write().unwrap();
        match map.get_mut(&pubkey) {
            Some(acc) => {
                acc.lamports = lamports;
                acc.data     = data;
                acc.owner    = owner; // update in case of program upgrade
            }
            None => {
                map.insert(pubkey, Account {
                    lamports, data, owner, executable: false, rent_epoch: u64::MAX,
                });
            }
        }
    }

    #[allow(dead_code)] // handy for a debug log line, nobody calls it yet
    pub fn len(&self) -> usize { self.0.read().unwrap().len() }
}

pub struct RpcSimulator {
    pub rpc: RpcClient,
}

impl RpcSimulator {
    pub fn new(rpc_url: &str) -> Self {
        Self { rpc: RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::processed()) }
    }

    // batch fetch at startup. call again after registry reload to pick up new pools.
    pub async fn warm_cache(&self, pubkeys: &[Pubkey], cache: &AccountCache) -> Result<usize> {
        let accounts = self.rpc.get_multiple_accounts(pubkeys).await
            .context("warm_cache rpc call")?;
        let mut loaded = 0;
        for (pk, maybe) in pubkeys.iter().zip(accounts) {
            if let Some(acc) = maybe {
                cache.upsert(*pk, acc.lamports, acc.data, acc.owner, acc.executable);
                loaded += 1;
            }
        }
        Ok(loaded)
    }

    pub async fn simulate(&self, payer: &Keypair, ixs: Vec<Instruction>) -> Result<SimResult> {
        let t0        = Instant::now();
        let blockhash = self.rpc.get_latest_blockhash().await?;
        let msg       = v0::Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)?;
        let tx        = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])?;

        let cfg = RpcSimulateTransactionConfig {
            sig_verify:               false,
            replace_recent_blockhash: true,
            commitment:               Some(CommitmentConfig::processed()),
            encoding:                 None,
            accounts:                 None,
            min_context_slot:         None,
            inner_instructions:       false,
        };

        let resp       = self.rpc.simulate_transaction_with_config(&tx, cfg).await?.value;
        let elapsed_us = t0.elapsed().as_micros() as u64;

        debug!("rpc sim {} µs success={}", elapsed_us, resp.err.is_none());

        Ok(SimResult {
            units_consumed: resp.units_consumed.unwrap_or(0),
            success:        resp.err.is_none(),
            error:          resp.err.map(|e| format!("{e:?}")),
            logs:           resp.logs.unwrap_or_default(),
            elapsed_us,
        })
    }
}

pub struct Simulator {
    pub rpc:            RpcSimulator,
    // populated at startup (see main.rs warm_cache) and kept live by the monitor, but
    // simulate() below doesn't consult it yet. real improvement pending: pass cached account
    // states into RpcSimulateTransactionConfig.accounts so simulation can run against our
    // predicted post-update state instead of whatever's live on the RPC node right this instant.
    #[allow(dead_code)]
    pub cache:          Arc<AccountCache>,
    pub spam_endpoints: Vec<String>,
}

impl Simulator {
    pub fn new(rpc_url: &str, cache: Arc<AccountCache>, spam_endpoints: Vec<String>) -> Self {
        Self {
            rpc:   RpcSimulator::new(rpc_url),
            cache,
            spam_endpoints,
        }
    }

    pub async fn simulate(&self, payer: &Keypair, ixs: Vec<Instruction>) -> Result<SimResult> {
        self.rpc.simulate(payer, ixs).await
    }

    // 10% CU headroom. tight enough to save fees, loose enough not to clip on variance.
    pub fn wrap_with_compute_budget(
        ixs:             Vec<Instruction>,
        simulated_units: u64,
        price_micro_lam: u64,
    ) -> Vec<Instruction> {
        let cu_limit = ((simulated_units as f64 * 1.10) as u32).max(50_000);
        let mut wrapped = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(price_micro_lam),
        ];
        wrapped.extend(ixs);
        wrapped
    }

    // 90th percentile of recent fees for these writable accounts.
    // defaults to 100k on rpc failure, better to overpay than miss the slot.
    pub async fn suggest_priority_fee(&self, accounts: &[Pubkey]) -> u64 {
        match self.rpc.rpc.get_recent_prioritization_fees(accounts).await {
            Ok(fees) if !fees.is_empty() => {
                let mut vals: Vec<u64> = fees.iter().map(|f| f.prioritization_fee).collect();
                vals.sort_unstable_by(|a, b| b.cmp(a));
                vals[vals.len() / 10]
            }
            _ => 100_000,
        }
    }
}
