// two-tier simulation: litesvm locally first, fall back to RPC if cache miss.
// local sim is ~200µs. rpc sim is ~50ms. strongly prefer local.
//
// STATE DELTA MODEL:
//   instead of re-fetching whole accounts, geyser sends us the full account data
//   on every write (it's always a full account update, not a diff). so `apply_delta`
//   is really just a targeted upsert — we update lamports + data atomically without
//   touching unrelated fields. this keeps the sim cache fresh within the same slot
//   as the geyser update, which is what matters for pre-flight accuracy.
use anyhow::{Context, Result};
use litesvm::LiteSVM;
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
use tracing::{debug, warn};

#[derive(Debug)]
pub struct SimResult {
    pub units_consumed: u64,
    pub success:        bool,
    pub error:          Option<String>,
    pub logs:           Vec<String>,
    pub elapsed_us:     u64,
}

// flat account cache shared between local sim and the geyser delta path.
// RwLock is fine — writes are rare (only on geyser updates), reads are on every sim.
pub struct AccountCache(RwLock<HashMap<Pubkey, Account>>);

impl AccountCache {
    pub fn new() -> Arc<Self> {
        // 4096 initial cap. at 64 bytes average overhead per entry that's ~256KB — nothing.
        // avoids the first few rehashes which are the ones that hurt during warm startup.
        Arc::new(Self(RwLock::new(HashMap::with_capacity(4096))))
    }

    // full upsert — used during warm cache at startup
    pub fn upsert(&self, pubkey: Pubkey, lamports: u64, data: Vec<u8>, owner: Pubkey, executable: bool) {
        self.0.write().unwrap().insert(pubkey, Account {
            lamports, data, owner, executable, rent_epoch: u64::MAX,
        });
    }

    // targeted delta update from a geyser account write notification.
    // only touches lamports + data — owner/executable don't change on normal state writes
    // and re-parsing them from geyser each time is pointless overhead.
    pub fn apply_delta(&self, pubkey: Pubkey, lamports: u64, data: Vec<u8>, owner: Pubkey) {
        let mut map = self.0.write().unwrap();
        match map.get_mut(&pubkey) {
            Some(acc) => {
                // existing entry: patch in place. avoids re-allocating the Account struct.
                acc.lamports = lamports;
                acc.data     = data;
                // owner theoretically doesn't change but update it anyway in case of program upgrade
                acc.owner    = owner;
            }
            None => {
                // first time we see this account — insert it properly
                map.insert(pubkey, Account {
                    lamports, data, owner, executable: false, rent_epoch: u64::MAX,
                });
            }
        }
    }

    fn snapshot(&self) -> Vec<(Pubkey, Account)> {
        self.0.read().unwrap().iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    pub fn len(&self) -> usize { self.0.read().unwrap().len() }
}

pub struct LocalSimulator {
    cache: Arc<AccountCache>,
}

impl LocalSimulator {
    pub fn new(cache: Arc<AccountCache>) -> Self { Self { cache } }

    pub fn simulate(&self, payer: &Keypair, ixs: &[Instruction]) -> Result<SimResult> {
        let t0      = Instant::now();
        let mut svm = LiteSVM::new();

        for (pubkey, account) in self.cache.snapshot() {
            svm.set_account(pubkey, account).ok();
        }
        // fake balance — real lamport check happens on-chain, not here
        svm.airdrop(&payer.pubkey(), 10_000_000_000).ok();

        let elapsed_us = || t0.elapsed().as_micros() as u64;

        match svm.send_instructions(payer, ixs) {
            Ok(meta) => {
                debug!("local sim ok — {} CU in {} µs", meta.compute_units_consumed, elapsed_us());
                Ok(SimResult {
                    units_consumed: meta.compute_units_consumed,
                    success:        true,
                    error:          None,
                    logs:           meta.logs,
                    elapsed_us:     elapsed_us(),
                })
            }
            Err(e) => {
                let msg = format!("{e:?}");
                warn!("local sim fail in {} µs: {msg}", elapsed_us());
                Ok(SimResult {
                    units_consumed: 0, success: false,
                    error: Some(msg), logs: vec![],
                    elapsed_us: elapsed_us(),
                })
            }
        }
    }
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
            sig_verify:              false,
            replace_recent_blockhash: true,
            commitment:              Some(CommitmentConfig::processed()),
            encoding:                None,
            accounts:                None,
            min_context_slot:        None,
            inner_instructions:      false,
        };

        let resp       = self.rpc.simulate_transaction_with_config(&tx, cfg).await?.value;
        let elapsed_us = t0.elapsed().as_micros() as u64;

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
    pub local:          LocalSimulator,
    pub rpc:            RpcSimulator,
    pub cache:          Arc<AccountCache>,
    pub spam_endpoints: Vec<String>, // passed through to JitoClient at executor construction
}

impl Simulator {
    pub fn new(rpc_url: &str, cache: Arc<AccountCache>, spam_endpoints: Vec<String>) -> Self {
        Self {
            local: LocalSimulator::new(cache.clone()),
            rpc:   RpcSimulator::new(rpc_url),
            cache,
            spam_endpoints,
        }
    }

    // local first, rpc fallback only on account miss. rpc is ~250x slower.
    pub async fn simulate(&self, payer: &Keypair, ixs: Vec<Instruction>) -> Result<SimResult> {
        let result = self.local.simulate(payer, &ixs)?;
        if !result.success {
            let err = result.error.as_deref().unwrap_or("");
            if err.contains("AccountNotFound") || err.contains("InvalidAccountData") {
                warn!("local sim cache miss ({} accounts cached) — falling back to RPC",
                      self.cache.len());
                return self.rpc.simulate(payer, ixs).await;
            }
        }
        Ok(result)
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
    // defaults to 100k on rpc failure — better to overpay than get stuck in the queue.
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
