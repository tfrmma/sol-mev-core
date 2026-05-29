// two-tier simulation: litesvm locally first, fall back to RPC if cache miss.
// local sim takes ~200µs, rpc sim takes ~50ms. prefer local whenever possible.
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

// flat account cache shared between local sim and account warming.
// RwLock is fine here — reads vastly outnumber writes.
pub struct AccountCache(RwLock<HashMap<Pubkey, Account>>);

impl AccountCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(RwLock::new(HashMap::with_capacity(2048))))
    }

    pub fn upsert(&self, pubkey: Pubkey, lamports: u64, data: Vec<u8>, owner: Pubkey, executable: bool) {
        self.0.write().unwrap().insert(pubkey, Account {
            lamports, data, owner, executable, rent_epoch: u64::MAX,
        });
    }

    fn snapshot(&self) -> Vec<(Pubkey, Account)> {
        self.0.read().unwrap().iter().map(|(k, v)| (*k, v.clone())).collect()
    }
}

pub struct LocalSimulator {
    cache: Arc<AccountCache>,
}

impl LocalSimulator {
    pub fn new(cache: Arc<AccountCache>) -> Self { Self { cache } }

    pub fn simulate(&self, payer: &Keypair, ixs: &[Instruction]) -> Result<SimResult> {
        let t0  = Instant::now();
        let mut svm = LiteSVM::new();

        for (pubkey, account) in self.cache.snapshot() {
            svm.set_account(pubkey, account).ok();
        }
        // fake balance so we never fail on lamports. real balance check happens on-chain.
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

    // warm the local cache with a batch fetch. call this at startup and after registry reload.
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
            sig_verify:             false,
            replace_recent_blockhash: true,
            commitment:             Some(CommitmentConfig::processed()),
            encoding:               None,
            accounts:               None,
            min_context_slot:       None,
            inner_instructions:     false,
        };

        let resp       = self.rpc.simulate_transaction_with_config(&tx, cfg).await?.value;
        let elapsed_us = t0.elapsed().as_micros() as u64;
        let success    = resp.err.is_none();

        Ok(SimResult {
            units_consumed: resp.units_consumed.unwrap_or(0),
            success,
            error:     resp.err.map(|e| format!("{e:?}")),
            logs:      resp.logs.unwrap_or_default(),
            elapsed_us,
        })
    }
}

pub struct Simulator {
    pub local: LocalSimulator,
    pub rpc:   RpcSimulator,
    pub cache: Arc<AccountCache>,
}

impl Simulator {
    pub fn new(rpc_url: &str, cache: Arc<AccountCache>) -> Self {
        Self {
            local: LocalSimulator::new(cache.clone()),
            rpc:   RpcSimulator::new(rpc_url),
            cache,
        }
    }

    // try local first. only fall back to rpc on cache miss — rpc is ~250x slower.
    pub async fn simulate(&self, payer: &Keypair, ixs: Vec<Instruction>) -> Result<SimResult> {
        let result = self.local.simulate(payer, &ixs)?;
        if !result.success {
            let err = result.error.as_deref().unwrap_or("");
            if err.contains("AccountNotFound") || err.contains("InvalidAccountData") {
                warn!("local sim cache miss — falling back to RPC");
                return self.rpc.simulate(payer, ixs).await;
            }
        }
        Ok(result)
    }

    // 10% headroom on CU limit. tight enough to save fees, loose enough not to clip on variance.
    pub fn wrap_with_compute_budget(
        ixs:              Vec<Instruction>,
        simulated_units:  u64,
        price_micro_lam:  u64,
    ) -> Vec<Instruction> {
        let cu_limit = ((simulated_units as f64 * 1.10) as u32).max(50_000);
        let mut wrapped = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(price_micro_lam),
        ];
        wrapped.extend(ixs);
        wrapped
    }

    // 90th percentile of recent fees for these accounts.
    // if the rpc call fails, default to 100k — better to overpay than get stuck.
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
