// program + pool registry. loaded from registry.json at startup.
// hot-reload is supported but we only do a stale check, no inotify nonsense.
// add new pools to registry.json; the bot picks them up on next registry refresh.
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tracing::{info, warn};

use crate::state::Dex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramEntry {
    pub program_id: String,
    pub label:      String,
    pub kind:       ProgramKind,
    pub version:    u8,
    pub enabled:    bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProgramKind {
    AmmRaydium, AmmOrca, AmmOrcaWhirlpool, AmmLifinity, AmmMeteora,
    LendingKamino, LendingSolend, LendingMarginFi,
}

impl ProgramKind {
    pub fn as_dex(&self) -> Option<Dex> {
        match self {
            Self::AmmRaydium       => Some(Dex::Raydium),
            Self::AmmOrca          => Some(Dex::Orca),
            Self::AmmOrcaWhirlpool => Some(Dex::OrcaWhirlpool),
            Self::AmmLifinity      => Some(Dex::Lifinity),
            Self::AmmMeteora       => Some(Dex::Meteora),
            _                      => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolMeta {
    pub pool_id:        String,
    pub program_id:     String,
    pub token_a_mint:   String,
    pub token_b_mint:   String,
    pub token_a_vault:  String,
    pub token_b_vault:  String,
    pub fee_bps:        u16,
    // program-specific remaining accounts. convention by dex:
    //   orca legacy (token-swap): [0]=pool_mint, [1]=pool_fee_account
    pub extra_accounts: Vec<String>,
    pub dex:            ProgramKind,
}

impl PoolMeta {
    pub fn pool_pubkey(&self)     -> Option<Pubkey> { self.pool_id.parse().ok() }
    pub fn program_pubkey(&self)  -> Option<Pubkey> { self.program_id.parse().ok() }
    pub fn token_a_mint_pk(&self) -> Option<Pubkey> { self.token_a_mint.parse().ok() }
    pub fn token_b_mint_pk(&self) -> Option<Pubkey> { self.token_b_mint.parse().ok() }
    pub fn vault_a_pk(&self)      -> Option<Pubkey> { self.token_a_vault.parse().ok() }
    pub fn vault_b_pk(&self)      -> Option<Pubkey> { self.token_b_vault.parse().ok() }
    pub fn extra_pubkeys(&self)   -> Vec<Pubkey>    { self.extra_accounts.iter().filter_map(|s| s.parse().ok()).collect() }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistryFile {
    programs: Vec<ProgramEntry>,
    pools:    Vec<PoolMeta>,
}

impl RegistryFile {
    fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            let raw = std::fs::read_to_string(path).context("read registry.json")?;
            return serde_json::from_str(&raw).context("parse registry.json");
        }
        // first run, write defaults and continue. operator can populate pools later.
        warn!("registry.json not found, writing defaults");
        let default = Self::default_mainnet();
        std::fs::write(path, serde_json::to_string_pretty(&default)?)?;
        Ok(default)
    }

    fn default_mainnet() -> Self {
        Self {
            pools: vec![],
            programs: vec![
                prog("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", "Raydium AMM v4",  ProgramKind::AmmRaydium,       4),
                prog("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", "Orca Swap v2",    ProgramKind::AmmOrca,          2),
                prog("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",  "Orca Whirlpool",  ProgramKind::AmmOrcaWhirlpool, 1),
                prog("KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD",  "Kamino Lending",  ProgramKind::LendingKamino,    1),
                prog("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo",  "Solend",          ProgramKind::LendingSolend,    1),
                prog("MFv2hWf31Z9kbCa1snEPdcgp7MkGkgy5oBR3uW1CiAX",  "MarginFi",        ProgramKind::LendingMarginFi,  1),
            ],
        }
    }
}

fn prog(id: &str, label: &str, kind: ProgramKind, version: u8) -> ProgramEntry {
    ProgramEntry { program_id: id.into(), label: label.into(), kind, version, enabled: true }
}

pub struct ProgramRegistry {
    programs:           HashMap<Pubkey, ProgramEntry>,
    pools:              HashMap<Pubkey, PoolMeta>,
    pools_by_pair:      HashMap<(Pubkey, Pubkey), Vec<Pubkey>>,
    active_program_ids: Vec<Pubkey>,
    last_reload:        Instant,
}

impl ProgramRegistry {
    fn from_file(file: RegistryFile) -> Self {
        let mut programs           = HashMap::new();
        let mut pools              = HashMap::new();
        let mut pools_by_pair: HashMap<(Pubkey, Pubkey), Vec<Pubkey>> = HashMap::new();

        for entry in file.programs.iter().filter(|e| e.enabled) {
            if let Ok(pk) = entry.program_id.parse::<Pubkey>() {
                programs.insert(pk, entry.clone());
            }
        }

        for pool in &file.pools {
            let Some(pool_pk) = pool.pool_pubkey()     else { continue };
            let Some(mint_a)  = pool.token_a_mint_pk() else { continue };
            let Some(mint_b)  = pool.token_b_mint_pk() else { continue };
            // index both directions so pair lookups don't need to care about ordering
            pools_by_pair.entry((mint_a, mint_b)).or_default().push(pool_pk);
            pools_by_pair.entry((mint_b, mint_a)).or_default().push(pool_pk);
            pools.insert(pool_pk, pool.clone());
        }

        let active_program_ids = programs.keys().cloned().collect();
        Self { programs, pools, pools_by_pair, active_program_ids, last_reload: Instant::now() }
    }

    pub fn program_for_id(&self, id: &Pubkey) -> Option<&ProgramEntry> { self.programs.get(id) }
    pub fn pool_meta(&self, id: &Pubkey) -> Option<&PoolMeta>           { self.pools.get(id) }
    pub fn pool_count(&self) -> usize                                    { self.pools.len() }
    pub fn active_program_ids(&self) -> &[Pubkey]                       { &self.active_program_ids }
    pub fn all_pool_ids(&self) -> Vec<Pubkey>                             { self.pools.keys().cloned().collect() }
    pub fn needs_reload(&self, period: Duration) -> bool                 { self.last_reload.elapsed() >= period }

    pub fn pools_for_pair(&self, a: Pubkey, b: Pubkey) -> &[Pubkey] {
        self.pools_by_pair.get(&(a, b)).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn register_pool(&mut self, meta: PoolMeta) {
        let Some(pool_pk) = meta.pool_pubkey()     else { return };
        let Some(mint_a)  = meta.token_a_mint_pk() else { return };
        let Some(mint_b)  = meta.token_b_mint_pk() else { return };
        self.pools_by_pair.entry((mint_a, mint_b)).or_default().push(pool_pk);
        self.pools_by_pair.entry((mint_b, mint_a)).or_default().push(pool_pk);
        self.pools.insert(pool_pk, meta);
        info!("registry: pool {pool_pk} registered");
    }
}

// thin Arc<RwLock<>> wrapper so we can share the registry across threads without cloning everything
#[derive(Clone)]
pub struct Registry(pub Arc<RwLock<ProgramRegistry>>);

impl Registry {
    pub fn load(path: &Path) -> Result<Self> {
        let file  = RegistryFile::load_or_create(path)?;
        let inner = ProgramRegistry::from_file(file);
        info!("registry loaded: {} programs {} pools", inner.programs.len(), inner.pool_count());
        Ok(Self(Arc::new(RwLock::new(inner))))
    }

    pub fn register_pool(&self, meta: PoolMeta)              { self.0.write().unwrap().register_pool(meta); }
    pub fn pool_meta(&self, id: &Pubkey) -> Option<PoolMeta> { self.0.read().unwrap().pool_meta(id).cloned() }
    pub fn pool_count(&self) -> usize                         { self.0.read().unwrap().pool_count() }

    pub fn pools_for_pair(&self, a: Pubkey, b: Pubkey) -> Vec<Pubkey> {
        self.0.read().unwrap().pools_for_pair(a, b).to_vec()
    }

    pub fn active_program_ids(&self) -> Vec<Pubkey> {
        self.0.read().unwrap().active_program_ids().to_vec()
    }

    pub fn all_pool_ids(&self) -> Vec<Pubkey> {
        self.0.read().unwrap().all_pool_ids()
    }

    pub fn active_program_id_strings(&self) -> Vec<String> {
        self.active_program_ids().iter().map(|p| p.to_string()).collect()
    }
}
