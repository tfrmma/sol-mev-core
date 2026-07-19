// all config lives here. clap handles env vars and CLI flags simultaneously.
// copy .env.example to .env before running. if you forget you'll get the
// public mainnet RPC and wonder why everything is slow.
use anyhow::Result;
use clap::Parser;
use solana_sdk::{signature::Keypair, signer::EncodableKey};

#[derive(Parser, Debug, Clone)]
#[command(name = "solana-mev-bot")]
pub struct BotConfig {
    #[arg(long, env = "RPC_URL", default_value = "https://api.mainnet-beta.solana.com")]
    pub rpc_url: String,

    #[arg(long, env = "WS_URL", default_value = "wss://api.mainnet-beta.solana.com")]
    pub ws_url: String,

    #[arg(long, env = "GEYSER_URL", default_value = "http://localhost:10000")]
    pub geyser_url: String,

    #[arg(long, env = "GEYSER_TOKEN", default_value = "")]
    pub geyser_token: String,

    #[arg(long, env = "JITO_URL", default_value = "https://mainnet.block-engine.jito.labs.io/api/v1/bundles")]
    pub jito_url: String,

    #[arg(long, env = "JITO_TIP_LAMPORTS", default_value_t = 500_000)]
    pub jito_tip_lamports: u64,

    #[arg(long, env = "KEYPAIR_PATH", default_value = "~/.config/solana/id.json")]
    pub keypair_path: String,

    #[arg(long, env = "ENABLE_ARBITRAGE", default_value_t = true)]
    pub enable_arbitrage: bool,

    #[arg(long, env = "ENABLE_LIQUIDATION", default_value_t = true)]
    pub enable_liquidation: bool,

    // off by default. read sandwich.rs docstring before you flip this.
    #[arg(long, env = "ENABLE_SANDWICH", default_value_t = false)]
    pub enable_sandwich: bool,

    #[arg(long, env = "MAX_CU_PRICE", default_value_t = 1_000_000)]
    pub max_cu_price_microlamports: u64,

    #[arg(long, env = "MAX_CU_LIMIT", default_value_t = 400_000)]
    pub max_cu_limit: u32,

    #[arg(long, env = "MIN_PROFIT_LAMPORTS", default_value_t = 1_000_000)]
    pub min_profit_lamports: u64,

    #[arg(long, env = "MAX_TRADE_SOL", default_value_t = 1.0)]
    pub max_trade_sol: f64,

    #[arg(long, env = "MAX_RETRIES", default_value_t = 3)]
    pub max_retries: u32,

    // always true in prod. if you set this to false and blow up your wallet, that's on you.
    #[arg(long, env = "SIMULATE", default_value_t = true)]
    pub simulate_before_send: bool,

    // comma-separated list of backup RPC endpoints for parallel tx spam alongside jito bundles.
    // e.g. "https://rpc1.example.com,https://rpc2.example.com"
    // leave empty to skip spam (jito-only). more endpoints = better inclusion odds at peak load.
    #[arg(long, env = "SPAM_RPC_ENDPOINTS", default_value = "")]
    pub spam_rpc_endpoints: String,
}

impl BotConfig {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv(); // best-effort. missing .env is fine if env vars are set directly.
        Ok(Self::parse())
    }

    pub fn spam_endpoints(&self) -> Vec<String> {
        self.spam_rpc_endpoints.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }

    pub fn load_keypair(&self) -> Result<Keypair> {
        Keypair::read_from_file(&self.keypair_path)
            .map_err(|e| anyhow::anyhow!("keypair load failed {}: {}", self.keypair_path, e))
    }
}
