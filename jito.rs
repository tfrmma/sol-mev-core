// jito bundle submission. the block engine is picky — 400ms timeout, max 5 txs,
// tip ix must be last. don't mess with the tip accounts list without checking
// the official jito docs first.
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_sdk::{
    hash::Hash, pubkey::Pubkey, signature::Keypair, signer::Signer,
    system_instruction, transaction::VersionedTransaction,
    message::{v0, VersionedMessage},
};
use std::time::Duration;
use tracing::{debug, info, warn};

// verified against https://jito-labs.gitbook.io/mev/searcher-resources/tip-payment-program
// do NOT hardcode your own tip account — you won't get the tip routing
const TIP_ACCOUNTS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

fn random_tip_account() -> Pubkey {
    let idx = rand::thread_rng().gen_range(0..TIP_ACCOUNTS.len());
    TIP_ACCOUNTS[idx].parse().unwrap()
}

#[derive(Serialize)]
struct RpcRequest {
    jsonrpc: &'static str,
    id:      u64,
    method:  &'static str,
    params:  Vec<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
pub struct BundleResponse {
    pub result: Option<String>,
    pub error:  Option<serde_json::Value>,
}

pub struct JitoBundle {
    pub transactions: Vec<VersionedTransaction>,
    pub tip_lamports: u64,
}

impl JitoBundle {
    // appends the tip transfer as the last tx. jito requires this.
    pub fn attach_tip(mut self, payer: &Keypair, blockhash: Hash) -> Self {
        let tip_ix = system_instruction::transfer(
            &payer.pubkey(),
            &random_tip_account(),
            self.tip_lamports,
        );
        let msg = v0::Message::try_compile(&payer.pubkey(), &[tip_ix], &[], blockhash)
            .expect("tip message compile");
        let tip_tx = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])
            .expect("tip tx sign");
        self.transactions.push(tip_tx);
        self
    }

    pub fn encode(&self) -> Vec<String> {
        self.transactions.iter().map(|tx| {
            B64.encode(bincode::serialize(tx).expect("tx serialize"))
        }).collect()
    }
}

pub struct JitoClient {
    http:        Client,
    endpoint:    String,
    max_retries: u32,
}

impl JitoClient {
    pub fn new(endpoint: &str, max_retries: u32) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_millis(400)) // block engine SLA is 400ms. hard limit.
            .build()
            .unwrap();
        Self { http, endpoint: endpoint.to_string(), max_retries }
    }

    pub async fn send_bundle(&self, bundle: &JitoBundle) -> Result<String> {
        let payload = RpcRequest {
            jsonrpc: "2.0",
            id:      1,
            method:  "sendBundle",
            params:  vec![serde_json::json!(bundle.encode())],
        };

        debug!("sending bundle: {} txs", bundle.transactions.len());

        let mut last_err = None;
        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                // exponential backoff capped implicitly by max_retries.
                // if you're retrying 3+ times something is probably wrong upstream.
                tokio::time::sleep(Duration::from_millis(50 * 2u64.pow(attempt - 1))).await;
            }
            match self.try_send(&payload).await {
                Ok(uuid) => {
                    info!("bundle accepted uuid={uuid}");
                    return Ok(uuid);
                }
                Err(e) => {
                    warn!("attempt {attempt} failed: {e}");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("max retries exceeded")))
    }

    async fn try_send(&self, payload: &RpcRequest) -> Result<String> {
        let body: BundleResponse = self.http
            .post(&self.endpoint)
            .json(payload)
            .send()
            .await?
            .json()
            .await
            .context("deserialize jito response")?;

        body.result.ok_or_else(|| anyhow::anyhow!("jito error: {:?}", body.error))
    }

    // mostly useful for debugging. don't poll this in a hot loop.
    pub async fn get_bundle_status(&self, uuid: &str) -> Result<String> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "getBundleStatuses",
            "params": [[uuid]],
        });
        let url = self.endpoint.replace("bundles", "getBundleStatuses");
        let val: serde_json::Value = self.http.post(&url).json(&payload).send().await?.json().await?;
        Ok(val.to_string())
    }
}
