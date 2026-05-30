// jito bundle submission + parallel RPC spam for belt-and-suspenders inclusion.
//
// execution model for slot-bound opportunities:
//   - NO retry loops with sleep. if the bundle is stale, it's dead. move on.
//   - fire jito bundle AND raw sendTransaction to N backup RPCs in parallel.
//   - first confirmation wins. the others are just noise at that point.
//
// jito block engine SLA: 400ms timeout, max 5 txs per bundle, tip ix must be last.
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
// do NOT use your own address — you won't get tip routing and the validator won't prioritize you
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
    TIP_ACCOUNTS[rand::thread_rng().gen_range(0..TIP_ACCOUNTS.len())].parse().unwrap()
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
    // appends the tip transfer as the last tx. jito requires this ordering.
    pub fn attach_tip(mut self, payer: &Keypair, blockhash: Hash) -> Self {
        let tip_ix = system_instruction::transfer(
            &payer.pubkey(), &random_tip_account(), self.tip_lamports,
        );
        let msg = v0::Message::try_compile(&payer.pubkey(), &[tip_ix], &[], blockhash)
            .expect("tip message compile");
        let tip_tx = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])
            .expect("tip tx sign");
        self.transactions.push(tip_tx);
        self
    }

    pub fn encode(&self) -> Vec<String> {
        self.transactions.iter()
            .map(|tx| B64.encode(bincode::serialize(tx).expect("tx serialize")))
            .collect()
    }

    // encode just the primary tx (not the tip) for raw RPC spam.
    // the tip ix is jito-only — no point sending it to vanilla validators.
    pub fn encode_primary(&self) -> Option<String> {
        self.transactions.first()
            .map(|tx| B64.encode(bincode::serialize(tx).expect("tx serialize")))
    }
}

pub struct JitoClient {
    http:         Client,
    endpoint:     String,
    spam_clients: Vec<(Client, String)>, // backup RPC endpoints for parallel spam
}

impl JitoClient {
    pub fn new(endpoint: &str, _max_retries: u32) -> Self {
        // _max_retries kept in signature for API compat but ignored.
        // slot-bound opportunities don't retry — stale = dead.
        let http = Client::builder()
            .timeout(Duration::from_millis(400))
            .build()
            .unwrap();
        Self { http, endpoint: endpoint.to_string(), spam_clients: Vec::new() }
    }

    // register additional RPC endpoints for parallel spam.
    // call this at startup with your fastest 2-3 non-jito RPCs.
    pub fn with_spam_endpoints(mut self, endpoints: Vec<String>) -> Self {
        self.spam_clients = endpoints.into_iter().map(|url| {
            let c = Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .unwrap();
            (c, url)
        }).collect();
        self
    }

    // fire jito bundle + raw RPC spam simultaneously. no retries — if it misses, it misses.
    // returns the jito bundle UUID if the bundle was accepted (spam is fire-and-forget).
    pub async fn send_bundle(&self, bundle: &JitoBundle) -> Result<String> {
        let payload = RpcRequest {
            jsonrpc: "2.0", id: 1, method: "sendBundle",
            params: vec![serde_json::json!(bundle.encode())],
        };

        debug!("firing bundle: {} txs + {} spam endpoints",
               bundle.transactions.len(), self.spam_clients.len());

        // kick off RPC spam in the background — don't await, don't care about errors
        if let Some(encoded) = bundle.encode_primary() {
            for (client, url) in &self.spam_clients {
                let c    = client.clone();
                let u    = url.clone();
                let body = serde_json::json!({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "sendTransaction",
                    "params": [encoded, {"encoding": "base64", "skipPreflight": true}]
                });
                tokio::spawn(async move {
                    if let Err(e) = c.post(&u).json(&body).send().await {
                        debug!("rpc spam {u} failed: {e}"); // non-fatal, expected sometimes
                    }
                });
            }
        }

        // jito submission — single attempt, no sleep, no retry loop
        match self.try_send(&payload).await {
            Ok(uuid) => {
                info!("bundle accepted uuid={uuid}");
                Ok(uuid)
            }
            Err(e) => {
                // log and propagate. caller decides whether to care.
                warn!("bundle rejected: {e}");
                Err(e)
            }
        }
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

    // mostly useful for debugging landed bundles. don't poll in a hot loop.
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

