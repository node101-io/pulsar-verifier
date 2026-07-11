use std::{collections::HashSet, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use libp2p::PeerId;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{Error, Result};

use super::identity::peer_id_from_ed25519;

const VALIDATORS_PER_PAGE: usize = 100;
const ED25519_PUBLIC_TYPES: &[&str] = &["tendermint/PubKeyEd25519", "cometbft/PubKeyEd25519"];

/// Loads complete validator snapshots; scheduling refreshes belongs to Pulsar Listener.
#[derive(Clone)]
pub(crate) struct ValidatorSetClient {
    client: Client,
    rpc_url: Url,
    chain_id: String,
}

impl ValidatorSetClient {
    pub(crate) fn new(rpc_url: Url, chain_id: String, timeout: Duration) -> Result<Self> {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| Error::P2pAuthorization(error.to_string()))?;
        Ok(Self {
            client,
            rpc_url,
            chain_id,
        })
    }

    pub(crate) async fn load(&self) -> Result<HashSet<PeerId>> {
        let status: StatusResult = self.call("status", Value::Null).await?;
        if status.node_info.network != self.chain_id {
            return Err(Error::P2pAuthorization(format!(
                "CometBFT chain ID mismatch: expected {}, got {}",
                self.chain_id, status.node_info.network
            )));
        }
        if status.sync_info.catching_up {
            return Err(Error::P2pAuthorization(
                "CometBFT node is still catching up".to_owned(),
            ));
        }

        let height = status.sync_info.latest_block_height;
        let mut page = 1usize;
        let mut peers = HashSet::new();
        let mut received = 0usize;
        let mut expected_total = None;

        loop {
            let result: ValidatorsResult = self
                .call(
                    "validators",
                    serde_json::json!({
                        "height": height,
                        "page": page.to_string(),
                        "per_page": VALIDATORS_PER_PAGE.to_string(),
                    }),
                )
                .await?;
            let total = parse_usize("validators.total", &result.total)?;
            expected_total.get_or_insert(total);
            if expected_total != Some(total) {
                return Err(Error::P2pAuthorization(
                    "validator total changed during paginated fetch".to_owned(),
                ));
            }

            for validator in result.validators {
                peers.insert(decode_validator_peer_id(&validator.pub_key)?);
                received += 1;
            }

            if received >= total {
                break;
            }
            if result.count == "0" {
                return Err(Error::P2pAuthorization(
                    "validator pagination ended before total was reached".to_owned(),
                ));
            }
            page += 1;
        }

        if peers.len() != expected_total.unwrap_or_default() {
            return Err(Error::P2pAuthorization(format!(
                "validator set contains duplicate public keys: expected {}, got {}",
                expected_total.unwrap_or_default(),
                peers.len()
            )));
        }
        Ok(peers)
    }

    async fn call<T>(&self, method: &str, params: Value) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let request = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        let response = self
            .client
            .post(self.rpc_url.clone())
            .json(&request)
            .send()
            .await
            .map_err(|error| Error::P2pAuthorization(format!("CometBFT RPC failed: {error}")))?
            .error_for_status()
            .map_err(|error| Error::P2pAuthorization(format!("CometBFT RPC failed: {error}")))?
            .json::<RpcResponse<T>>()
            .await
            .map_err(|error| {
                Error::P2pAuthorization(format!("invalid CometBFT RPC response: {error}"))
            })?;

        if let Some(error) = response.error {
            return Err(Error::P2pAuthorization(format!(
                "CometBFT RPC {}: {}",
                error.code, error.message
            )));
        }
        response.result.ok_or_else(|| {
            Error::P2pAuthorization("CometBFT RPC response has no result".to_owned())
        })
    }
}

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: Value,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct StatusResult {
    node_info: NodeInfo,
    sync_info: SyncInfo,
}

#[derive(Deserialize)]
struct NodeInfo {
    network: String,
}

#[derive(Deserialize)]
struct SyncInfo {
    latest_block_height: String,
    catching_up: bool,
}

#[derive(Deserialize)]
struct ValidatorsResult {
    validators: Vec<Validator>,
    count: String,
    total: String,
}

#[derive(Deserialize)]
struct Validator {
    pub_key: ValidatorPublicKey,
}

#[derive(Deserialize)]
struct ValidatorPublicKey {
    #[serde(rename = "type")]
    key_type: String,
    value: String,
}

fn decode_validator_peer_id(encoded: &ValidatorPublicKey) -> Result<PeerId> {
    if !ED25519_PUBLIC_TYPES.contains(&encoded.key_type.as_str()) {
        return Err(Error::P2pAuthorization(format!(
            "unsupported validator key type: {}",
            encoded.key_type
        )));
    }
    let public = STANDARD.decode(&encoded.value).map_err(|error| {
        Error::P2pAuthorization(format!("invalid validator public key base64: {error}"))
    })?;
    peer_id_from_ed25519(&public)
}

fn parse_usize(field: &str, value: &str) -> Result<usize> {
    value
        .parse()
        .map_err(|error| Error::P2pAuthorization(format!("invalid {field}: {error}")))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use libp2p::identity;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        task::JoinHandle,
    };
    use tokio_util::sync::CancellationToken;

    use super::*;

    type Handler = Arc<dyn Fn(Value) -> Value + Send + Sync>;

    #[tokio::test]
    async fn paginates_one_pinned_validator_height() {
        let validators = (0..101)
            .map(|_| validator(identity::Keypair::generate_ed25519().public()))
            .collect::<Vec<_>>();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let handler: Handler = Arc::new(move |request| {
            recorded.lock().unwrap().push(request.clone());
            match request["method"].as_str().unwrap() {
                "status" => rpc_result(&status("pulsar-test", "42", false)),
                "validators" => {
                    let page = request["params"]["page"]
                        .as_str()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    let start = (page - 1) * VALIDATORS_PER_PAGE;
                    let page_values = validators
                        .iter()
                        .skip(start)
                        .take(VALIDATORS_PER_PAGE)
                        .cloned()
                        .collect::<Vec<_>>();
                    rpc_result(&serde_json::json!({
                        "block_height": "42",
                        "validators": page_values,
                        "count": page_values.len().to_string(),
                        "total": validators.len().to_string(),
                    }))
                }
                method => panic!("unexpected RPC method {method}"),
            }
        });
        let (url, cancellation, server) = spawn_rpc_server(handler).await;

        let client =
            ValidatorSetClient::new(url, "pulsar-test".to_owned(), Duration::from_secs(2)).unwrap();
        let peers = client.load().await.unwrap();

        assert_eq!(peers.len(), 101);
        {
            let calls = calls.lock().unwrap();
            let validator_calls = calls
                .iter()
                .filter(|request| request["method"] == "validators")
                .collect::<Vec<_>>();
            assert_eq!(validator_calls.len(), 2);
            assert!(
                validator_calls
                    .iter()
                    .all(|request| request["params"]["height"] == "42")
            );
        }

        cancellation.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_chain_id_mismatch() {
        let handler: Handler = Arc::new(|_| rpc_result(&status("wrong-chain", "9", false)));
        let (url, cancellation, server) = spawn_rpc_server(handler).await;
        let client =
            ValidatorSetClient::new(url, "expected-chain".to_owned(), Duration::from_secs(2))
                .unwrap();

        assert!(matches!(
            client.load().await,
            Err(Error::P2pAuthorization(_))
        ));

        cancellation.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_catching_up_node() {
        let handler: Handler = Arc::new(|_| rpc_result(&status("pulsar-test", "9", true)));
        let (url, cancellation, server) = spawn_rpc_server(handler).await;
        let client =
            ValidatorSetClient::new(url, "pulsar-test".to_owned(), Duration::from_secs(2)).unwrap();

        assert!(matches!(
            client.load().await,
            Err(Error::P2pAuthorization(_))
        ));

        cancellation.cancel();
        server.await.unwrap();
    }

    fn validator(public: identity::PublicKey) -> Value {
        let public = public.try_into_ed25519().unwrap();
        serde_json::json!({
            "address": "unused",
            "pub_key": {
                "type": "tendermint/PubKeyEd25519",
                "value": STANDARD.encode(public.to_bytes()),
            },
            "voting_power": "1",
            "proposer_priority": "0",
        })
    }

    fn status(chain_id: &str, height: &str, catching_up: bool) -> Value {
        serde_json::json!({
            "node_info": { "network": chain_id },
            "sync_info": {
                "latest_block_height": height,
                "catching_up": catching_up,
            }
        })
    }

    fn rpc_result(result: &Value) -> Value {
        serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": result })
    }

    async fn spawn_rpc_server(handler: Handler) -> (Url, CancellationToken, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = task_cancellation.cancelled() => return,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let handler = handler.clone();
                        tokio::spawn(async move { serve_connection(stream, handler).await });
                    }
                }
            }
        });
        (
            Url::parse(&format!("http://{address}")).unwrap(),
            cancellation,
            task,
        )
    }

    async fn serve_connection(mut stream: TcpStream, handler: Handler) {
        let mut bytes = Vec::new();
        let header_end = loop {
            let mut chunk = [0_u8; 1024];
            let count = stream.read(&mut chunk).await.unwrap();
            if count == 0 {
                return;
            }
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("content-length: ")
                    .or_else(|| line.strip_prefix("Content-Length: "))
            })
            .unwrap()
            .trim()
            .parse::<usize>()
            .unwrap();
        while bytes.len() < header_end + content_length {
            let mut chunk = vec![0_u8; header_end + content_length - bytes.len()];
            let count = stream.read(&mut chunk).await.unwrap();
            bytes.extend_from_slice(&chunk[..count]);
        }
        let request: Value =
            serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap();
        let body = handler(request).to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    }
}
