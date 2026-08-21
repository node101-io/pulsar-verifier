use std::{collections::HashSet, fmt, time::Duration};

use bytes::Bytes;
use futures::StreamExt as _;
use prost::Message as _;
use pulsar_verifier_proto::{
    chain_v1::{QueryProofsByHeightRequest, QueryProofsByHeightResponse},
    cosmos::base::query::v1beta1::PageRequest,
};
use tendermint_rpc::{
    Client as _, HttpClient, HttpClientUrl, Paging, Subscription, SubscriptionClient as _,
    WebSocketClient, WebSocketClientUrl, client::CompatMode, event::Event, query::EventType,
};
use tokio::{task::JoinHandle, time::timeout};

use crate::{Error, Result, config::ChainConfig};

use super::{ChainProof, descriptor::validate_query_proof};

const PROOFS_BY_HEIGHT_PATH: &str = "/pulsarchain.verification.v1.Query/ProofsByHeight";
const QUERY_PAGE_SIZE: u64 = 100;
const MAX_PROOFS_PER_BLOCK: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChainStatus {
    pub(crate) latest_height: u64,
}

/// Immediate `CheckTx` receipt returned by `CometBFT`'s synchronous broadcast.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BroadcastReceipt {
    pub(crate) transaction_hash: [u8; 32],
    pub(crate) code: u32,
    pub(crate) codespace: String,
    pub(crate) log: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedBlock {
    pub(crate) height: u64,
    pub(crate) validators_hash: [u8; 32],
    pub(crate) events: Vec<CommittedEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedEvent {
    pub(crate) kind: String,
    pub(crate) attributes: Vec<(String, String)>,
}

/// One owned `NewBlock` subscription and the transport task that drives it.
pub(crate) struct NewBlockSubscription {
    client: Option<WebSocketClient>,
    events: Option<Subscription>,
    driver: Option<JoinHandle<std::result::Result<(), tendermint_rpc::Error>>>,
    timeout: Duration,
}

impl NewBlockSubscription {
    pub(crate) async fn next(&mut self) -> Result<Option<CommittedBlock>> {
        let event = self
            .events
            .as_mut()
            .expect("subscription exists until close")
            .next()
            .await
            .transpose()
            .map_err(chain_error)?;
        event.map(committed_block).transpose()
    }

    pub(crate) async fn close(mut self) -> Result<()> {
        self.events.take();
        if let Some(client) = self.client.take() {
            client.close().map_err(chain_error)?;
        }
        if let Some(mut driver) = self.driver.take() {
            match timeout(self.timeout, &mut driver).await {
                Ok(Ok(result)) => result.map_err(chain_error),
                Ok(Err(error)) => Err(Error::Task(error)),
                Err(_) => {
                    driver.abort();
                    let _ = driver.await;
                    Err(Error::Chain(
                        "WebSocket driver did not stop before timeout".to_owned(),
                    ))
                }
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for NewBlockSubscription {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.abort();
        }
    }
}

/// Shared `CometBFT` v0.38 client used by P2P bootstrap and the Listener.
#[derive(Clone)]
pub(crate) struct PulsarClient {
    http: HttpClient,
    chain_id: String,
    rpc_url: String,
    timeout: Duration,
}

impl PulsarClient {
    pub(crate) fn new(config: &ChainConfig) -> Result<Self> {
        let url: HttpClientUrl = config
            .comet_rpc_url
            .as_str()
            .try_into()
            .map_err(chain_error)?;
        let http = HttpClient::builder(url)
            .compat_mode(CompatMode::V0_38)
            .timeout(config.request_timeout)
            .build()
            .map_err(chain_error)?;
        Ok(Self {
            http,
            chain_id: config.chain_id.clone(),
            rpc_url: config.comet_rpc_url.clone(),
            timeout: config.request_timeout,
        })
    }

    pub(crate) async fn status(&self) -> Result<ChainStatus> {
        let status = self.with_timeout("status", self.http.status()).await?;
        let actual_chain_id = status.node_info.network.to_string();
        if actual_chain_id != self.chain_id {
            return Err(Error::ChainIdMismatch {
                expected: self.chain_id.clone(),
                actual: actual_chain_id,
            });
        }
        if status.sync_info.catching_up {
            return Err(Error::Chain(
                "CometBFT node is still catching up".to_owned(),
            ));
        }
        Ok(ChainStatus {
            latest_height: status.sync_info.latest_block_height.value(),
        })
    }

    /// Relays an already signed Cosmos transaction and returns its `CheckTx` result.
    pub(crate) async fn broadcast_tx_sync(&self, tx_raw: Bytes) -> Result<BroadcastReceipt> {
        let response = self
            .with_timeout(
                "broadcast_tx_sync",
                self.http.broadcast_tx_sync(tx_raw.to_vec()),
            )
            .await?;
        let transaction_hash = response
            .hash
            .as_bytes()
            .try_into()
            .map_err(|_| Error::Chain("transaction hash must contain 32 bytes".to_owned()))?;
        Ok(BroadcastReceipt {
            transaction_hash,
            code: response.code.value(),
            codespace: bounded_text(response.codespace, 64),
            log: bounded_text(response.log, 512),
        })
    }

    /// Loads one complete validator snapshot at an exact committed height.
    pub(crate) async fn validator_public_keys(&self, height: u64) -> Result<Vec<[u8; 32]>> {
        let height = tendermint::block::Height::try_from(height)
            .map_err(|error| Error::Chain(format!("invalid validator height: {error}")))?;
        let response = self
            .with_timeout("validators", self.http.validators(height, Paging::All))
            .await?;
        if response.block_height != height {
            return Err(Error::Chain(format!(
                "validator response height {} does not match requested height {}",
                response.block_height.value(),
                height.value()
            )));
        }
        let total = usize::try_from(response.total).map_err(|_| {
            Error::Chain(format!(
                "validator response has invalid total {}",
                response.total
            ))
        })?;
        if total != response.validators.len() {
            return Err(Error::Chain(format!(
                "validator response count {} does not match total {}",
                response.validators.len(),
                response.total
            )));
        }

        let mut unique = HashSet::with_capacity(response.validators.len());
        for validator in response.validators {
            let bytes = validator.pub_key.to_bytes();
            let key: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
                Error::Chain(format!(
                    "validator public key must be Ed25519 (32 bytes), got {} bytes",
                    bytes.len()
                ))
            })?;
            if !unique.insert(key) {
                return Err(Error::Chain(
                    "validator set contains a duplicate public key".to_owned(),
                ));
            }
        }
        Ok(unique.into_iter().collect())
    }

    pub(crate) async fn validators_hash(&self, height: u64) -> Result<[u8; 32]> {
        let height = tendermint::block::Height::try_from(height)
            .map_err(|error| Error::Chain(format!("invalid block height: {error}")))?;
        let response = self.with_timeout("block", self.http.block(height)).await?;
        if response.block.header.height != height {
            return Err(Error::Chain(format!(
                "block response height {} does not match requested height {}",
                response.block.header.height.value(),
                height.value()
            )));
        }
        response
            .block
            .header
            .validators_hash
            .as_bytes()
            .try_into()
            .map_err(|_| Error::Chain("validators_hash must contain 32 bytes".to_owned()))
    }

    /// Recovers pending verification requests submitted at one block height.
    pub(crate) async fn proofs_by_height(&self, height: u64) -> Result<Vec<ChainProof>> {
        if height == 0 {
            return Err(Error::Chain(
                "proof query height must be positive".to_owned(),
            ));
        }
        let mut next_key = Vec::new();
        let mut proofs = Vec::new();
        let mut positions = HashSet::new();
        let mut pagination_keys = HashSet::new();
        let mut last_index = None;
        let mut query_height = None;

        loop {
            if !pagination_keys.insert(next_key.clone()) {
                return Err(Error::InvalidChainContract(
                    "ProofsByHeight repeated a pagination key".to_owned(),
                ));
            }
            let request = QueryProofsByHeightRequest {
                submission_height: height,
                pagination: Some(PageRequest {
                    key: next_key,
                    offset: 0,
                    limit: QUERY_PAGE_SIZE,
                    count_total: false,
                    reverse: false,
                }),
            };
            let response = self
                .with_timeout(
                    "ProofsByHeight",
                    self.http.abci_query(
                        Some(PROOFS_BY_HEIGHT_PATH.to_owned()),
                        request.encode_to_vec(),
                        query_height,
                        false,
                    ),
                )
                .await?;
            if let Some(expected) = query_height {
                if response.height != expected {
                    return Err(Error::InvalidChainContract(format!(
                        "ProofsByHeight response height {} does not match pinned height {}",
                        response.height.value(),
                        expected.value()
                    )));
                }
            } else {
                query_height = Some(response.height);
            }
            if !response.code.is_ok() {
                return Err(Error::Chain(format!(
                    "ProofsByHeight ABCI query failed with code {}: {}",
                    response.code.value(),
                    response.log
                )));
            }
            let page = QueryProofsByHeightResponse::decode(response.value.as_slice()).map_err(
                |error| {
                    Error::InvalidChainContract(format!("invalid ProofsByHeight response: {error}"))
                },
            )?;
            let page_was_empty = page.proofs.is_empty();
            for proof in page.proofs {
                let key = proof.proof_key.as_ref().ok_or_else(|| {
                    Error::InvalidChainContract(
                        "proof query response is missing proof_key".to_owned(),
                    )
                })?;
                if !positions.insert((key.submission_height, key.index_in_block)) {
                    return Err(Error::InvalidChainContract(format!(
                        "duplicate proof index {} at height {}",
                        key.index_in_block, key.submission_height
                    )));
                }
                if last_index.is_some_and(|index| key.index_in_block <= index) {
                    return Err(Error::InvalidChainContract(
                        "ProofsByHeight records are not strictly ordered by proof key".to_owned(),
                    ));
                }
                last_index = Some(key.index_in_block);
                if let Some(proof) = validate_query_proof(proof, height)? {
                    proofs.push(proof);
                }
            }
            if positions.len() > MAX_PROOFS_PER_BLOCK {
                return Err(Error::InvalidChainContract(format!(
                    "ProofsByHeight returned more than {MAX_PROOFS_PER_BLOCK} records"
                )));
            }

            next_key = page.pagination.map_or_else(Vec::new, |page| page.next_key);
            if next_key.is_empty() {
                break;
            }
            if page_was_empty {
                return Err(Error::InvalidChainContract(
                    "ProofsByHeight returned an empty page with a continuation key".to_owned(),
                ));
            }
        }

        Ok(proofs)
    }

    pub(crate) async fn subscribe_new_blocks(&self) -> Result<NewBlockSubscription> {
        let websocket_url = websocket_url(&self.rpc_url)?;
        let url: WebSocketClientUrl = websocket_url.as_str().try_into().map_err(chain_error)?;
        let (client, driver) = Box::pin(
            self.with_timeout(
                "WebSocket connect",
                WebSocketClient::builder(url)
                    .compat_mode(CompatMode::V0_38)
                    .build(),
            ),
        )
        .await?;
        let driver = tokio::spawn(driver.run());
        let events = match self
            .with_timeout(
                "NewBlock subscribe",
                client.subscribe(EventType::NewBlock.into()),
            )
            .await
        {
            Ok(events) => events,
            Err(error) => {
                let _ = client.close();
                driver.abort();
                let _ = driver.await;
                return Err(error);
            }
        };
        Ok(NewBlockSubscription {
            client: Some(client),
            events: Some(events),
            driver: Some(driver),
            timeout: self.timeout,
        })
    }

    async fn with_timeout<T, F>(&self, operation: &str, future: F) -> Result<T>
    where
        F: Future<Output = std::result::Result<T, tendermint_rpc::Error>>,
    {
        timeout(self.timeout, future)
            .await
            .map_err(|_| Error::Chain(format!("CometBFT {operation} timed out")))?
            .map_err(chain_error)
    }
}

fn committed_block(event: Event) -> Result<CommittedBlock> {
    let tendermint_rpc::event::EventData::NewBlock {
        block,
        result_finalize_block,
        ..
    } = event.data
    else {
        return Err(Error::InvalidChainContract(
            "NewBlock subscription returned a different event type".to_owned(),
        ));
    };
    let block = block.ok_or_else(|| {
        Error::InvalidChainContract("NewBlock event is missing block data".to_owned())
    })?;
    let finalize = result_finalize_block.ok_or_else(|| {
        Error::InvalidChainContract("NewBlock event is missing FinalizeBlock data".to_owned())
    })?;
    let validators_hash = block
        .header
        .validators_hash
        .as_bytes()
        .try_into()
        .map_err(|_| {
            Error::InvalidChainContract("block validators_hash must contain 32 bytes".to_owned())
        })?;
    let mut events = Vec::new();
    for tx in finalize.tx_results {
        if !tx.code.is_ok() {
            continue;
        }
        for event in tx.events {
            let attributes = event
                .attributes
                .iter()
                .map(|attribute| {
                    Ok((
                        attribute
                            .key_str()
                            .map_err(|error| {
                                Error::InvalidChainContract(format!(
                                    "event attribute key is not UTF-8: {error}"
                                ))
                            })?
                            .to_owned(),
                        attribute
                            .value_str()
                            .map_err(|error| {
                                Error::InvalidChainContract(format!(
                                    "event attribute value is not UTF-8: {error}"
                                ))
                            })?
                            .to_owned(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            events.push(CommittedEvent {
                kind: event.kind,
                attributes,
            });
        }
    }
    Ok(CommittedBlock {
        height: block.header.height.value(),
        validators_hash,
        events,
    })
}

fn websocket_url(http_url: &str) -> Result<String> {
    let (scheme, rest) = http_url.split_once("://").ok_or_else(|| {
        Error::InvalidConfig("chain.comet_rpc_url must include an HTTP scheme".to_owned())
    })?;
    let websocket_scheme = match scheme {
        "http" => "ws",
        "https" => "wss",
        _ => {
            return Err(Error::InvalidConfig(
                "chain.comet_rpc_url must use http or https".to_owned(),
            ));
        }
    };
    let base = rest.trim_end_matches('/');
    Ok(format!("{websocket_scheme}://{base}/websocket"))
}

fn chain_error(error: impl fmt::Display) -> Error {
    Error::Chain(error.to_string())
}

fn bounded_text(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use libp2p::identity;
    use pulsar_verifier_proto::{
        chain_v1::{
            ProofKey, ProofRecord, QueryProofResponse, QueryProofsByHeightResponse,
            query_proof_response,
        },
        cosmos::base::query::v1beta1::PageResponse,
    };
    use serde_json::Value;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        task::JoinHandle,
    };
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::proof::{ProofType, VerificationId};

    type Handler = Arc<dyn Fn(Value) -> Value + Send + Sync>;

    #[test]
    fn derives_websocket_url() {
        assert_eq!(
            websocket_url("http://127.0.0.1:26657").unwrap(),
            "ws://127.0.0.1:26657/websocket"
        );
        assert_eq!(
            websocket_url("https://rpc.example.test/").unwrap(),
            "wss://rpc.example.test/websocket"
        );
        assert!(websocket_url("ftp://rpc.example.test").is_err());
    }

    #[tokio::test]
    async fn validates_status_and_paginates_exact_validator_height() {
        let validators = (0..101)
            .map(|_| validator(identity::Keypair::generate_ed25519().public()))
            .collect::<Vec<_>>();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&calls);
        let handler: Handler = Arc::new(move |request| {
            recorded.lock().unwrap().push(request.clone());
            let result = match request["method"].as_str().unwrap() {
                "status" => status("pulsar-test", 42, false),
                "validators" => {
                    let page = request["params"]["page"]
                        .as_str()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    let values = validators
                        .iter()
                        .skip((page - 1) * 100)
                        .take(100)
                        .cloned()
                        .collect::<Vec<_>>();
                    serde_json::json!({
                        "block_height": "42",
                        "validators": values,
                        "count": values.len().to_string(),
                        "total": validators.len().to_string(),
                    })
                }
                method => panic!("unexpected RPC method {method}"),
            };
            rpc_result(&request, &result)
        });
        let (url, stop, server) = spawn_rpc_server(handler).await;
        let client = client(url);

        assert_eq!(client.status().await.unwrap().latest_height, 42);
        assert_eq!(client.validator_public_keys(42).await.unwrap().len(), 101);
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

        stop.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn decodes_and_validates_proofs_by_height() {
        let id = VerificationId::from_component_hashes(
            ProofType::NoirBarretenberg,
            &[1; 32],
            &[2; 32],
            &[3; 32],
        );
        let response = QueryProofsByHeightResponse {
            proofs: vec![QueryProofResponse {
                proof_key: Some(ProofKey {
                    submission_height: 42,
                    index_in_block: 0,
                }),
                state: Some(query_proof_response::State::Pending(ProofRecord {
                    proof_hash: vec![1; 32],
                    proof_type: 2,
                    public_inputs_hash: vec![2; 32],
                    verification_key_hash: vec![3; 32],
                    verification_id: id.as_bytes().to_vec(),
                })),
            }],
            pagination: Some(PageResponse {
                next_key: Vec::new(),
                total: 1,
            }),
        };
        let encoded = STANDARD.encode(response.encode_to_vec());
        let handler: Handler = Arc::new(move |request| {
            assert_eq!(request["method"], "abci_query");
            rpc_result(
                &request,
                &serde_json::json!({
                    "response": {
                        "code": 0,
                        "codespace": "",
                        "height": "42",
                        "index": "0",
                        "info": "",
                        "key": "",
                        "log": "",
                        "proofOps": null,
                        "value": encoded,
                    }
                }),
            )
        });
        let (url, stop, server) = spawn_rpc_server(handler).await;

        let proofs = client(url).proofs_by_height(42).await.unwrap();
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].verification_id, id);

        stop.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn pins_proof_pagination_to_the_first_response_height() {
        let calls = Arc::new(Mutex::new(0_usize));
        let recorded = Arc::clone(&calls);
        let handler: Handler = Arc::new(move |request| {
            let mut call = recorded.lock().unwrap();
            let index = *call;
            *call += 1;
            if index == 1 {
                assert_eq!(request["params"]["height"], "99");
            }
            let component = u8::try_from(index + 1).unwrap();
            let id = VerificationId::from_component_hashes(
                ProofType::NoirBarretenberg,
                &[component; 32],
                &[2; 32],
                &[3; 32],
            );
            let response = QueryProofsByHeightResponse {
                proofs: vec![QueryProofResponse {
                    proof_key: Some(ProofKey {
                        submission_height: 42,
                        index_in_block: u32::try_from(index).unwrap(),
                    }),
                    state: Some(query_proof_response::State::Pending(ProofRecord {
                        proof_hash: vec![component; 32],
                        proof_type: 2,
                        public_inputs_hash: vec![2; 32],
                        verification_key_hash: vec![3; 32],
                        verification_id: id.as_bytes().to_vec(),
                    })),
                }],
                pagination: Some(PageResponse {
                    next_key: (index == 0).then(|| vec![7]).unwrap_or_default(),
                    total: 2,
                }),
            };
            rpc_result(
                &request,
                &serde_json::json!({
                    "response": {
                        "code": 0,
                        "codespace": "",
                        "height": "99",
                        "index": "0",
                        "info": "",
                        "key": "",
                        "log": "",
                        "proofOps": null,
                        "value": STANDARD.encode(response.encode_to_vec()),
                    }
                }),
            )
        });
        let (url, stop, server) = spawn_rpc_server(handler).await;

        assert_eq!(client(url).proofs_by_height(42).await.unwrap().len(), 2);
        assert_eq!(*calls.lock().unwrap(), 2);

        stop.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn validator_queries_accept_heights_above_u32() {
        let height = u64::from(u32::MAX) + 1;
        let public = validator(identity::Keypair::generate_ed25519().public());
        let handler: Handler = Arc::new(move |request| {
            assert_eq!(request["params"]["height"], height.to_string());
            rpc_result(
                &request,
                &serde_json::json!({
                    "block_height": height.to_string(),
                    "validators": [public],
                    "count": "1",
                    "total": "1",
                }),
            )
        });
        let (url, stop, server) = spawn_rpc_server(handler).await;

        assert_eq!(
            client(url)
                .validator_public_keys(height)
                .await
                .unwrap()
                .len(),
            1
        );

        stop.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_chain_id_mismatch_and_syncing_node() {
        for result in [status("wrong", 9, false), status("expected", 9, true)] {
            let handler: Handler = Arc::new(move |request| rpc_result(&request, &result));
            let (url, stop, server) = spawn_rpc_server(handler).await;
            assert!(client(url).status().await.is_err());
            stop.cancel();
            server.await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "requires a running Pulsar/CometBFT v0.38.21 node"]
    async fn real_cometbft_v0_38_compatibility_smoke() {
        let rpc_url = std::env::var("PULSAR_COMET_RPC_URL")
            .expect("PULSAR_COMET_RPC_URL must point to the test node");
        let chain_id =
            std::env::var("PULSAR_CHAIN_ID").expect("PULSAR_CHAIN_ID must match the test node");
        let client = PulsarClient::new(&ChainConfig {
            chain_id,
            comet_rpc_url: rpc_url,
            request_timeout: Duration::from_secs(10),
        })
        .unwrap();

        let status = client.status().await.unwrap();
        assert!(
            !client
                .validator_public_keys(status.latest_height)
                .await
                .unwrap()
                .is_empty()
        );
        let subscription = client.subscribe_new_blocks().await.unwrap();
        subscription.close().await.unwrap();
    }

    fn client(url: String) -> PulsarClient {
        PulsarClient::new(&ChainConfig {
            chain_id: "pulsar-test".to_owned(),
            comet_rpc_url: url,
            request_timeout: Duration::from_secs(2),
        })
        .unwrap()
    }

    fn validator(public: identity::PublicKey) -> Value {
        let public = public.try_into_ed25519().unwrap();
        serde_json::json!({
            "address": "0000000000000000000000000000000000000000",
            "pub_key": {
                "type": "tendermint/PubKeyEd25519",
                "value": STANDARD.encode(public.to_bytes()),
            },
            "voting_power": "1",
            "proposer_priority": "0",
        })
    }

    fn status(chain_id: &str, height: u64, catching_up: bool) -> Value {
        serde_json::json!({
            "node_info": {
                "protocol_version": { "p2p": "8", "block": "11", "app": "1" },
                "id": "0000000000000000000000000000000000000000",
                "listen_addr": "tcp://0.0.0.0:26656",
                "network": chain_id,
                "version": "0.38.21",
                "channels": "40202122233038606100",
                "moniker": "test",
                "other": { "tx_index": "on", "rpc_address": "tcp://0.0.0.0:26657" }
            },
            "sync_info": {
                "latest_block_hash": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "latest_app_hash": "",
                "latest_block_height": height.to_string(),
                "latest_block_time": "2026-01-01T00:00:00Z",
                "earliest_block_hash": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                "earliest_app_hash": "",
                "earliest_block_height": "1",
                "earliest_block_time": "2026-01-01T00:00:00Z",
                "catching_up": catching_up
            },
            "validator_info": {
                "address": "0000000000000000000000000000000000000000",
                "pub_key": {
                    "type": "tendermint/PubKeyEd25519",
                    "value": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
                },
                "voting_power": "1"
            }
        })
    }

    fn rpc_result(request: &Value, result: &Value) -> Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": request["id"].clone(),
            "result": result,
        })
    }

    async fn spawn_rpc_server(handler: Handler) -> (String, CancellationToken, JoinHandle<()>) {
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
                        let handler = Arc::clone(&handler);
                        tokio::spawn(async move { serve_connection(stream, handler).await });
                    }
                }
            }
        });
        (format!("http://{address}"), cancellation, task)
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
