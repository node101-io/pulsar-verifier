use std::{collections::HashSet, fmt, time::Duration};

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

/// One owned `NewBlock` subscription and the transport task that drives it.
#[allow(dead_code, reason = "used by the listener in the next stacked change")]
pub(crate) struct NewBlockSubscription {
    client: Option<WebSocketClient>,
    events: Option<Subscription>,
    driver: Option<JoinHandle<std::result::Result<(), tendermint_rpc::Error>>>,
    timeout: Duration,
}

impl NewBlockSubscription {
    #[allow(dead_code, reason = "used by the listener in the next stacked change")]
    pub(crate) async fn next(&mut self) -> Result<Option<Event>> {
        timeout(
            self.timeout,
            self.events
                .as_mut()
                .expect("subscription exists until close")
                .next(),
        )
        .await
        .map_err(|_| Error::Chain("NewBlock subscription timed out".to_owned()))?
        .transpose()
        .map_err(chain_error)
    }

    #[allow(dead_code, reason = "used by the listener in the next stacked change")]
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
            return Err(Error::Chain(format!(
                "CometBFT chain ID mismatch: expected {}, got {actual_chain_id}",
                self.chain_id
            )));
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

    /// Loads one complete validator snapshot at an exact committed height.
    pub(crate) async fn validator_public_keys(&self, height: u64) -> Result<Vec<[u8; 32]>> {
        let height_u32 = u32::try_from(height)
            .map_err(|_| Error::Chain(format!("validator height {height} exceeds u32")))?;
        let response = self
            .with_timeout("validators", self.http.validators(height_u32, Paging::All))
            .await?;
        if response.block_height.value() != height {
            return Err(Error::Chain(format!(
                "validator response height {} does not match requested height {height}",
                response.block_height.value()
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

    /// Recovers pending verification requests submitted at one block height.
    #[allow(dead_code, reason = "used by the listener in the next stacked change")]
    pub(crate) async fn proofs_by_height(&self, height: u64) -> Result<Vec<ChainProof>> {
        if height == 0 {
            return Err(Error::Chain(
                "proof query height must be positive".to_owned(),
            ));
        }
        let mut next_key = Vec::new();
        let mut proofs = Vec::new();
        let mut positions = HashSet::new();

        loop {
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
                        None,
                        false,
                    ),
                )
                .await?;
            if !response.code.is_ok() {
                return Err(Error::Chain(format!(
                    "ProofsByHeight ABCI query failed with code {}: {}",
                    response.code.value(),
                    response.log
                )));
            }
            let page = QueryProofsByHeightResponse::decode(response.value.as_slice()).map_err(
                |error| Error::Chain(format!("invalid ProofsByHeight response: {error}")),
            )?;
            let page_was_empty = page.proofs.is_empty();
            for proof in page.proofs {
                let key = proof.proof_key.as_ref().ok_or_else(|| {
                    Error::Chain("proof query response is missing proof_key".to_owned())
                })?;
                if !positions.insert((key.submission_height, key.index_in_block)) {
                    return Err(Error::Chain(format!(
                        "duplicate proof index {} at height {}",
                        key.index_in_block, key.submission_height
                    )));
                }
                if let Some(proof) = validate_query_proof(proof, height)? {
                    proofs.push(proof);
                }
            }
            if positions.len() > MAX_PROOFS_PER_BLOCK {
                return Err(Error::Chain(format!(
                    "ProofsByHeight returned more than {MAX_PROOFS_PER_BLOCK} records"
                )));
            }

            next_key = page.pagination.map_or_else(Vec::new, |page| page.next_key);
            if next_key.is_empty() {
                break;
            }
            if page_was_empty {
                return Err(Error::Chain(
                    "ProofsByHeight returned an empty page with a continuation key".to_owned(),
                ));
            }
        }

        proofs.sort_unstable_by_key(|proof| proof.index_in_block);
        Ok(proofs)
    }

    #[allow(dead_code, reason = "used by the listener in the next stacked change")]
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
