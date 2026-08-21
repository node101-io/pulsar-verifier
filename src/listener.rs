use std::{collections::HashSet, sync::Arc, time::Duration};

use tokio::{sync::oneshot, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    chain::{CommittedBlock, CommittedEvent, PulsarClient, validate_descriptor, validate_position},
    config::{ChainConfig, ListenerConfig},
    p2p::ValidatorSetUpdater,
    store::ProofStore,
};

const PROOF_SUBMITTED_EVENT: &str = "verification.proof_submitted";
const ACTIVE_PROOF_HEIGHTS: u64 = 3;
const LISTENER_TASK: &str = "Pulsar listener";

const VERIFICATION_ID: &str = "verification_id";
const PROOF_HASH: &str = "proof_hash";
const PUBLIC_INPUTS_HASH: &str = "public_inputs_hash";
const VERIFICATION_KEY_HASH: &str = "verification_key_hash";
const SUBMISSION_HEIGHT: &str = "submission_height";
const INDEX_IN_BLOCK: &str = "index_in_block";
const PROOF_TYPE: &str = "proof_type";

pub(crate) struct ListenerExit {
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
}

impl ListenerExit {
    pub(crate) fn into_error(self) -> Error {
        match self.result {
            Ok(Ok(())) => Error::TaskExitedUnexpectedly(LISTENER_TASK),
            Ok(Err(error)) => error,
            Err(error) => Error::Task(error),
        }
    }
}

/// Owns committed block observation, bounded recovery, and reconnect lifecycle.
pub(crate) struct PulsarListener {
    stop: CancellationToken,
    task: Option<JoinHandle<Result<()>>>,
}

impl PulsarListener {
    pub(crate) async fn start(
        config: ListenerConfig,
        chain: ChainConfig,
        store: Arc<ProofStore>,
        validator_updates: Option<ValidatorSetUpdater>,
    ) -> Result<Self> {
        let client = PulsarClient::new(&chain)?;
        let stop = CancellationToken::new();
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(run_listener(
            config,
            client,
            store,
            validator_updates,
            stop.clone(),
            ready_tx,
        ));
        let mut listener = Self {
            stop,
            task: Some(task),
        };
        if ready_rx.await.is_ok() {
            Ok(listener)
        } else {
            let result = listener
                .task
                .as_mut()
                .expect("listener task exists during startup")
                .await;
            listener.task.take();
            match result {
                Ok(Err(error)) => Err(error),
                Ok(Ok(())) => Err(Error::TaskExitedUnexpectedly(LISTENER_TASK)),
                Err(error) => Err(Error::Task(error)),
            }
        }
    }

    pub(crate) async fn wait_for_exit(&mut self) -> ListenerExit {
        let result = self
            .task
            .as_mut()
            .expect("listener task exists while service is active")
            .await;
        self.task.take();
        ListenerExit { result }
    }

    pub(crate) async fn shutdown(mut self, shutdown_timeout: Duration) -> Result<()> {
        self.stop.cancel();
        let task = self
            .task
            .as_mut()
            .ok_or(Error::TaskExitedUnexpectedly(LISTENER_TASK))?;
        if let Ok(result) = timeout(shutdown_timeout, task).await {
            self.task.take();
            result.map_err(Error::Task)?
        } else {
            self.abort_and_join().await;
            Err(Error::ShutdownTimeout(shutdown_timeout))
        }
    }

    pub(crate) async fn force_shutdown(mut self) {
        self.abort_and_join().await;
    }

    async fn abort_and_join(&mut self) {
        let Some(task) = self.task.take() else {
            return;
        };
        task.abort();
        if let Err(error) = task.await
            && !error.is_cancelled()
        {
            tracing::warn!(%error, "failed to join force-stopped Pulsar listener");
        }
    }
}

impl Drop for PulsarListener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            tracing::warn!("Pulsar listener dropped before shutdown");
            task.abort();
        }
    }
}

struct ListenerState {
    last_height: Option<u64>,
    validators_hash: Option<[u8; 32]>,
}

enum SessionExit {
    Stopped,
    Reconnect { error: Error, established: bool },
}

async fn run_listener(
    config: ListenerConfig,
    client: PulsarClient,
    store: Arc<ProofStore>,
    validator_updates: Option<ValidatorSetUpdater>,
    stop: CancellationToken,
    ready: oneshot::Sender<()>,
) -> Result<()> {
    let mut ready = Some(ready);
    let mut backoff = config.reconnect_initial_backoff;
    let mut state = ListenerState {
        last_height: None,
        validators_hash: None,
    };

    loop {
        match run_session(
            &client,
            &store,
            validator_updates.as_ref(),
            &stop,
            &mut state,
            &mut ready,
        )
        .await
        {
            Ok(SessionExit::Stopped) => return Ok(()),
            Ok(SessionExit::Reconnect { error, established }) => {
                if established {
                    backoff = config.reconnect_initial_backoff;
                }
                tracing::warn!(%error, "Pulsar listener connection lost; reconnecting");
            }
            Err(error) => return Err(error),
        }

        let delay = full_jitter(backoff);
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay) => {}
        }
        backoff = backoff
            .checked_mul(2)
            .unwrap_or(config.reconnect_max_backoff)
            .min(config.reconnect_max_backoff);
    }
}

async fn run_session(
    client: &PulsarClient,
    store: &ProofStore,
    validator_updates: Option<&ValidatorSetUpdater>,
    stop: &CancellationToken,
    state: &mut ListenerState,
    ready: &mut Option<oneshot::Sender<()>>,
) -> Result<SessionExit> {
    // Subscribe first so blocks committed during the recovery queries remain queued.
    let mut blocks = match cancel_or(stop, client.subscribe_new_blocks()).await {
        Some(Ok(blocks)) => blocks,
        Some(Err(error)) if is_fatal(&error) => return Err(error),
        Some(Err(error)) => {
            return Ok(SessionExit::Reconnect {
                error,
                established: false,
            });
        }
        None => return Ok(SessionExit::Stopped),
    };
    let Some(initialization) =
        cancel_or(stop, initialize(client, store, validator_updates, state)).await
    else {
        blocks.close().await?;
        return Ok(SessionExit::Stopped);
    };
    match initialization {
        Ok(()) => {
            if let Some(ready) = ready.take() {
                let _ = ready.send(());
                tracing::info!("Pulsar committed-block listener is ready");
            }
        }
        Err(error) if is_fatal(&error) => {
            let _ = blocks.close().await;
            return Err(error);
        }
        Err(error) => {
            let _ = blocks.close().await;
            return Ok(SessionExit::Reconnect {
                error,
                established: false,
            });
        }
    }

    loop {
        tokio::select! {
            () = stop.cancelled() => {
                blocks.close().await?;
                return Ok(SessionExit::Stopped);
            }
            event = blocks.next() => {
                match event {
                    Ok(Some(block)) => {
                        if let Err(error) = process_block(client, store, validator_updates, state, block).await {
                            let _ = blocks.close().await;
                            return if is_fatal(&error) {
                                Err(error)
                            } else {
                                Ok(SessionExit::Reconnect {
                                    error,
                                    established: true,
                                })
                            };
                        }
                    }
                    Ok(None) => return Ok(SessionExit::Reconnect {
                        error: Error::Chain("NewBlock subscription closed".to_owned()),
                        established: true,
                    }),
                    Err(error) => return if is_fatal(&error) {
                        Err(error)
                    } else {
                        Ok(SessionExit::Reconnect {
                            error,
                            established: true,
                        })
                    },
                }
            }
        }
    }
}

async fn initialize(
    client: &PulsarClient,
    store: &ProofStore,
    validator_updates: Option<&ValidatorSetUpdater>,
    state: &mut ListenerState,
) -> Result<()> {
    let status = client.status().await?;
    reconcile_active_window(client, store, status.latest_height).await?;
    if let Some(updater) = validator_updates {
        let validators_hash = client.validators_hash(status.latest_height).await?;
        if validator_set_changed(state.validators_hash, validators_hash) {
            updater.replace_at(status.latest_height).await?;
        }
        state.validators_hash = Some(validators_hash);
    }
    state.last_height = Some(status.latest_height);
    Ok(())
}

fn validator_set_changed(previous: Option<[u8; 32]>, current: [u8; 32]) -> bool {
    previous != Some(current)
}

async fn process_block(
    client: &PulsarClient,
    store: &ProofStore,
    validator_updates: Option<&ValidatorSetUpdater>,
    state: &mut ListenerState,
    block: CommittedBlock,
) -> Result<()> {
    let observations = parse_proof_events(&block)?;

    if state
        .last_height
        .is_some_and(|height| block.height > height.saturating_add(1))
    {
        reconcile_active_window(client, store, block.height).await?;
    }
    for verification_id in observations {
        store.observe_chain_verification(verification_id).await?;
    }

    if state.last_height.is_none_or(|height| block.height > height) {
        if validator_set_changed(state.validators_hash, block.validators_hash)
            && let Some(updater) = validator_updates
        {
            // Fetch and validate the complete snapshot before mutating the Driver.
            updater.replace_at(block.height).await?;
        }
        state.validators_hash = Some(block.validators_hash);
        state.last_height = Some(block.height);
    }
    Ok(())
}

async fn reconcile_active_window(
    client: &PulsarClient,
    store: &ProofStore,
    latest_height: u64,
) -> Result<()> {
    for height in active_heights(latest_height) {
        for proof in client.proofs_by_height(height).await? {
            store
                .observe_chain_verification(proof.verification_id)
                .await?;
        }
    }
    Ok(())
}

fn parse_proof_events(block: &CommittedBlock) -> Result<Vec<crate::proof::VerificationId>> {
    let mut positions = HashSet::new();
    block
        .events
        .iter()
        .filter(|event| event.kind == PROOF_SUBMITTED_EVENT)
        .map(|event| {
            let attributes = attributes(event)?;
            let submission_height = parse_u64(&attributes, SUBMISSION_HEIGHT)?;
            let index_in_block = parse_u32(&attributes, INDEX_IN_BLOCK)?;
            validate_position(submission_height, index_in_block, block.height)?;
            if !positions.insert(index_in_block) {
                return Err(Error::InvalidChainContract(format!(
                    "duplicate proof index {index_in_block} in committed block {}",
                    block.height
                )));
            }
            validate_descriptor(
                parse_i32(&attributes, PROOF_TYPE)?,
                &parse_hex(&attributes, PROOF_HASH)?,
                &parse_hex(&attributes, PUBLIC_INPUTS_HASH)?,
                &parse_hex(&attributes, VERIFICATION_KEY_HASH)?,
                &parse_hex(&attributes, VERIFICATION_ID)?,
            )
        })
        .collect()
}

fn attributes(event: &CommittedEvent) -> Result<std::collections::HashMap<&str, &str>> {
    let mut values = std::collections::HashMap::with_capacity(event.attributes.len());
    for (key, value) in &event.attributes {
        if values.insert(key.as_str(), value.as_str()).is_some() {
            return Err(Error::InvalidChainContract(format!(
                "duplicate {key} attribute in {PROOF_SUBMITTED_EVENT}"
            )));
        }
    }
    Ok(values)
}

fn required<'a>(
    attributes: &'a std::collections::HashMap<&str, &str>,
    key: &str,
) -> Result<&'a str> {
    attributes.get(key).copied().ok_or_else(|| {
        Error::InvalidChainContract(format!("{PROOF_SUBMITTED_EVENT} is missing {key}"))
    })
}

fn parse_hex(attributes: &std::collections::HashMap<&str, &str>, key: &str) -> Result<Vec<u8>> {
    hex::decode(required(attributes, key)?).map_err(|error| {
        Error::InvalidChainContract(format!(
            "{PROOF_SUBMITTED_EVENT} has invalid {key}: {error}"
        ))
    })
}

fn parse_u64(attributes: &std::collections::HashMap<&str, &str>, key: &str) -> Result<u64> {
    required(attributes, key)?.parse().map_err(|error| {
        Error::InvalidChainContract(format!(
            "{PROOF_SUBMITTED_EVENT} has invalid {key}: {error}"
        ))
    })
}

fn parse_u32(attributes: &std::collections::HashMap<&str, &str>, key: &str) -> Result<u32> {
    required(attributes, key)?.parse().map_err(|error| {
        Error::InvalidChainContract(format!(
            "{PROOF_SUBMITTED_EVENT} has invalid {key}: {error}"
        ))
    })
}

fn parse_i32(attributes: &std::collections::HashMap<&str, &str>, key: &str) -> Result<i32> {
    required(attributes, key)?.parse().map_err(|error| {
        Error::InvalidChainContract(format!(
            "{PROOF_SUBMITTED_EVENT} has invalid {key}: {error}"
        ))
    })
}

fn active_heights(latest_height: u64) -> std::ops::RangeInclusive<u64> {
    latest_height
        .saturating_sub(ACTIVE_PROOF_HEIGHTS - 1)
        .max(1)..=latest_height
}

fn full_jitter(maximum: Duration) -> Duration {
    let max_millis = u64::try_from(maximum.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis(rand::random_range(0..=max_millis))
}

fn is_fatal(error: &Error) -> bool {
    matches!(
        error,
        Error::ChainIdMismatch { .. }
            | Error::InvalidChainContract(_)
            | Error::InvalidVerificationId(_)
            | Error::UnsupportedProofType(_)
            | Error::VerificationIdMismatch(_)
            | Error::LocalValidatorRemoved(_)
            | Error::ProofStore(_)
    )
}

async fn cancel_or<T>(
    stop: &CancellationToken,
    future: impl Future<Output = Result<T>>,
) -> Option<Result<T>> {
    tokio::select! {
        () = stop.cancelled() => None,
        result = future => Some(result),
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bytes::Bytes;
    use sha2::{Digest as _, Sha256};

    use super::*;
    use crate::{
        config::{ProofStoreConfig, VerificationConfig},
        proof::{Proof, ProofType, VerificationId},
        store::{ProofSource, VerificationFailure, VerificationState, VerificationVerdict},
        verification::{VerificationWorker, Verifier, VerifierRegistry},
    };

    fn proof_event(height: u64, index: u32) -> CommittedEvent {
        let proof_hash = [1; 32];
        let public_inputs_hash = [2; 32];
        let verification_key_hash = [3; 32];
        let id = VerificationId::from_component_hashes(
            ProofType::NoirBarretenberg,
            &proof_hash,
            &public_inputs_hash,
            &verification_key_hash,
        );
        CommittedEvent {
            kind: PROOF_SUBMITTED_EVENT.to_owned(),
            attributes: vec![
                (VERIFICATION_ID.to_owned(), hex::encode(id.as_bytes())),
                (PROOF_HASH.to_owned(), hex::encode(proof_hash)),
                (
                    PUBLIC_INPUTS_HASH.to_owned(),
                    hex::encode(public_inputs_hash),
                ),
                (
                    VERIFICATION_KEY_HASH.to_owned(),
                    hex::encode(verification_key_hash),
                ),
                (SUBMISSION_HEIGHT.to_owned(), height.to_string()),
                (INDEX_IN_BLOCK.to_owned(), index.to_string()),
                (PROOF_TYPE.to_owned(), "2".to_owned()),
            ],
        }
    }

    fn proof_event_for(proof: &Proof, height: u64, index: u32) -> CommittedEvent {
        let proof_hash: [u8; 32] = Sha256::digest(&proof.proof).into();
        let public_inputs_hash: [u8; 32] = Sha256::digest(&proof.public_inputs).into();
        let verification_key_hash: [u8; 32] = Sha256::digest(&proof.verification_key).into();
        CommittedEvent {
            kind: PROOF_SUBMITTED_EVENT.to_owned(),
            attributes: vec![
                (
                    VERIFICATION_ID.to_owned(),
                    hex::encode(proof.verification_id().as_bytes()),
                ),
                (PROOF_HASH.to_owned(), hex::encode(proof_hash)),
                (
                    PUBLIC_INPUTS_HASH.to_owned(),
                    hex::encode(public_inputs_hash),
                ),
                (
                    VERIFICATION_KEY_HASH.to_owned(),
                    hex::encode(verification_key_hash),
                ),
                (SUBMISSION_HEIGHT.to_owned(), height.to_string()),
                (INDEX_IN_BLOCK.to_owned(), index.to_string()),
                (
                    PROOF_TYPE.to_owned(),
                    i32::from(proof.proof_type).to_string(),
                ),
            ],
        }
    }

    #[test]
    fn parses_committed_proof_events_atomically() {
        let block = CommittedBlock {
            height: 42,
            validators_hash: [9; 32],
            events: vec![proof_event(42, 0), proof_event(42, 1)],
        };
        assert_eq!(parse_proof_events(&block).unwrap().len(), 2);

        let mut malformed = block;
        malformed.events[1]
            .attributes
            .retain(|(key, _)| key != VERIFICATION_ID);
        assert!(matches!(
            parse_proof_events(&malformed),
            Err(Error::InvalidChainContract(_))
        ));
    }

    #[test]
    fn rejects_duplicate_indices_and_mismatched_heights() {
        let duplicate = CommittedBlock {
            height: 42,
            validators_hash: [9; 32],
            events: vec![proof_event(42, 0), proof_event(42, 0)],
        };
        assert!(parse_proof_events(&duplicate).is_err());

        let wrong_height = CommittedBlock {
            height: 42,
            validators_hash: [9; 32],
            events: vec![proof_event(41, 0)],
        };
        assert!(parse_proof_events(&wrong_height).is_err());
    }

    #[test]
    fn active_window_is_fixed_to_three_committed_heights() {
        assert_eq!(active_heights(1).collect::<Vec<_>>(), vec![1]);
        assert_eq!(active_heights(2).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(active_heights(10).collect::<Vec<_>>(), vec![8, 9, 10]);
    }

    #[test]
    fn validator_refresh_is_needed_only_when_the_hash_changes() {
        assert!(validator_set_changed(None, [1; 32]));
        assert!(!validator_set_changed(Some([1; 32]), [1; 32]));
        assert!(validator_set_changed(Some([1; 32]), [2; 32]));
    }

    #[tokio::test]
    async fn committed_event_observes_the_store_without_a_gap_query() {
        let store = ProofStore::new(ProofStoreConfig::test_default()).unwrap();
        let block = CommittedBlock {
            height: 42,
            validators_hash: [9; 32],
            events: vec![proof_event(42, 0)],
        };
        let expected = parse_proof_events(&block).unwrap()[0];
        let client = PulsarClient::new(&ChainConfig {
            chain_id: "unused".to_owned(),
            comet_rpc_url: "http://127.0.0.1:1".to_owned(),
            request_timeout: Duration::from_millis(10),
        })
        .unwrap();
        let mut state = ListenerState {
            last_height: Some(41),
            validators_hash: Some([9; 32]),
        };

        process_block(&client, &store, None, &mut state, block)
            .await
            .unwrap();

        assert!(store.get(expected).await.is_some());
        assert_eq!(state.last_height, Some(42));
    }

    struct ValidVerifier;

    #[async_trait]
    impl Verifier for ValidVerifier {
        async fn verify(
            &self,
            _proof: &Proof,
        ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
            Ok(VerificationVerdict::Valid)
        }
    }

    #[tokio::test]
    async fn committed_observation_drives_the_verification_pipeline() {
        let store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
        let proof = Proof {
            proof_type: ProofType::NoirBarretenberg,
            proof: Bytes::from_static(b"proof"),
            public_inputs: Bytes::from_static(b"inputs"),
            verification_key: Bytes::from_static(b"key"),
        };
        let verification_id = proof.verification_id();
        store
            .insert_local_proof(proof.clone(), ProofSource::Rpc)
            .await
            .unwrap();
        let worker = VerificationWorker::new(
            Arc::clone(&store),
            VerifierRegistry::new([(
                ProofType::NoirBarretenberg,
                Arc::new(ValidVerifier) as Arc<dyn Verifier>,
            )])
            .unwrap(),
            VerificationConfig {
                max_concurrent_jobs: 2,
                job_timeout: Duration::from_secs(1),
                max_retries: 0,
                retry_backoff: Duration::ZERO,
            },
        );
        let stop = CancellationToken::new();
        let task = tokio::spawn(worker.run(stop.clone()));
        let client = PulsarClient::new(&ChainConfig {
            chain_id: "unused".to_owned(),
            comet_rpc_url: "http://127.0.0.1:1".to_owned(),
            request_timeout: Duration::from_millis(10),
        })
        .unwrap();
        let mut state = ListenerState {
            last_height: Some(41),
            validators_hash: Some([9; 32]),
        };
        process_block(
            &client,
            &store,
            None,
            &mut state,
            CommittedBlock {
                height: 42,
                validators_hash: [9; 32],
                events: vec![proof_event_for(&proof, 42, 0)],
            },
        )
        .await
        .unwrap();

        timeout(Duration::from_secs(1), async {
            loop {
                let record = store.get(verification_id).await.unwrap();
                if matches!(
                    record.metadata.verification,
                    Some(VerificationState::Completed(VerificationVerdict::Valid))
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.cancel();
        task.await.unwrap().unwrap();
    }
}
