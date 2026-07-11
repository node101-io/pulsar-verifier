mod event;
mod expiry;
mod record;

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use libp2p::PeerId;
use moka::{
    future::Cache,
    notification::RemovalCause,
    ops::compute::{CompResult, Op},
};
use tokio::sync::broadcast;

use crate::{
    Error, Result,
    config::ProofStoreConfig,
    proof::{ProofContent, ProofHash, ProofType},
};

pub use event::{ProofEvictionCause, ProofSource, ProofStoreEvent, ProofStoreSubscription};
use expiry::TerminalExpiry;
pub use record::{
    ChainProofStatus, ProofMetadata, ProofRecord, StoreChange, StoredProof, VerificationJob,
    VerificationResult, VerificationState,
};

const METADATA_WEIGHT_BYTES: u32 = 256;

/// Concurrent, process-local source of truth for proof content and lifecycle state.
#[derive(Clone)]
pub struct ProofStore {
    records: Cache<ProofHash, Arc<ProofRecord>>,
    events: broadcast::Sender<ProofStoreEvent>,
    max_proof_bytes: usize,
}

impl ProofStore {
    /// Builds a bounded cache with terminal-state expiry and eviction notifications.
    ///
    /// # Errors
    ///
    /// Returns an error when a capacity, size, retention, or channel limit is zero.
    pub fn new(config: ProofStoreConfig) -> Result<Self> {
        if config.max_capacity_bytes == 0
            || config.max_proof_bytes == 0
            || config.terminal_retention.is_zero()
            || config.event_buffer == 0
        {
            return Err(Error::ProofStore(
                "capacity, proof limit, retention, and event buffer must be positive".to_owned(),
            ));
        }
        if u64::try_from(config.max_proof_bytes).unwrap_or(u64::MAX) > config.max_capacity_bytes {
            return Err(Error::ProofStore(
                "maximum proof size must not exceed cache capacity".to_owned(),
            ));
        }

        let (events, _) = broadcast::channel(config.event_buffer);
        let eviction_events = events.clone();
        let records = Cache::builder()
            .max_capacity(config.max_capacity_bytes)
            .weigher(|_hash: &ProofHash, record: &Arc<ProofRecord>| {
                record
                    .proof_bytes
                    .as_ref()
                    .map_or(METADATA_WEIGHT_BYTES, |bytes| {
                        u32::try_from(bytes.len())
                            .unwrap_or(u32::MAX)
                            .saturating_add(METADATA_WEIGHT_BYTES)
                    })
            })
            .expire_after(TerminalExpiry::new(config.terminal_retention))
            .eviction_listener(move |hash, _record, cause| {
                let cause = match cause {
                    RemovalCause::Expired => ProofEvictionCause::Expired,
                    RemovalCause::Explicit => ProofEvictionCause::Explicit,
                    RemovalCause::Size => ProofEvictionCause::Size,
                    RemovalCause::Replaced => return,
                };
                if eviction_events
                    .send(ProofStoreEvent::ProofEvicted {
                        proof_hash: *hash,
                        cause,
                    })
                    .is_err()
                {
                    tracing::trace!(?hash, ?cause, "proof eviction had no subscribers");
                }
            })
            .build();

        Ok(Self {
            records,
            events,
            max_proof_bytes: config.max_proof_bytes,
        })
    }

    /// Records canonical chain metadata without requiring proof content to exist.
    ///
    /// # Errors
    ///
    /// Returns an error if an existing record has a different proof type.
    pub async fn observe_chain_proof(
        &self,
        hash: ProofHash,
        proof_type: ProofType,
    ) -> Result<StoreChange> {
        let observed_at = Instant::now();
        let incoming_type = proof_type.clone();
        let result = self
            .records
            .entry(hash)
            .and_try_compute_with(move |entry| async move {
                let Some(entry) = entry else {
                    return Ok::<_, Error>(Op::Put(Arc::new(ProofRecord {
                        metadata: ProofMetadata {
                            proof_type,
                            chain_observed_at: Some(observed_at),
                            content_source: None,
                            content_stored_at: None,
                            verification: VerificationState::NotStarted,
                            completed_at: None,
                        },
                        proof_bytes: None,
                    })));
                };
                let current = entry.value();
                ensure_type(hash, &current.metadata.proof_type, &incoming_type)?;
                if current.metadata.chain_observed_at.is_some() {
                    return Ok::<_, Error>(Op::Nop);
                }
                let mut updated = current.as_ref().clone();
                updated.metadata.chain_observed_at = Some(observed_at);
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let change = store_change(&result);
        if change != StoreChange::Unchanged {
            self.publish(ProofStoreEvent::ChainProofObserved { proof_hash: hash });
        }
        Ok(change)
    }

    /// Stores proof bytes supplied by a trusted local boundary such as RPC.
    ///
    /// # Errors
    ///
    /// Returns an error for oversized or hash-mismatched content, or conflicting type metadata.
    pub async fn insert_local_proof(
        &self,
        hash: ProofHash,
        proof_type: ProofType,
        bytes: Bytes,
        source: ProofSource,
    ) -> Result<StoreChange> {
        self.validate_content(hash, &bytes)?;
        let stored_at = Instant::now();
        let incoming_type = proof_type.clone();
        let result = self
            .records
            .entry(hash)
            .and_try_compute_with(move |entry| async move {
                let Some(entry) = entry else {
                    return Ok::<_, Error>(Op::Put(Arc::new(ProofRecord {
                        metadata: ProofMetadata {
                            proof_type,
                            chain_observed_at: None,
                            content_source: Some(source),
                            content_stored_at: Some(stored_at),
                            verification: VerificationState::NotStarted,
                            completed_at: None,
                        },
                        proof_bytes: Some(bytes),
                    })));
                };
                let current = entry.value();
                ensure_type(hash, &current.metadata.proof_type, &incoming_type)?;
                if current.proof_bytes.is_some() {
                    return Ok::<_, Error>(Op::Nop);
                }
                let mut updated = current.as_ref().clone();
                updated.proof_bytes = Some(bytes);
                updated.metadata.content_source = Some(source);
                updated.metadata.content_stored_at = Some(stored_at);
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let change = store_change(&result);
        if change != StoreChange::Unchanged {
            self.publish(ProofStoreEvent::ProofStored {
                proof_hash: hash,
                source,
            });
        }
        Ok(change)
    }

    /// Attaches a requested peer response to metadata previously observed on-chain.
    ///
    /// # Errors
    ///
    /// Returns an error if content is invalid or the hash was not observed on-chain.
    pub async fn attach_downloaded_proof(
        &self,
        hash: ProofHash,
        bytes: Bytes,
        peer: PeerId,
    ) -> Result<StoreChange> {
        self.validate_content(hash, &bytes)?;
        let stored_at = Instant::now();
        let source = ProofSource::Peer(peer);
        let result = self
            .records
            .entry(hash)
            .and_try_compute_with(move |entry| async move {
                let entry = entry.ok_or(Error::ProofNotObserved(hash))?;
                let current = entry.value();
                if current.metadata.chain_observed_at.is_none() {
                    return Err(Error::ProofNotObserved(hash));
                }
                if current.proof_bytes.is_some() {
                    return Ok::<_, Error>(Op::Nop);
                }
                let mut updated = current.as_ref().clone();
                updated.proof_bytes = Some(bytes);
                updated.metadata.content_source = Some(source);
                updated.metadata.content_stored_at = Some(stored_at);
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let change = store_change(&result);
        if change != StoreChange::Unchanged {
            self.publish(ProofStoreEvent::ProofStored {
                proof_hash: hash,
                source,
            });
        }
        Ok(change)
    }

    #[must_use]
    pub async fn get(&self, hash: ProofHash) -> Option<Arc<ProofRecord>> {
        self.records.get(&hash).await
    }

    #[must_use]
    pub async fn get_content(&self, hash: ProofHash) -> Option<ProofContent> {
        let record = self.get(hash).await?;
        Some(ProofContent {
            proof_hash: hash,
            proof: record.proof_bytes.clone()?,
        })
    }

    /// Claims one ready proof for verification; concurrent claims are serialized per hash.
    ///
    /// # Errors
    ///
    /// Returns an error if the atomic cache transition fails.
    pub async fn begin_verification(&self, hash: ProofHash) -> Result<Option<VerificationJob>> {
        let result = self
            .records
            .entry(hash)
            .and_try_compute_with(move |entry| async move {
                let Some(entry) = entry else {
                    return Ok::<_, Error>(Op::Nop);
                };
                let current = entry.value();
                if current.metadata.chain_observed_at.is_none()
                    || current.proof_bytes.is_none()
                    || current.metadata.verification != VerificationState::NotStarted
                {
                    return Ok::<_, Error>(Op::Nop);
                }
                let mut updated = current.as_ref().clone();
                updated.metadata.verification = VerificationState::Verifying;
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let CompResult::ReplacedWith(entry) = result else {
            return Ok(None);
        };
        let record = entry.value();
        let Some(proof_bytes) = record.proof_bytes.clone() else {
            return Err(Error::InvalidVerificationTransition {
                proof_hash: hash,
                reason: "claimed record has no proof bytes".to_owned(),
            });
        };
        self.publish(ProofStoreEvent::VerificationChanged {
            proof_hash: hash,
            state: VerificationState::Verifying,
        });
        Ok(Some(VerificationJob {
            proof_hash: hash,
            proof_type: record.metadata.proof_type.clone(),
            proof_bytes,
        }))
    }

    /// Commits the terminal cryptographic result and starts its retention period.
    ///
    /// # Errors
    ///
    /// Returns an error unless the record is currently in `Verifying` or already
    /// has the same terminal result.
    pub async fn finish_verification(
        &self,
        hash: ProofHash,
        result: VerificationResult,
    ) -> Result<StoreChange> {
        let terminal = match result {
            VerificationResult::Verified => VerificationState::Verified,
            VerificationResult::Wrong => VerificationState::Wrong,
        };
        let completed_at = Instant::now();
        let computed = self
            .records
            .entry(hash)
            .and_try_compute_with(move |entry| async move {
                let entry = entry.ok_or_else(|| Error::InvalidVerificationTransition {
                    proof_hash: hash,
                    reason: "record does not exist".to_owned(),
                })?;
                let current = entry.value();
                if current.metadata.verification == terminal {
                    return Ok::<_, Error>(Op::Nop);
                }
                if current.metadata.verification != VerificationState::Verifying {
                    return Err(Error::InvalidVerificationTransition {
                        proof_hash: hash,
                        reason: format!(
                            "expected Verifying, found {:?}",
                            current.metadata.verification
                        ),
                    });
                }
                let mut updated = current.as_ref().clone();
                updated.metadata.verification = terminal;
                updated.metadata.completed_at = Some(completed_at);
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let change = store_change(&computed);
        if change != StoreChange::Unchanged {
            self.publish(ProofStoreEvent::VerificationChanged {
                proof_hash: hash,
                state: terminal,
            });
        }
        Ok(change)
    }

    pub async fn statuses(&self, hashes: &[ProofHash]) -> Vec<ChainProofStatus> {
        let mut statuses = Vec::with_capacity(hashes.len());
        for hash in hashes {
            let status = self
                .get(*hash)
                .await
                .map_or(ChainProofStatus::Unavailable, |record| {
                    match record.metadata.verification {
                        VerificationState::Verified => ChainProofStatus::Verified,
                        VerificationState::Wrong => ChainProofStatus::Wrong,
                        VerificationState::NotStarted | VerificationState::Verifying => {
                            ChainProofStatus::Unavailable
                        }
                    }
                });
            statuses.push(status);
        }
        statuses
    }

    pub async fn invalidate(&self, hash: ProofHash) {
        self.records.invalidate(&hash).await;
        self.records.run_pending_tasks().await;
    }

    #[must_use]
    pub fn subscribe(&self) -> ProofStoreSubscription {
        ProofStoreSubscription {
            receiver: self.events.subscribe(),
        }
    }

    #[must_use]
    pub fn records_waiting_for_content(&self) -> Vec<StoredProof> {
        self.snapshot(|record| {
            record.metadata.chain_observed_at.is_some() && record.proof_bytes.is_none()
        })
    }

    #[must_use]
    pub fn records_ready_for_verification(&self) -> Vec<StoredProof> {
        self.snapshot(|record| {
            record.metadata.chain_observed_at.is_some()
                && record.proof_bytes.is_some()
                && record.metadata.verification == VerificationState::NotStarted
        })
    }

    #[must_use]
    pub fn locally_available_proofs(&self) -> Vec<StoredProof> {
        self.snapshot(|record| record.proof_bytes.is_some())
    }

    fn snapshot(&self, predicate: impl Fn(&ProofRecord) -> bool) -> Vec<StoredProof> {
        self.records
            .iter()
            .filter_map(|(hash, record)| {
                predicate(&record).then(|| StoredProof {
                    hash: *hash,
                    record,
                })
            })
            .collect()
    }

    fn validate_content(&self, hash: ProofHash, bytes: &Bytes) -> Result<()> {
        if bytes.len() > self.max_proof_bytes {
            return Err(Error::ProofTooLarge {
                proof_hash: hash,
                actual_bytes: bytes.len(),
                max_bytes: self.max_proof_bytes,
            });
        }
        if ProofHash::digest(bytes) != hash {
            return Err(Error::ProofHashMismatch(hash));
        }
        Ok(())
    }

    fn publish(&self, event: ProofStoreEvent) {
        if self.events.send(event).is_err() {
            tracing::trace!("proof store event had no subscribers");
        }
    }

    #[cfg(test)]
    pub(crate) async fn run_pending_tasks(&self) {
        self.records.run_pending_tasks().await;
    }
}

fn ensure_type(hash: ProofHash, existing: &ProofType, incoming: &ProofType) -> Result<()> {
    if existing != incoming {
        return Err(Error::ProofTypeConflict {
            proof_hash: hash,
            existing: existing.clone(),
            incoming: incoming.clone(),
        });
    }
    Ok(())
}

fn store_change(result: &CompResult<ProofHash, Arc<ProofRecord>>) -> StoreChange {
    match result {
        CompResult::Inserted(_) => StoreChange::Inserted,
        CompResult::ReplacedWith(_) => StoreChange::Updated,
        CompResult::Unchanged(_) | CompResult::StillNone(_) | CompResult::Removed(_) => {
            StoreChange::Unchanged
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use super::*;

    fn config() -> ProofStoreConfig {
        ProofStoreConfig {
            max_capacity_bytes: 1024 * 1024,
            max_proof_bytes: 1024,
            terminal_retention: Duration::from_secs(60),
            event_buffer: 16,
        }
    }

    fn proof_type() -> ProofType {
        ProofType::new("mock").unwrap()
    }

    async fn stored_chain_proof(store: &ProofStore, bytes: &[u8]) -> ProofHash {
        let hash = ProofHash::digest(bytes);
        store.observe_chain_proof(hash, proof_type()).await.unwrap();
        store
            .insert_local_proof(
                hash,
                proof_type(),
                Bytes::copy_from_slice(bytes),
                ProofSource::Rpc,
            )
            .await
            .unwrap();
        hash
    }

    #[tokio::test]
    async fn merges_chain_first_metadata_with_content() {
        let store = ProofStore::new(config()).unwrap();
        let mut events = store.subscribe();
        let proof = Bytes::from_static(b"proof");
        let hash = ProofHash::digest(&proof);

        assert_eq!(
            store.observe_chain_proof(hash, proof_type()).await.unwrap(),
            StoreChange::Inserted
        );
        let metadata_only = store.get(hash).await.unwrap();
        assert!(metadata_only.metadata.chain_observed_at.is_some());
        assert!(metadata_only.proof_bytes.is_none());
        assert!(matches!(
            events.recv().await.unwrap(),
            ProofStoreEvent::ChainProofObserved { proof_hash } if proof_hash == hash
        ));

        assert_eq!(
            store
                .attach_downloaded_proof(hash, proof.clone(), PeerId::random())
                .await
                .unwrap(),
            StoreChange::Updated
        );
        assert_eq!(store.get(hash).await.unwrap().proof_bytes, Some(proof));
        assert!(matches!(
            events.recv().await.unwrap(),
            ProofStoreEvent::ProofStored { proof_hash, .. } if proof_hash == hash
        ));
    }

    #[tokio::test]
    async fn merges_proof_first_with_chain_observation_without_duplicate_events() {
        let store = ProofStore::new(config()).unwrap();
        let mut events = store.subscribe();
        let proof = Bytes::from_static(b"proof-first");
        let hash = ProofHash::digest(&proof);

        assert_eq!(
            store
                .insert_local_proof(hash, proof_type(), proof, ProofSource::Rpc)
                .await
                .unwrap(),
            StoreChange::Inserted
        );
        assert!(matches!(
            events.recv().await.unwrap(),
            ProofStoreEvent::ProofStored { .. }
        ));
        assert_eq!(
            store.observe_chain_proof(hash, proof_type()).await.unwrap(),
            StoreChange::Updated
        );
        assert!(matches!(
            events.recv().await.unwrap(),
            ProofStoreEvent::ChainProofObserved { .. }
        ));

        assert_eq!(
            store.observe_chain_proof(hash, proof_type()).await.unwrap(),
            StoreChange::Unchanged
        );
        assert!(
            timeout(Duration::from_millis(20), events.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_unobserved_download_hash_mismatch_and_type_conflict() {
        let store = ProofStore::new(config()).unwrap();
        let proof = Bytes::from_static(b"proof");
        let hash = ProofHash::digest(&proof);
        assert!(matches!(
            store
                .attach_downloaded_proof(hash, proof.clone(), PeerId::random())
                .await,
            Err(Error::ProofNotObserved(_))
        ));
        assert!(matches!(
            store
                .insert_local_proof(
                    hash,
                    proof_type(),
                    Bytes::from_static(b"wrong"),
                    ProofSource::Rpc
                )
                .await,
            Err(Error::ProofHashMismatch(_))
        ));

        store.observe_chain_proof(hash, proof_type()).await.unwrap();
        let other = ProofType::new("groth16").unwrap();
        assert!(matches!(
            store.observe_chain_proof(hash, other).await,
            Err(Error::ProofTypeConflict { .. })
        ));
        assert_eq!(
            store.get(hash).await.unwrap().metadata.proof_type,
            proof_type()
        );
    }

    #[tokio::test]
    async fn verification_claim_is_single_flight_and_status_order_is_stable() {
        let store = ProofStore::new(config()).unwrap();
        let verified = stored_chain_proof(&store, b"verified").await;
        let wrong = stored_chain_proof(&store, b"wrong").await;
        let missing = ProofHash::digest(b"missing");

        let (first, second) = tokio::join!(
            store.begin_verification(verified),
            store.begin_verification(verified)
        );
        assert_eq!(
            usize::from(first.unwrap().is_some()) + usize::from(second.unwrap().is_some()),
            1
        );
        store
            .finish_verification(verified, VerificationResult::Verified)
            .await
            .unwrap();
        store.begin_verification(wrong).await.unwrap().unwrap();
        store
            .finish_verification(wrong, VerificationResult::Wrong)
            .await
            .unwrap();

        assert_eq!(
            store.statuses(&[missing, wrong, verified]).await,
            vec![
                ChainProofStatus::Unavailable,
                ChainProofStatus::Wrong,
                ChainProofStatus::Verified,
            ]
        );
    }

    #[tokio::test]
    async fn lagged_subscription_recovers_from_snapshot() {
        let mut config = config();
        config.event_buffer = 1;
        let store = ProofStore::new(config).unwrap();
        let mut events = store.subscribe();
        for proof in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            let hash = ProofHash::digest(proof);
            store
                .insert_local_proof(
                    hash,
                    proof_type(),
                    Bytes::copy_from_slice(proof),
                    ProofSource::Rpc,
                )
                .await
                .unwrap();
        }

        assert!(matches!(
            events.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        assert_eq!(store.locally_available_proofs().len(), 3);
    }

    #[tokio::test]
    async fn terminal_records_expire_without_read_extension() {
        let mut config = config();
        config.terminal_retention = Duration::from_millis(100);
        let store = ProofStore::new(config).unwrap();
        let mut events = store.subscribe();
        let hash = stored_chain_proof(&store, b"expiring").await;
        store.begin_verification(hash).await.unwrap().unwrap();
        store
            .finish_verification(hash, VerificationResult::Verified)
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;
        assert!(store.get(hash).await.is_some());
        // Moka's expiration wheel advances at roughly one-second resolution.
        sleep(Duration::from_millis(1_100)).await;
        store.run_pending_tasks().await;
        assert!(store.get(hash).await.is_none());
        store.run_pending_tasks().await;
        sleep(Duration::from_millis(20)).await;
        store.run_pending_tasks().await;

        let mut saw_expiry = false;
        let mut received = Vec::new();
        while let Ok(event) = timeout(Duration::from_millis(100), events.recv()).await {
            let event = event.unwrap();
            if matches!(
                event,
                ProofStoreEvent::ProofEvicted {
                    proof_hash,
                    cause: ProofEvictionCause::Expired,
                } if proof_hash == hash
            ) {
                saw_expiry = true;
                break;
            }
            received.push(event);
        }
        assert!(saw_expiry, "received events: {received:?}");
    }

    #[tokio::test]
    async fn explicit_and_size_eviction_emit_once_and_preserve_external_arc() {
        let store = ProofStore::new(config()).unwrap();
        let mut events = store.subscribe();
        let hash = stored_chain_proof(&store, b"retained").await;
        let retained = store.get(hash).await.unwrap();
        store.invalidate(hash).await;
        assert_eq!(
            retained.proof_bytes.as_deref(),
            Some(b"retained".as_slice())
        );

        let mut explicit = 0;
        while let Ok(Ok(event)) = timeout(Duration::from_millis(20), events.recv()).await {
            if matches!(
                event,
                ProofStoreEvent::ProofEvicted {
                    proof_hash,
                    cause: ProofEvictionCause::Explicit,
                } if proof_hash == hash
            ) {
                explicit += 1;
            }
        }
        assert_eq!(explicit, 1);

        let mut small = config();
        small.max_capacity_bytes = 512;
        small.max_proof_bytes = 400;
        let sized = ProofStore::new(small).unwrap();
        let mut size_events = sized.subscribe();
        let proof = Bytes::from(vec![7; 400]);
        let size_hash = ProofHash::digest(&proof);
        sized
            .insert_local_proof(size_hash, proof_type(), proof, ProofSource::Rpc)
            .await
            .unwrap();
        sized.run_pending_tasks().await;
        assert!(sized.get(size_hash).await.is_none());
        assert!(matches!(
            timeout(Duration::from_secs(1), size_events.recv())
                .await
                .unwrap()
                .unwrap(),
            ProofStoreEvent::ProofStored { .. }
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), size_events.recv())
                .await
                .unwrap()
                .unwrap(),
            ProofStoreEvent::ProofEvicted {
                cause: ProofEvictionCause::Size,
                ..
            }
        ));
    }
}
