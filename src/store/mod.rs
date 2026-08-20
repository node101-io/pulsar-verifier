mod event;
mod expiry;
mod record;

use std::{sync::Arc, time::Instant};

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
    proof::{Proof, VerificationId},
};

pub use event::{ProofEvictionCause, ProofSource, ProofStoreEvent, ProofStoreSubscription};
use expiry::TerminalExpiry;
pub use record::{
    CompletedVerification, ProofMetadata, ProofRecord, StoreChange, StoredProof,
    StoredVerificationStatus, VerificationFailure, VerificationJob, VerificationOutcome,
    VerificationState, VerificationStatus, VerificationVerdict,
};

const METADATA_WEIGHT_BYTES: u32 = 256;

/// Concurrent, process-local source of truth for proof content and lifecycle state.
#[derive(Clone)]
pub struct ProofStore {
    records: Cache<VerificationId, Arc<ProofRecord>>,
    events: broadcast::Sender<ProofStoreEvent>,
    max_proof_bytes: usize,
}

impl ProofStore {
    /// Builds a bounded cache with terminal-state expiry and eviction notifications.
    ///
    /// # Errors
    ///
    /// Returns an error when a capacity, size, retention, or channel limit is invalid.
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
        let maximum_weight = u32::try_from(config.max_proof_bytes)
            .ok()
            .and_then(|weight| weight.checked_add(METADATA_WEIGHT_BYTES));
        if maximum_weight.is_none() {
            return Err(Error::ProofStore(
                "maximum proof size exceeds Moka's per-entry weight range".to_owned(),
            ));
        }

        let (events, _) = broadcast::channel(config.event_buffer);
        let eviction_events = events.clone();
        let records = Cache::builder()
            .max_capacity(config.max_capacity_bytes)
            .weigher(|_id: &VerificationId, record: &Arc<ProofRecord>| proof_weight(record))
            .expire_after(TerminalExpiry::new(config.terminal_retention))
            .eviction_listener(move |verification_id, _record, cause| {
                let cause = match cause {
                    RemovalCause::Expired => ProofEvictionCause::Expired,
                    RemovalCause::Explicit => ProofEvictionCause::Explicit,
                    RemovalCause::Size => ProofEvictionCause::Size,
                    RemovalCause::Replaced => return,
                };
                if eviction_events
                    .send(ProofStoreEvent::ProofEvicted {
                        verification_id: *verification_id,
                        cause,
                    })
                    .is_err()
                {
                    tracing::trace!(
                        ?verification_id,
                        ?cause,
                        "proof eviction had no subscribers"
                    );
                }
            })
            .build();

        Ok(Self {
            records,
            events,
            max_proof_bytes: config.max_proof_bytes,
        })
    }

    /// Records a canonical chain identity without requiring proof content.
    ///
    /// # Errors
    ///
    /// Returns an error if Moka cannot commit the atomic transition.
    pub async fn observe_chain_verification(
        &self,
        verification_id: VerificationId,
    ) -> Result<StoreChange> {
        let observed_at = Instant::now();
        let result = self
            .records
            .entry(verification_id)
            .and_try_compute_with(move |entry| async move {
                let Some(entry) = entry else {
                    return Ok::<_, Error>(Op::Put(Arc::new(ProofRecord {
                        metadata: ProofMetadata {
                            chain_observed_at: Some(observed_at),
                            content_source: None,
                            content_stored_at: None,
                            verification: None,
                            completed_at: None,
                        },
                        proof: None,
                    })));
                };
                let current = entry.value();
                if current.metadata.chain_observed_at.is_some() {
                    return Ok::<_, Error>(Op::Nop);
                }
                let mut updated = current.as_ref().clone();
                updated.metadata.chain_observed_at = Some(observed_at);
                queue_if_ready(&mut updated);
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let change = store_change(&result);
        if change != StoreChange::Unchanged {
            self.publish(ProofStoreEvent::VerificationObserved { verification_id });
            if result_state(&result) == Some(&VerificationState::Queued) {
                self.publish(ProofStoreEvent::VerificationChanged {
                    verification_id,
                    state: VerificationState::Queued,
                });
            }
        }
        Ok(change)
    }

    /// Stores a complete proof supplied by a trusted local boundary such as RPC.
    ///
    /// # Errors
    ///
    /// Returns an error when the proof exceeds the configured encoded-size limit.
    pub async fn insert_local_proof(
        &self,
        proof: Proof,
        source: ProofSource,
    ) -> Result<(VerificationId, StoreChange)> {
        let verification_id = proof.verification_id();
        self.validate_proof(verification_id, &proof)?;
        let stored_at = Instant::now();
        let result = self
            .records
            .entry(verification_id)
            .and_try_compute_with(move |entry| async move {
                let Some(entry) = entry else {
                    return Ok::<_, Error>(Op::Put(Arc::new(ProofRecord {
                        metadata: ProofMetadata {
                            chain_observed_at: None,
                            content_source: Some(source),
                            content_stored_at: Some(stored_at),
                            verification: None,
                            completed_at: None,
                        },
                        proof: Some(proof),
                    })));
                };
                let current = entry.value();
                if current.proof.is_some() {
                    return Ok::<_, Error>(Op::Nop);
                }
                let mut updated = current.as_ref().clone();
                updated.proof = Some(proof);
                updated.metadata.content_source = Some(source);
                updated.metadata.content_stored_at = Some(stored_at);
                queue_if_ready(&mut updated);
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let change = store_change(&result);
        if change != StoreChange::Unchanged {
            self.publish(ProofStoreEvent::ProofStored {
                verification_id,
                source,
            });
            if result_state(&result) == Some(&VerificationState::Queued) {
                self.publish(ProofStoreEvent::VerificationChanged {
                    verification_id,
                    state: VerificationState::Queued,
                });
            }
        }
        Ok((verification_id, change))
    }

    /// Attaches a requested peer response to an identity previously observed on-chain.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown identity, oversized proof, or ID mismatch.
    pub async fn attach_downloaded_proof(
        &self,
        expected_id: VerificationId,
        proof: Proof,
        peer: PeerId,
    ) -> Result<StoreChange> {
        self.validate_proof(expected_id, &proof)?;
        let stored_at = Instant::now();
        let source = ProofSource::Peer(peer);
        let result = self
            .records
            .entry(expected_id)
            .and_try_compute_with(move |entry| async move {
                let entry = entry.ok_or(Error::ProofNotObserved(expected_id))?;
                let current = entry.value();
                if current.metadata.chain_observed_at.is_none() {
                    return Err(Error::ProofNotObserved(expected_id));
                }
                if current.proof.is_some() {
                    return Ok::<_, Error>(Op::Nop);
                }
                let mut updated = current.as_ref().clone();
                updated.proof = Some(proof);
                updated.metadata.content_source = Some(source);
                updated.metadata.content_stored_at = Some(stored_at);
                queue_if_ready(&mut updated);
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let change = store_change(&result);
        if change != StoreChange::Unchanged {
            self.publish(ProofStoreEvent::ProofStored {
                verification_id: expected_id,
                source,
            });
            if result_state(&result) == Some(&VerificationState::Queued) {
                self.publish(ProofStoreEvent::VerificationChanged {
                    verification_id: expected_id,
                    state: VerificationState::Queued,
                });
            }
        }
        Ok(change)
    }

    #[must_use]
    pub async fn get(&self, verification_id: VerificationId) -> Option<Arc<ProofRecord>> {
        self.records.get(&verification_id).await
    }

    #[must_use]
    pub async fn get_proof(&self, verification_id: VerificationId) -> Option<Proof> {
        self.get(verification_id).await?.proof.clone()
    }

    /// Claims one ready proof for verification; concurrent claims are serialized per ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the atomic transition produces an inconsistent record.
    pub async fn begin_verification(
        &self,
        verification_id: VerificationId,
    ) -> Result<Option<VerificationJob>> {
        let result = self
            .records
            .entry(verification_id)
            .and_try_compute_with(move |entry| async move {
                let Some(entry) = entry else {
                    return Ok::<_, Error>(Op::Nop);
                };
                let current = entry.value();
                if current.metadata.chain_observed_at.is_none()
                    || current.proof.is_none()
                    || current.metadata.verification != Some(VerificationState::Queued)
                {
                    return Ok::<_, Error>(Op::Nop);
                }
                let mut updated = current.as_ref().clone();
                updated.metadata.verification = Some(VerificationState::Verifying);
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let CompResult::ReplacedWith(entry) = result else {
            return Ok(None);
        };
        let proof =
            entry
                .value()
                .proof
                .clone()
                .ok_or_else(|| Error::InvalidVerificationTransition {
                    verification_id,
                    reason: "claimed record has no proof".to_owned(),
                })?;
        self.publish(ProofStoreEvent::VerificationChanged {
            verification_id,
            state: VerificationState::Verifying,
        });
        Ok(Some(VerificationJob {
            verification_id,
            proof,
        }))
    }

    /// Commits a cryptographic verdict or an operational failure.
    ///
    /// # Errors
    ///
    /// Returns an error unless the record is `Verifying` or already has the same outcome.
    pub async fn finish_verification(
        &self,
        verification_id: VerificationId,
        outcome: VerificationOutcome,
    ) -> Result<StoreChange> {
        let next = match outcome {
            VerificationOutcome::Completed(verdict) => VerificationState::Completed(verdict),
            VerificationOutcome::Failed(failure) => VerificationState::Failed(failure),
        };
        let completed_at = matches!(&next, VerificationState::Completed(_)).then(Instant::now);
        let committed = next.clone();
        let computed = self
            .records
            .entry(verification_id)
            .and_try_compute_with(move |entry| async move {
                let entry = entry.ok_or_else(|| Error::InvalidVerificationTransition {
                    verification_id,
                    reason: "record does not exist".to_owned(),
                })?;
                let current = entry.value();
                if current.metadata.verification.as_ref() == Some(&committed) {
                    return Ok::<_, Error>(Op::Nop);
                }
                if current.metadata.verification != Some(VerificationState::Verifying) {
                    return Err(Error::InvalidVerificationTransition {
                        verification_id,
                        reason: format!(
                            "expected Verifying, found {:?}",
                            current.metadata.verification
                        ),
                    });
                }
                let mut updated = current.as_ref().clone();
                updated.metadata.verification = Some(committed);
                updated.metadata.completed_at = completed_at;
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let change = store_change(&computed);
        if change != StoreChange::Unchanged {
            self.publish(ProofStoreEvent::VerificationChanged {
                verification_id,
                state: next,
            });
        }
        Ok(change)
    }

    /// Requeues a retryable operational failure without changing proof content.
    ///
    /// # Errors
    ///
    /// Returns an error unless the record is in a retryable failed state.
    pub async fn retry_verification(&self, verification_id: VerificationId) -> Result<StoreChange> {
        let computed = self
            .records
            .entry(verification_id)
            .and_try_compute_with(move |entry| async move {
                let entry = entry.ok_or_else(|| Error::InvalidVerificationTransition {
                    verification_id,
                    reason: "record does not exist".to_owned(),
                })?;
                let current = entry.value();
                match current.metadata.verification.as_ref() {
                    Some(VerificationState::Failed(failure)) if failure.retryable() => {}
                    Some(VerificationState::Queued) => return Ok::<_, Error>(Op::Nop),
                    state => {
                        return Err(Error::InvalidVerificationTransition {
                            verification_id,
                            reason: format!("expected retryable Failed, found {state:?}"),
                        });
                    }
                }
                let mut updated = current.as_ref().clone();
                updated.metadata.verification = Some(VerificationState::Queued);
                updated.metadata.completed_at = None;
                Ok::<_, Error>(Op::Put(Arc::new(updated)))
            })
            .await?;

        let change = store_change(&computed);
        if change != StoreChange::Unchanged {
            self.publish(ProofStoreEvent::VerificationChanged {
                verification_id,
                state: VerificationState::Queued,
            });
        }
        Ok(change)
    }

    pub async fn statuses(
        &self,
        verification_ids: &[VerificationId],
    ) -> Vec<StoredVerificationStatus> {
        let mut statuses = Vec::with_capacity(verification_ids.len());
        for verification_id in verification_ids {
            let status = self.get(*verification_id).await.map_or(
                VerificationStatus::Unavailable,
                |record| match record.metadata.verification.as_ref() {
                    None => VerificationStatus::Unavailable,
                    Some(VerificationState::Queued) => VerificationStatus::Queued,
                    Some(VerificationState::Verifying) => VerificationStatus::Verifying,
                    Some(VerificationState::Completed(verdict)) => {
                        VerificationStatus::Completed(*verdict)
                    }
                    Some(VerificationState::Failed(failure)) => {
                        VerificationStatus::Failed(failure.clone())
                    }
                },
            );
            statuses.push(StoredVerificationStatus {
                verification_id: *verification_id,
                status,
            });
        }
        statuses
    }

    /// Returns only completed verdicts, preserving the requested ID order.
    pub async fn completed_results(
        &self,
        verification_ids: &[VerificationId],
    ) -> Vec<CompletedVerification> {
        let mut results = Vec::new();
        for verification_id in verification_ids {
            let Some(record) = self.get(*verification_id).await else {
                continue;
            };
            if let Some(VerificationState::Completed(verdict)) =
                record.metadata.verification.as_ref()
            {
                results.push(CompletedVerification {
                    verification_id: *verification_id,
                    verdict: *verdict,
                });
            }
        }
        results
    }

    pub async fn invalidate(&self, verification_id: VerificationId) {
        self.records.invalidate(&verification_id).await;
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
            record.metadata.chain_observed_at.is_some() && record.proof.is_none()
        })
    }

    #[must_use]
    pub fn records_ready_for_verification(&self) -> Vec<StoredProof> {
        self.snapshot(|record| {
            record.metadata.chain_observed_at.is_some()
                && record.proof.is_some()
                && record.metadata.verification == Some(VerificationState::Queued)
        })
    }

    #[must_use]
    pub fn locally_available_proofs(&self) -> Vec<StoredProof> {
        self.snapshot(|record| record.proof.is_some())
    }

    fn snapshot(&self, predicate: impl Fn(&ProofRecord) -> bool) -> Vec<StoredProof> {
        self.records
            .iter()
            .filter_map(|(verification_id, record)| {
                predicate(&record).then(|| StoredProof {
                    verification_id: *verification_id,
                    record,
                })
            })
            .collect()
    }

    fn validate_proof(&self, expected_id: VerificationId, proof: &Proof) -> Result<()> {
        let actual_bytes = proof.encoded_len();
        if actual_bytes > self.max_proof_bytes {
            return Err(Error::ProofTooLarge {
                verification_id: expected_id,
                actual_bytes,
                max_bytes: self.max_proof_bytes,
            });
        }
        if proof.verification_id() != expected_id {
            return Err(Error::VerificationIdMismatch(expected_id));
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

fn store_change(result: &CompResult<VerificationId, Arc<ProofRecord>>) -> StoreChange {
    match result {
        CompResult::Inserted(_) => StoreChange::Inserted,
        CompResult::ReplacedWith(_) => StoreChange::Updated,
        CompResult::Unchanged(_) | CompResult::StillNone(_) | CompResult::Removed(_) => {
            StoreChange::Unchanged
        }
    }
}

fn result_state(
    result: &CompResult<VerificationId, Arc<ProofRecord>>,
) -> Option<&VerificationState> {
    match result {
        CompResult::Inserted(entry)
        | CompResult::ReplacedWith(entry)
        | CompResult::Unchanged(entry) => entry.value().metadata.verification.as_ref(),
        CompResult::StillNone(_) | CompResult::Removed(_) => None,
    }
}

fn queue_if_ready(record: &mut ProofRecord) {
    if record.metadata.chain_observed_at.is_some()
        && record.proof.is_some()
        && record.metadata.verification.is_none()
    {
        record.metadata.verification = Some(VerificationState::Queued);
    }
}

fn proof_weight(record: &ProofRecord) -> u32 {
    let payload = record.proof.as_ref().map_or(0, |proof| {
        proof.payload_len().expect("proof size was validated")
    });
    u32::try_from(payload)
        .expect("validated proof size fits Moka weight")
        .checked_add(METADATA_WEIGHT_BYTES)
        .expect("validated proof weight fits Moka weight")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::proof::ProofType;

    fn config() -> ProofStoreConfig {
        ProofStoreConfig {
            max_capacity_bytes: 1024 * 1024,
            max_proof_bytes: 1024,
            terminal_retention: Duration::from_secs(60),
            event_buffer: 16,
        }
    }

    fn proof(bytes: impl Into<Bytes>) -> Proof {
        Proof {
            proof_type: ProofType::MinaPickles,
            proof: bytes.into(),
            public_inputs: Bytes::from_static(b"inputs"),
            verification_key: Bytes::from_static(b"key"),
        }
    }

    async fn stored_chain_proof(store: &ProofStore, bytes: &'static [u8]) -> VerificationId {
        let proof = proof(Bytes::from_static(bytes));
        let id = proof.verification_id();
        store.observe_chain_verification(id).await.unwrap();
        store
            .insert_local_proof(proof, ProofSource::Rpc)
            .await
            .unwrap();
        id
    }

    #[tokio::test]
    async fn merges_chain_first_metadata_with_content() {
        let store = ProofStore::new(config()).unwrap();
        let mut events = store.subscribe();
        let proof = proof(Bytes::from_static(b"proof"));
        let id = proof.verification_id();

        assert_eq!(
            store.observe_chain_verification(id).await.unwrap(),
            StoreChange::Inserted
        );
        assert!(store.get(id).await.unwrap().proof.is_none());
        assert!(matches!(
            events.recv().await.unwrap(),
            ProofStoreEvent::VerificationObserved { verification_id } if verification_id == id
        ));

        assert_eq!(
            store
                .attach_downloaded_proof(id, proof.clone(), PeerId::random())
                .await
                .unwrap(),
            StoreChange::Updated
        );
        assert_eq!(store.get_proof(id).await, Some(proof));
    }

    #[tokio::test]
    async fn merges_proof_first_without_duplicate_events() {
        let store = ProofStore::new(config()).unwrap();
        let mut events = store.subscribe();
        let proof = proof(Bytes::from_static(b"proof-first"));
        let id = proof.verification_id();

        assert_eq!(
            store
                .insert_local_proof(proof.clone(), ProofSource::Rpc)
                .await
                .unwrap(),
            (id, StoreChange::Inserted)
        );
        assert!(matches!(
            events.recv().await.unwrap(),
            ProofStoreEvent::ProofStored { verification_id, .. } if verification_id == id
        ));
        assert_eq!(
            store.observe_chain_verification(id).await.unwrap(),
            StoreChange::Updated
        );
        assert!(matches!(
            events.recv().await.unwrap(),
            ProofStoreEvent::VerificationObserved { verification_id } if verification_id == id
        ));
        assert!(matches!(
            events.recv().await.unwrap(),
            ProofStoreEvent::VerificationChanged {
                verification_id,
                state: VerificationState::Queued,
            } if verification_id == id
        ));
        assert_eq!(
            store.observe_chain_verification(id).await.unwrap(),
            StoreChange::Unchanged
        );
        assert!(
            timeout(Duration::from_millis(20), events.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_unobserved_mismatched_and_oversized_downloads() {
        let store = ProofStore::new(config()).unwrap();
        let valid = proof(Bytes::from_static(b"proof"));
        let id = valid.verification_id();
        assert!(matches!(
            store
                .attach_downloaded_proof(id, valid.clone(), PeerId::random())
                .await,
            Err(Error::ProofNotObserved(_))
        ));

        store.observe_chain_verification(id).await.unwrap();
        let different = proof(Bytes::from_static(b"different"));
        assert!(matches!(
            store
                .attach_downloaded_proof(id, different, PeerId::random())
                .await,
            Err(Error::VerificationIdMismatch(_))
        ));
        let oversized = proof(Bytes::from(vec![0; 1024]));
        let oversized_id = oversized.verification_id();
        assert!(matches!(
            store
                .insert_local_proof(oversized, ProofSource::Rpc)
                .await,
            Err(Error::ProofTooLarge { verification_id, .. }) if verification_id == oversized_id
        ));
        assert!(store.get(id).await.unwrap().proof.is_none());
    }

    #[tokio::test]
    async fn applies_limit_to_the_encoded_composite_proof() {
        let candidate = proof(Bytes::from_static(b"bounded"));
        let encoded_len = candidate.encoded_len();
        let mut exact = config();
        exact.max_proof_bytes = encoded_len;
        let store = ProofStore::new(exact).unwrap();
        assert!(
            store
                .insert_local_proof(candidate.clone(), ProofSource::Rpc)
                .await
                .is_ok()
        );

        let mut too_small = config();
        too_small.max_proof_bytes = encoded_len - 1;
        let store = ProofStore::new(too_small).unwrap();
        assert!(matches!(
            store
                .insert_local_proof(candidate, ProofSource::Rpc)
                .await,
            Err(Error::ProofTooLarge { actual_bytes, max_bytes, .. })
                if actual_bytes == encoded_len && max_bytes == encoded_len - 1
        ));
    }

    #[tokio::test]
    async fn verification_is_single_flight_and_status_order_is_stable() {
        let store = ProofStore::new(config()).unwrap();
        let verified = stored_chain_proof(&store, b"verified").await;
        let wrong = stored_chain_proof(&store, b"wrong").await;
        let missing = proof(Bytes::from_static(b"missing")).verification_id();

        let (first, second) = tokio::join!(
            store.begin_verification(verified),
            store.begin_verification(verified)
        );
        assert_eq!(
            usize::from(first.unwrap().is_some()) + usize::from(second.unwrap().is_some()),
            1
        );
        store
            .finish_verification(
                verified,
                VerificationOutcome::Completed(VerificationVerdict::Valid),
            )
            .await
            .unwrap();
        store.begin_verification(wrong).await.unwrap().unwrap();
        store
            .finish_verification(
                wrong,
                VerificationOutcome::Completed(VerificationVerdict::Invalid),
            )
            .await
            .unwrap();

        assert_eq!(
            store.statuses(&[missing, wrong, verified]).await,
            vec![
                StoredVerificationStatus {
                    verification_id: missing,
                    status: VerificationStatus::Unavailable,
                },
                StoredVerificationStatus {
                    verification_id: wrong,
                    status: VerificationStatus::Completed(VerificationVerdict::Invalid),
                },
                StoredVerificationStatus {
                    verification_id: verified,
                    status: VerificationStatus::Completed(VerificationVerdict::Valid),
                },
            ]
        );
        assert_eq!(
            store.completed_results(&[missing, wrong, verified]).await,
            vec![
                CompletedVerification {
                    verification_id: wrong,
                    verdict: VerificationVerdict::Invalid,
                },
                CompletedVerification {
                    verification_id: verified,
                    verdict: VerificationVerdict::Valid,
                },
            ]
        );
    }

    #[tokio::test]
    async fn operational_failure_is_distinct_and_only_retryable_failure_requeues() {
        let store = ProofStore::new(config()).unwrap();
        let retryable = stored_chain_proof(&store, b"retryable").await;
        store.begin_verification(retryable).await.unwrap().unwrap();
        let failure = VerificationFailure::new("backend_timeout", "timed out", true).unwrap();
        assert_eq!(
            store
                .finish_verification(retryable, VerificationOutcome::Failed(failure.clone()),)
                .await
                .unwrap(),
            StoreChange::Updated
        );
        assert!(
            store
                .get(retryable)
                .await
                .unwrap()
                .metadata
                .completed_at
                .is_none()
        );
        assert_eq!(
            store
                .finish_verification(retryable, VerificationOutcome::Failed(failure.clone()),)
                .await
                .unwrap(),
            StoreChange::Unchanged
        );
        assert_eq!(
            store.retry_verification(retryable).await.unwrap(),
            StoreChange::Updated
        );
        assert_eq!(
            store.statuses(&[retryable]).await[0].status,
            VerificationStatus::Queued
        );

        let permanent = stored_chain_proof(&store, b"permanent").await;
        store.begin_verification(permanent).await.unwrap().unwrap();
        store
            .finish_verification(
                permanent,
                VerificationOutcome::Failed(
                    VerificationFailure::new("unsupported_backend", "not configured", false)
                        .unwrap(),
                ),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.retry_verification(permanent).await,
            Err(Error::InvalidVerificationTransition { .. })
        ));
    }

    #[tokio::test]
    async fn failed_records_do_not_start_terminal_retention() {
        let mut config = config();
        config.terminal_retention = Duration::from_millis(50);
        let store = ProofStore::new(config).unwrap();
        let id = stored_chain_proof(&store, b"failed-retention").await;
        store.begin_verification(id).await.unwrap().unwrap();
        store
            .finish_verification(
                id,
                VerificationOutcome::Failed(
                    VerificationFailure::new("backend_crash", "worker exited", true).unwrap(),
                ),
            )
            .await
            .unwrap();

        sleep(Duration::from_millis(1_100)).await;
        store.run_pending_tasks().await;
        assert!(store.get(id).await.is_some());
    }

    #[tokio::test]
    async fn lagged_subscription_recovers_from_snapshot() {
        let mut config = config();
        config.event_buffer = 1;
        let store = ProofStore::new(config).unwrap();
        let mut events = store.subscribe();
        for bytes in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            store
                .insert_local_proof(proof(Bytes::copy_from_slice(bytes)), ProofSource::Rpc)
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
        let id = stored_chain_proof(&store, b"expiring").await;
        store.begin_verification(id).await.unwrap().unwrap();
        store
            .finish_verification(
                id,
                VerificationOutcome::Completed(VerificationVerdict::Valid),
            )
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;
        assert!(store.get(id).await.is_some());
        sleep(Duration::from_millis(1_100)).await;
        store.run_pending_tasks().await;
        assert!(store.get(id).await.is_none());

        let mut saw_expiry = false;
        while let Ok(Ok(event)) = timeout(Duration::from_millis(100), events.recv()).await {
            if matches!(
                event,
                ProofStoreEvent::ProofEvicted {
                    verification_id,
                    cause: ProofEvictionCause::Expired,
                } if verification_id == id
            ) {
                saw_expiry = true;
                break;
            }
        }
        assert!(saw_expiry);
    }

    #[tokio::test]
    async fn explicit_and_size_eviction_preserve_external_arc() {
        let store = ProofStore::new(config()).unwrap();
        let mut events = store.subscribe();
        let id = stored_chain_proof(&store, b"retained").await;
        let retained = store.get(id).await.unwrap();
        store.invalidate(id).await;
        assert_eq!(
            retained.proof.as_ref().unwrap().proof,
            b"retained".as_slice()
        );

        let mut explicit = 0;
        while let Ok(Ok(event)) = timeout(Duration::from_millis(20), events.recv()).await {
            if matches!(
                event,
                ProofStoreEvent::ProofEvicted {
                    verification_id,
                    cause: ProofEvictionCause::Explicit,
                } if verification_id == id
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
        let proof = proof(Bytes::from(vec![7; 300]));
        let size_id = proof.verification_id();
        sized
            .insert_local_proof(proof, ProofSource::Rpc)
            .await
            .unwrap();
        sized.run_pending_tasks().await;
        assert!(sized.get(size_id).await.is_none());
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
