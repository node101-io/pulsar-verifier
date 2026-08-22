use std::{
    collections::{HashSet, VecDeque},
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Instant,
};

use futures::FutureExt as _;
use tokio::{sync::broadcast, task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    config::VerificationConfig,
    proof::VerificationId,
    store::{
        ProofStore, ProofStoreEvent, ProofStoreSubscription, VerificationFailure, VerificationJob,
        VerificationOutcome, VerificationState,
    },
};

use super::VerifierRegistry;

const ATTEMPT_CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Subscribes to ready Store records and runs a bounded set of verifier jobs.
pub(crate) struct VerificationWorker {
    store: Arc<ProofStore>,
    store_events: ProofStoreSubscription,
    registry: VerifierRegistry,
    config: VerificationConfig,
    pending: VecDeque<VerificationId>,
    pending_ids: HashSet<VerificationId>,
    active_ids: HashSet<VerificationId>,
    jobs: JoinSet<JobCompletion>,
}

struct JobCompletion {
    job: VerificationJob,
    outcome: VerificationOutcome,
}

impl VerificationWorker {
    pub(crate) fn new(
        store: Arc<ProofStore>,
        registry: VerifierRegistry,
        config: VerificationConfig,
    ) -> Self {
        let store_events = store.subscribe();
        Self {
            store,
            store_events,
            registry,
            config,
            pending: VecDeque::new(),
            pending_ids: HashSet::new(),
            active_ids: HashSet::new(),
            jobs: JoinSet::new(),
        }
    }

    pub(crate) async fn run(mut self, stop: CancellationToken) -> Result<()> {
        self.reconcile_ready_records();

        loop {
            if stop.is_cancelled() {
                break;
            }
            self.start_ready_jobs(&stop).await?;
            tokio::select! {
                biased;
                () = stop.cancelled() => break,
                event = self.store_events.recv() => self.handle_store_result(&event)?,
                completion = self.jobs.join_next(), if !self.jobs.is_empty() => {
                    self.commit_completion(completion.expect("active job must complete")).await?;
                }
            }
        }

        // Pending records remain Queued; only already claimed jobs are drained.
        self.pending.clear();
        self.pending_ids.clear();
        while let Some(completion) = self.jobs.join_next().await {
            self.commit_completion(completion).await?;
        }
        Ok(())
    }

    async fn start_ready_jobs(&mut self, stop: &CancellationToken) -> Result<()> {
        while self.jobs.len() < self.config.max_concurrent_jobs {
            if stop.is_cancelled() {
                break;
            }
            let Some(verification_id) = self.pending.pop_front() else {
                break;
            };
            self.pending_ids.remove(&verification_id);
            if self.active_ids.contains(&verification_id) {
                continue;
            }
            let Some(job) = self.store.begin_verification(verification_id).await? else {
                continue;
            };
            let verifier = self.registry.get(job.proof.proof_type);
            let config = self.config;
            self.active_ids.insert(verification_id);
            self.jobs
                .spawn(async move { execute_job(job, verifier, config).await });
        }
        Ok(())
    }

    async fn commit_completion(
        &mut self,
        completion: std::result::Result<JobCompletion, tokio::task::JoinError>,
    ) -> Result<()> {
        let completion = completion.map_err(Error::Task)?;
        let verification_id = completion.job.verification_id;
        self.active_ids.remove(&verification_id);
        self.store
            .finish_verification(&completion.job, completion.outcome)
            .await?;
        if self
            .store
            .get(verification_id)
            .await
            .is_some_and(|record| record.metadata.verification == Some(VerificationState::Queued))
        {
            self.enqueue(verification_id);
        }
        Ok(())
    }

    fn handle_store_result(
        &mut self,
        event: &std::result::Result<ProofStoreEvent, broadcast::error::RecvError>,
    ) -> Result<()> {
        match event {
            Ok(ProofStoreEvent::VerificationChanged {
                verification_id,
                state: VerificationState::Queued,
            }) => self.enqueue(*verification_id),
            Ok(ProofStoreEvent::ProofEvicted {
                verification_id, ..
            }) => self.remove_pending(*verification_id),
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(
                    skipped,
                    "verification worker lagged; reconciling ready proofs"
                );
                self.reconcile_ready_records();
            }
            Err(broadcast::error::RecvError::Closed) => {
                return Err(Error::ProofStore(
                    "proof store event channel closed".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn reconcile_ready_records(&mut self) {
        self.pending.clear();
        self.pending_ids.clear();
        for proof in self.store.records_ready_for_verification() {
            self.enqueue(proof.verification_id);
        }
    }

    fn enqueue(&mut self, verification_id: VerificationId) {
        if !self.active_ids.contains(&verification_id) && self.pending_ids.insert(verification_id) {
            self.pending.push_back(verification_id);
        }
    }

    fn remove_pending(&mut self, verification_id: VerificationId) {
        if self.pending_ids.remove(&verification_id) {
            self.pending.retain(|pending| *pending != verification_id);
        }
    }
}

async fn execute_job(
    job: VerificationJob,
    verifier: Option<Arc<dyn super::Verifier>>,
    config: VerificationConfig,
) -> JobCompletion {
    let verification_id = job.verification_id;
    let proof_type = job.proof.proof_type;
    let Some(verifier) = verifier else {
        return JobCompletion {
            job,
            outcome: VerificationOutcome::Failed(failure(
                "unsupported_proof_type",
                "no verifier backend is registered for this proof type",
                false,
            )),
        };
    };

    for attempt in 0..=config.max_retries {
        let started = Instant::now();
        let outcome = execute_attempt(verifier.as_ref(), &job.proof, config.job_timeout).await;

        tracing::debug!(
            %verification_id,
            ?proof_type,
            attempt = attempt + 1,
            elapsed_ms = started.elapsed().as_millis(),
            "verification attempt completed"
        );

        let retryable = matches!(&outcome, VerificationOutcome::Failed(error) if error.retryable());
        if !retryable || attempt == config.max_retries {
            return JobCompletion { job, outcome };
        }

        let multiplier = 1_u32 << attempt;
        tokio::time::sleep(config.retry_backoff.saturating_mul(multiplier)).await;
    }

    unreachable!("bounded verification loop always returns")
}

async fn execute_attempt(
    verifier: &dyn super::Verifier,
    proof: &crate::proof::Proof,
    attempt_timeout: std::time::Duration,
) -> VerificationOutcome {
    let cancel = CancellationToken::new();
    let verification = AssertUnwindSafe(verifier.verify(proof, cancel.clone())).catch_unwind();
    tokio::pin!(verification);
    match timeout(attempt_timeout, &mut verification).await {
        Ok(Ok(Ok(verdict))) => VerificationOutcome::Completed(verdict),
        Ok(Ok(Err(error))) => VerificationOutcome::Failed(error),
        Ok(Err(_)) => VerificationOutcome::Failed(failure(
            "verifier_panicked",
            "verifier backend panicked",
            false,
        )),
        Err(_) => {
            cancel.cancel();
            let _ = timeout(ATTEMPT_CLEANUP_TIMEOUT, &mut verification).await;
            VerificationOutcome::Failed(failure(
                "verification_timeout",
                "verification attempt timed out",
                true,
            ))
        }
    }
}

fn failure(code: &'static str, message: &'static str, retryable: bool) -> VerificationFailure {
    VerificationFailure::new(code, message, retryable)
        .expect("static verification failure must satisfy wire bounds")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::sync::{Notify, Semaphore};

    use super::*;
    use crate::{
        config::ProofStoreConfig,
        proof::{Proof, ProofType},
        store::{ProofSource, VerificationStatus, VerificationVerdict},
        verification::Verifier,
    };

    fn store(event_buffer: usize) -> Arc<ProofStore> {
        Arc::new(
            ProofStore::new(ProofStoreConfig {
                max_capacity_bytes: 4 * 1024 * 1024,
                max_proof_bytes: 1024,
                terminal_retention: std::time::Duration::from_secs(60),
                event_buffer,
            })
            .unwrap(),
        )
    }

    fn config(max_concurrent_jobs: usize) -> VerificationConfig {
        VerificationConfig {
            max_concurrent_jobs,
            job_timeout: std::time::Duration::from_secs(30),
            max_retries: 2,
            retry_backoff: std::time::Duration::from_millis(250),
        }
    }

    fn proof(seed: u8) -> Proof {
        Proof {
            proof_type: ProofType::NoirBarretenberg,
            proof: Bytes::from(vec![seed]),
            public_inputs: Bytes::from_static(b"inputs"),
            verification_key: Bytes::from_static(b"key"),
        }
    }

    async fn insert_ready(store: &ProofStore, proof: Proof) -> VerificationId {
        let id = proof.verification_id();
        store.observe_chain_verification(id).await.unwrap();
        store
            .insert_local_proof(proof, ProofSource::Rpc)
            .await
            .unwrap();
        id
    }

    struct TrackingVerifier {
        active: AtomicUsize,
        maximum: AtomicUsize,
        calls: AtomicUsize,
        started: Notify,
        release: Semaphore,
    }

    impl TrackingVerifier {
        fn new() -> Self {
            Self {
                active: AtomicUsize::new(0),
                maximum: AtomicUsize::new(0),
                calls: AtomicUsize::new(0),
                started: Notify::new(),
                release: Semaphore::new(0),
            }
        }
    }

    #[async_trait]
    impl Verifier for TrackingVerifier {
        async fn verify(
            &self,
            _proof: &Proof,
            _cancel: CancellationToken,
        ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            self.started.notify_waiters();
            self.release.acquire().await.unwrap().forget();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(VerificationVerdict::Valid)
        }
    }

    #[tokio::test]
    async fn processes_full_block_with_exact_concurrency_bound() {
        let store = store(8);
        let verifier = Arc::new(TrackingVerifier::new());
        let registry = VerifierRegistry::new([(
            ProofType::NoirBarretenberg,
            Arc::clone(&verifier) as Arc<dyn Verifier>,
        )])
        .unwrap();
        let worker = VerificationWorker::new(Arc::clone(&store), registry, config(2));
        let stop = CancellationToken::new();

        let ids = futures::future::join_all((0..=u8::MAX).map(|seed| {
            let store = Arc::clone(&store);
            async move { insert_ready(&store, proof(seed)).await }
        }))
        .await;
        let task = tokio::spawn(worker.run(stop.clone()));

        timeout(std::time::Duration::from_secs(2), async {
            while verifier.active.load(Ordering::SeqCst) != 2 {
                verifier.started.notified().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(verifier.maximum.load(Ordering::SeqCst), 2);

        verifier.release.add_permits(ids.len());
        timeout(std::time::Duration::from_secs(5), async {
            loop {
                let statuses = store.statuses(&ids).await;
                if statuses.iter().all(|status| {
                    status.status == VerificationStatus::Completed(VerificationVerdict::Valid)
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(verifier.calls.load(Ordering::SeqCst), ids.len());
        assert_eq!(verifier.maximum.load(Ordering::SeqCst), 2);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    struct InvalidVerifier;

    #[async_trait]
    impl Verifier for InvalidVerifier {
        async fn verify(
            &self,
            _proof: &Proof,
            _cancel: CancellationToken,
        ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
            Ok(VerificationVerdict::Invalid)
        }
    }

    #[tokio::test]
    async fn commits_invalid_as_a_cryptographic_verdict() {
        let store = store(16);
        let registry = VerifierRegistry::new([(
            ProofType::NoirBarretenberg,
            Arc::new(InvalidVerifier) as Arc<dyn Verifier>,
        )])
        .unwrap();
        let worker = VerificationWorker::new(Arc::clone(&store), registry, config(1));
        let id = insert_ready(&store, proof(1)).await;
        let stop = CancellationToken::new();
        let task = tokio::spawn(worker.run(stop.clone()));

        wait_for_status(
            &store,
            id,
            VerificationStatus::Completed(VerificationVerdict::Invalid),
        )
        .await;
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn missing_backend_is_failed_never_invalid() {
        let store = store(16);
        let worker =
            VerificationWorker::new(Arc::clone(&store), VerifierRegistry::default(), config(1));
        let id = insert_ready(&store, proof(2)).await;
        let stop = CancellationToken::new();
        let task = tokio::spawn(worker.run(stop.clone()));

        timeout(std::time::Duration::from_secs(2), async {
            loop {
                let status = store.statuses(&[id]).await.pop().unwrap().status;
                if let VerificationStatus::Failed(failure) = status {
                    assert_eq!(failure.code(), "unsupported_proof_type");
                    assert!(!failure.retryable());
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

    struct FlakyVerifier {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Verifier for FlakyVerifier {
        async fn verify(
            &self,
            _proof: &Proof,
            _cancel: CancellationToken,
        ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
            if self.calls.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(failure("backend_busy", "backend is busy", true))
            } else {
                Ok(VerificationVerdict::Valid)
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_retryable_failures_with_exponential_backoff() {
        let verifier = Arc::new(FlakyVerifier {
            calls: AtomicUsize::new(0),
        });
        let proof = proof(3);
        let job = VerificationJob::detached(proof.verification_id(), proof);
        let task = tokio::spawn(execute_job(
            job,
            Some(Arc::clone(&verifier) as Arc<dyn Verifier>),
            config(1),
        ));

        tokio::time::advance(std::time::Duration::from_millis(750)).await;
        let completion = task.await.unwrap();
        assert_eq!(verifier.calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            completion.outcome,
            VerificationOutcome::Completed(VerificationVerdict::Valid)
        );
    }

    struct AlwaysFailVerifier {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Verifier for AlwaysFailVerifier {
        async fn verify(
            &self,
            _proof: &Proof,
            _cancel: CancellationToken,
        ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(failure("backend_busy", "backend is busy", true))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stores_last_failure_after_retry_budget_is_exhausted() {
        let verifier = Arc::new(AlwaysFailVerifier {
            calls: AtomicUsize::new(0),
        });
        let proof = proof(8);
        let job = VerificationJob::detached(proof.verification_id(), proof);
        let completion = execute_job(
            job,
            Some(Arc::clone(&verifier) as Arc<dyn Verifier>),
            config(1),
        )
        .await;

        assert_eq!(verifier.calls.load(Ordering::SeqCst), 3);
        let VerificationOutcome::Failed(failure) = completion.outcome else {
            panic!("exhausted retry budget must preserve the operational failure")
        };
        assert_eq!(failure.code(), "backend_busy");
        assert!(failure.retryable());
    }

    struct PendingVerifier {
        calls: AtomicUsize,
        cancellations: AtomicUsize,
    }

    #[async_trait]
    impl Verifier for PendingVerifier {
        async fn verify(
            &self,
            _proof: &Proof,
            cancel: CancellationToken,
        ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            cancel.cancelled().await;
            self.cancellations.fetch_add(1, Ordering::SeqCst);
            Err(failure("backend_cancelled", "backend cancelled", true))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_timeouts_and_reports_a_bounded_failure() {
        let verifier = Arc::new(PendingVerifier {
            calls: AtomicUsize::new(0),
            cancellations: AtomicUsize::new(0),
        });
        let proof = proof(9);
        let completion = execute_job(
            VerificationJob::detached(proof.verification_id(), proof),
            Some(Arc::clone(&verifier) as Arc<dyn Verifier>),
            VerificationConfig {
                max_concurrent_jobs: 1,
                job_timeout: std::time::Duration::from_secs(1),
                max_retries: 2,
                retry_backoff: std::time::Duration::from_millis(250),
            },
        )
        .await;

        assert_eq!(verifier.calls.load(Ordering::SeqCst), 3);
        assert_eq!(verifier.cancellations.load(Ordering::SeqCst), 3);
        let VerificationOutcome::Failed(failure) = completion.outcome else {
            panic!("timeout must be an operational failure")
        };
        assert_eq!(failure.code(), "verification_timeout");
        assert!(failure.retryable());
    }

    struct PanicVerifier;

    #[async_trait]
    impl Verifier for PanicVerifier {
        async fn verify(
            &self,
            _proof: &Proof,
            _cancel: CancellationToken,
        ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
            panic!("test verifier panic")
        }
    }

    #[tokio::test]
    async fn catches_backend_panic_as_non_retryable_failure() {
        let proof = proof(4);
        let completion = execute_job(
            VerificationJob::detached(proof.verification_id(), proof),
            Some(Arc::new(PanicVerifier)),
            config(1),
        )
        .await;

        let VerificationOutcome::Failed(failure) = completion.outcome else {
            panic!("panic must be an operational failure")
        };
        assert_eq!(failure.code(), "verifier_panicked");
        assert!(!failure.retryable());
    }

    #[tokio::test]
    async fn shutdown_drains_active_jobs_without_starting_pending_work() {
        let store = store(16);
        let verifier = Arc::new(TrackingVerifier::new());
        let registry = VerifierRegistry::new([(
            ProofType::NoirBarretenberg,
            Arc::clone(&verifier) as Arc<dyn Verifier>,
        )])
        .unwrap();
        let worker = VerificationWorker::new(Arc::clone(&store), registry, config(2));
        let ids = futures::future::join_all((10..14).map(|seed| {
            let store = Arc::clone(&store);
            async move { insert_ready(&store, proof(seed)).await }
        }))
        .await;
        let stop = CancellationToken::new();
        let task = tokio::spawn(worker.run(stop.clone()));

        timeout(std::time::Duration::from_secs(2), async {
            while verifier.active.load(Ordering::SeqCst) != 2 {
                verifier.started.notified().await;
            }
        })
        .await
        .unwrap();
        stop.cancel();
        verifier.release.add_permits(2);
        task.await.unwrap().unwrap();

        assert_eq!(verifier.calls.load(Ordering::SeqCst), 2);
        let statuses = store.statuses(&ids).await;
        assert_eq!(
            statuses
                .iter()
                .filter(|status| status.status == VerificationStatus::Queued)
                .count(),
            2
        );
        assert_eq!(
            statuses
                .iter()
                .filter(|status| {
                    status.status == VerificationStatus::Completed(VerificationVerdict::Valid)
                })
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn eviction_and_reinsertion_do_not_run_the_same_id_concurrently() {
        let store = store(32);
        let verifier = Arc::new(TrackingVerifier::new());
        let registry = VerifierRegistry::new([(
            ProofType::NoirBarretenberg,
            Arc::clone(&verifier) as Arc<dyn Verifier>,
        )])
        .unwrap();
        let candidate = proof(42);
        let id = insert_ready(&store, candidate.clone()).await;
        let worker = VerificationWorker::new(Arc::clone(&store), registry, config(2));
        let stop = CancellationToken::new();
        let task = tokio::spawn(worker.run(stop.clone()));

        timeout(std::time::Duration::from_secs(2), async {
            while verifier.calls.load(Ordering::SeqCst) != 1 {
                verifier.started.notified().await;
            }
        })
        .await
        .unwrap();

        store.invalidate(id).await;
        store.run_pending_tasks().await;
        store.observe_chain_verification(id).await.unwrap();
        store
            .insert_local_proof(candidate, ProofSource::Rpc)
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(verifier.calls.load(Ordering::SeqCst), 1);

        verifier.release.add_permits(1);
        timeout(std::time::Duration::from_secs(2), async {
            while verifier.calls.load(Ordering::SeqCst) != 2 {
                verifier.started.notified().await;
            }
        })
        .await
        .unwrap();
        verifier.release.add_permits(1);
        wait_for_status(
            &store,
            id,
            VerificationStatus::Completed(VerificationVerdict::Valid),
        )
        .await;

        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn eviction_removes_pending_work_and_reconciliation_rebuilds_the_queue() {
        let store = store(16);
        let mut worker =
            VerificationWorker::new(Arc::clone(&store), VerifierRegistry::default(), config(1));
        let stale = proof(50).verification_id();
        worker.enqueue(stale);
        worker
            .handle_store_result(&Ok(ProofStoreEvent::ProofEvicted {
                verification_id: stale,
                cause: crate::store::ProofEvictionCause::Size,
            }))
            .unwrap();
        assert!(worker.pending.is_empty());
        assert!(worker.pending_ids.is_empty());

        worker.enqueue(stale);
        let ready = insert_ready(&store, proof(51)).await;
        worker.reconcile_ready_records();
        assert_eq!(worker.pending.into_iter().collect::<Vec<_>>(), vec![ready]);
        assert_eq!(worker.pending_ids, HashSet::from([ready]));
    }

    async fn wait_for_status(store: &ProofStore, id: VerificationId, expected: VerificationStatus) {
        timeout(std::time::Duration::from_secs(2), async {
            loop {
                if store.statuses(&[id]).await[0].status == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
