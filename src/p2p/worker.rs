use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use futures::{FutureExt as _, StreamExt as _, future::BoxFuture, stream::FuturesUnordered};
use libp2p::PeerId;
use rand::seq::SliceRandom as _;
use tokio::{
    sync::{broadcast, mpsc},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    config::P2pConfig,
    proof::VerificationId,
    store::{ProofStore, ProofStoreEvent, ProofStoreSubscription},
};

use super::{DriverClient, DriverEvent, ProofRequestId, QueryId};

struct RetrievalState {
    deadline: Instant,
    attempted: HashSet<PeerId>,
    active: Option<(ProofRequestId, PeerId)>,
    query: Option<QueryId>,
    retry_round: u32,
    timer_generation: u64,
    queued: bool,
    finished: bool,
}

struct RetrievalWake {
    verification_id: VerificationId,
    generation: u64,
}

/// Processes network/store events while the driver remains the sole Swarm owner.
pub(super) struct Worker {
    driver: DriverClient,
    driver_events: mpsc::Receiver<DriverEvent>,
    store: Arc<ProofStore>,
    store_events: ProofStoreSubscription,
    local_peer_id: PeerId,
    max_concurrent_retrievals: usize,
    retrieval_timeout: Duration,
    retrieval_initial_backoff: Duration,
    retrieval_max_backoff: Duration,
    retrievals: HashMap<VerificationId, RetrievalState>,
    pending: VecDeque<VerificationId>,
    active_retrievals: usize,
    wakes: FuturesUnordered<BoxFuture<'static, RetrievalWake>>,
}

impl Worker {
    pub(super) fn new(
        driver: DriverClient,
        driver_events: mpsc::Receiver<DriverEvent>,
        store: Arc<ProofStore>,
        local_peer_id: PeerId,
        config: &P2pConfig,
    ) -> Self {
        let store_events = store.subscribe();
        Self {
            driver,
            driver_events,
            store,
            store_events,
            local_peer_id,
            max_concurrent_retrievals: config.max_concurrent_retrievals,
            retrieval_timeout: config.retrieval_timeout,
            retrieval_initial_backoff: config.retrieval_initial_backoff,
            retrieval_max_backoff: config.retrieval_max_backoff,
            retrievals: HashMap::new(),
            pending: VecDeque::new(),
            active_retrievals: 0,
            wakes: FuturesUnordered::new(),
        }
    }

    pub(super) async fn run(mut self, stop: CancellationToken) -> Result<()> {
        self.reconcile_store().await?;
        loop {
            tokio::select! {
                () = stop.cancelled() => {
                    tracing::info!(
                        active = self.active_retrievals,
                        pending = self.retrievals.len(),
                        "p2p worker drain started"
                    );
                    self.drain().await?;
                    tracing::info!("p2p worker drained");
                    return Ok(());
                }
                event = self.driver_events.recv() => {
                    let event = event.ok_or(Error::P2pDriverClosed)?;
                    self.handle_driver_event(event, false).await?;
                }
                event = self.store_events.recv() => {
                    self.handle_store_result(event, false).await?;
                }
                Some(wake) = self.wakes.next(), if !self.wakes.is_empty() => {
                    self.handle_wake(wake).await?;
                }
            }
        }
    }

    async fn drain(&mut self) -> Result<()> {
        self.pending.clear();
        self.wakes.clear();
        loop {
            let mut progressed = false;
            loop {
                match self.driver_events.try_recv() {
                    Ok(event) => {
                        progressed = true;
                        self.handle_driver_event(event, true).await?;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        return Err(Error::P2pDriverClosed);
                    }
                }
            }
            loop {
                match self.store_events.try_recv() {
                    Ok(event) => {
                        progressed = true;
                        self.handle_store_event(event, true).await?;
                    }
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                        progressed = true;
                        tracing::warn!(
                            skipped,
                            "proof store subscriber lagged during shutdown drain"
                        );
                    }
                    Err(broadcast::error::TryRecvError::Closed) => {
                        return Err(Error::ProofStore(
                            "proof store event channel closed".to_owned(),
                        ));
                    }
                }
            }
            if !progressed {
                return Ok(());
            }
        }
    }

    async fn handle_driver_event(&mut self, event: DriverEvent, draining: bool) -> Result<()> {
        match event {
            DriverEvent::ProofRequested {
                request_id,
                verification_id,
                ..
            } => {
                let proof = self.store.get_proof(verification_id).await;
                self.driver.respond_proof(request_id, proof).await?;
            }
            DriverEvent::ProofReceived {
                request_id,
                peer,
                verification_id,
                proof,
            } => {
                let stored = self
                    .store
                    .attach_downloaded_proof(verification_id, proof, peer)
                    .await;
                match stored {
                    Ok(_) => self.finish_request(verification_id, request_id, peer, true),
                    Err(
                        error @ (Error::VerificationIdMismatch(_) | Error::ProofTooLarge { .. }),
                    ) => {
                        tracing::debug!(%error, %peer, "downloaded proof was not stored");
                        self.finish_request(verification_id, request_id, peer, false);
                    }
                    Err(error @ Error::ProofNotObserved(_)) => {
                        tracing::debug!(%error, %peer, "downloaded proof no longer has an active chain record");
                        self.finish_request(verification_id, request_id, peer, true);
                        self.stop_retrieval(verification_id);
                    }
                    Err(error) => return Err(error),
                }
                if !draining {
                    self.drive_pending().await?;
                }
            }
            DriverEvent::ProofNotFound {
                request_id,
                peer,
                verification_id,
            }
            | DriverEvent::ProofRequestFailed {
                request_id,
                peer,
                verification_id,
                ..
            } => {
                self.finish_request(verification_id, request_id, peer, false);
                if !draining {
                    self.drive_pending().await?;
                }
            }
            DriverEvent::AvailabilityAnnounced {
                peer,
                verification_id,
            } if !draining => {
                let attempted = self
                    .retrievals
                    .get(&verification_id)
                    .is_some_and(|state| state.attempted.contains(&peer));
                if !attempted {
                    self.ensure_retrieval(verification_id).await?;
                }
            }
            DriverEvent::ProvidersDiscovered {
                query_id,
                verification_id,
                ..
            } if !draining => {
                let matches = self
                    .retrievals
                    .get(&verification_id)
                    .is_some_and(|state| state.query == Some(query_id));
                if matches {
                    self.clear_query(verification_id);
                    self.enqueue(verification_id);
                    self.drive_pending().await?;
                }
            }
            DriverEvent::PeerConnected { .. } if !draining => {
                self.reconcile_store().await?;
            }
            event => tracing::debug!(?event, "P2P event"),
        }
        Ok(())
    }

    async fn handle_store_result(
        &mut self,
        event: std::result::Result<ProofStoreEvent, broadcast::error::RecvError>,
        draining: bool,
    ) -> Result<()> {
        match event {
            Ok(event) => self.handle_store_event(event, draining).await,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(
                    skipped,
                    "proof store subscriber lagged; reconciling P2P state"
                );
                if !draining {
                    self.reconcile_store().await?;
                }
                Ok(())
            }
            Err(broadcast::error::RecvError::Closed) => Err(Error::ProofStore(
                "proof store event channel closed".to_owned(),
            )),
        }
    }

    async fn handle_store_event(&mut self, event: ProofStoreEvent, draining: bool) -> Result<()> {
        if draining {
            tracing::debug!(?event, "proof store event drained during shutdown");
            return Ok(());
        }
        match event {
            ProofStoreEvent::VerificationObserved { verification_id } => {
                self.ensure_retrieval(verification_id).await?;
            }
            ProofStoreEvent::ProofStored {
                verification_id, ..
            } => {
                self.stop_retrieval(verification_id);
                self.driver.announce(verification_id).await?;
                self.drive_pending().await?;
            }
            ProofStoreEvent::ProofEvicted {
                verification_id, ..
            } => {
                self.stop_retrieval(verification_id);
                self.driver.forget_local_proof(verification_id).await?;
                self.drive_pending().await?;
            }
            ProofStoreEvent::VerificationChanged {
                verification_id, ..
            } => {
                self.stop_retrieval(verification_id);
                self.drive_pending().await?;
            }
        }
        Ok(())
    }

    async fn ensure_retrieval(&mut self, verification_id: VerificationId) -> Result<()> {
        let waiting = self.store.get(verification_id).await.is_some_and(|record| {
            record.metadata.chain_observed_at.is_some() && record.proof.is_none()
        });
        if !waiting {
            self.stop_retrieval(verification_id);
            return Ok(());
        }
        self.retrievals
            .entry(verification_id)
            .or_insert_with(|| RetrievalState {
                deadline: Instant::now() + self.retrieval_timeout,
                attempted: HashSet::new(),
                active: None,
                query: None,
                retry_round: 0,
                timer_generation: 0,
                queued: false,
                finished: false,
            });
        self.enqueue(verification_id);
        self.drive_pending().await
    }

    fn enqueue(&mut self, verification_id: VerificationId) {
        let Some(state) = self.retrievals.get_mut(&verification_id) else {
            return;
        };
        if state.queued || state.active.is_some() || state.finished {
            return;
        }
        state.queued = true;
        self.pending.push_back(verification_id);
    }

    async fn drive_pending(&mut self) -> Result<()> {
        while self.active_retrievals < self.max_concurrent_retrievals {
            let Some(verification_id) = self.pending.pop_front() else {
                return Ok(());
            };
            let Some(state) = self.retrievals.get_mut(&verification_id) else {
                continue;
            };
            state.queued = false;
            if state.finished || state.active.is_some() {
                continue;
            }
            if Instant::now() >= state.deadline {
                self.stop_retrieval(verification_id);
                continue;
            }
            self.try_retrieve(verification_id).await?;
        }
        Ok(())
    }

    async fn try_retrieve(&mut self, verification_id: VerificationId) -> Result<()> {
        loop {
            let attempted = self
                .retrievals
                .get(&verification_id)
                .map(|state| state.attempted.clone())
                .unwrap_or_default();
            let mut providers = self.driver.providers(verification_id).await?;
            providers.retain(|peer| *peer != self.local_peer_id && !attempted.contains(peer));
            providers.shuffle(&mut rand::rng());

            if let Some(peer) = providers.pop() {
                if self.active_retrievals >= self.max_concurrent_retrievals {
                    self.enqueue(verification_id);
                    return Ok(());
                }
                match self.driver.request_proof(peer, verification_id).await {
                    Ok(request_id) => {
                        let Some(state) = self.retrievals.get_mut(&verification_id) else {
                            return Ok(());
                        };
                        state.query = None;
                        state.timer_generation = state.timer_generation.wrapping_add(1);
                        state.active = Some((request_id, peer));
                        self.active_retrievals += 1;
                        tracing::debug!(%verification_id, %peer, "proof retrieval started");
                        return Ok(());
                    }
                    Err(Error::P2pAuthorization(_)) => {
                        if let Some(state) = self.retrievals.get_mut(&verification_id) {
                            state.attempted.insert(peer);
                        }
                    }
                    Err(Error::P2pDraining) => {
                        self.stop_retrieval(verification_id);
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
                continue;
            }

            if self
                .retrievals
                .get(&verification_id)
                .is_some_and(|state| state.query.is_some())
            {
                return Ok(());
            }
            match self.driver.query_availability(verification_id).await {
                Ok(query_id) => {
                    if let Some(state) = self.retrievals.get_mut(&verification_id) {
                        state.query = Some(query_id);
                    }
                    self.schedule_retry(verification_id);
                }
                Err(Error::P2pDraining) => self.stop_retrieval(verification_id),
                Err(error) => return Err(error),
            }
            return Ok(());
        }
    }

    fn schedule_retry(&mut self, verification_id: VerificationId) {
        let Some(state) = self.retrievals.get_mut(&verification_id) else {
            return;
        };
        let cap = retry_cap(
            self.retrieval_initial_backoff,
            self.retrieval_max_backoff,
            state.retry_round,
        );
        let remaining = state.deadline.saturating_duration_since(Instant::now());
        let delay = full_jitter(cap, remaining);
        state.retry_round = state.retry_round.saturating_add(1);
        state.timer_generation = state.timer_generation.wrapping_add(1);
        let generation = state.timer_generation;
        self.wakes.push(
            async move {
                tokio::time::sleep(delay).await;
                RetrievalWake {
                    verification_id,
                    generation,
                }
            }
            .boxed(),
        );
    }

    async fn handle_wake(&mut self, wake: RetrievalWake) -> Result<()> {
        let Some(state) = self.retrievals.get_mut(&wake.verification_id) else {
            return Ok(());
        };
        if state.timer_generation != wake.generation || state.finished {
            return Ok(());
        }
        state.query = None;
        if Instant::now() >= state.deadline {
            self.stop_retrieval(wake.verification_id);
            return Ok(());
        }
        self.enqueue(wake.verification_id);
        self.drive_pending().await
    }

    fn clear_query(&mut self, verification_id: VerificationId) {
        if let Some(state) = self.retrievals.get_mut(&verification_id) {
            state.query = None;
            state.timer_generation = state.timer_generation.wrapping_add(1);
        }
    }

    fn finish_request(
        &mut self,
        verification_id: VerificationId,
        request_id: ProofRequestId,
        peer: PeerId,
        succeeded: bool,
    ) {
        let Some(state) = self.retrievals.get_mut(&verification_id) else {
            return;
        };
        if state.active != Some((request_id, peer)) {
            return;
        }
        state.active = None;
        self.active_retrievals = self
            .active_retrievals
            .checked_sub(1)
            .expect("tracked proof request owns one retrieval slot");
        if succeeded || state.finished || Instant::now() >= state.deadline {
            self.retrievals.remove(&verification_id);
        } else {
            state.attempted.insert(peer);
            self.enqueue(verification_id);
        }
    }

    fn stop_retrieval(&mut self, verification_id: VerificationId) {
        let Some(state) = self.retrievals.get_mut(&verification_id) else {
            return;
        };
        state.finished = true;
        state.queued = false;
        state.query = None;
        state.timer_generation = state.timer_generation.wrapping_add(1);
        if state.active.is_none() {
            self.retrievals.remove(&verification_id);
        }
    }

    async fn reconcile_store(&mut self) -> Result<()> {
        let local = self.store.locally_available_proofs();
        let local_ids = local
            .iter()
            .map(|proof| proof.verification_id)
            .collect::<HashSet<_>>();
        self.driver.replace_local_proofs(local_ids).await?;
        for proof in local {
            self.driver.announce(proof.verification_id).await?;
        }

        let waiting = self.store.records_waiting_for_content();
        let waiting_ids = waiting
            .iter()
            .map(|proof| proof.verification_id)
            .collect::<HashSet<_>>();
        for verification_id in self.retrievals.keys().copied().collect::<Vec<_>>() {
            if !waiting_ids.contains(&verification_id) {
                self.stop_retrieval(verification_id);
            }
        }
        for proof in waiting {
            self.ensure_retrieval(proof.verification_id).await?;
        }
        Ok(())
    }
}

fn retry_cap(initial: Duration, maximum: Duration, round: u32) -> Duration {
    let multiplier = 1_u32.checked_shl(round.min(31)).unwrap_or(u32::MAX);
    initial.saturating_mul(multiplier).min(maximum)
}

fn full_jitter(cap: Duration, remaining: Duration) -> Duration {
    let cap_nanos = u64::try_from(cap.min(remaining).as_nanos()).unwrap_or(u64::MAX);
    Duration::from_nanos(rand::random_range(0..=cap_nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_doubles_until_the_configured_cap() {
        let initial = Duration::from_millis(250);
        let maximum = Duration::from_secs(2);

        assert_eq!(retry_cap(initial, maximum, 0), Duration::from_millis(250));
        assert_eq!(retry_cap(initial, maximum, 1), Duration::from_millis(500));
        assert_eq!(retry_cap(initial, maximum, 2), Duration::from_secs(1));
        assert_eq!(retry_cap(initial, maximum, 3), Duration::from_secs(2));
        assert_eq!(retry_cap(initial, maximum, 30), Duration::from_secs(2));
    }

    #[test]
    fn full_jitter_never_exceeds_backoff_or_remaining_deadline() {
        for _ in 0..100 {
            assert!(
                full_jitter(Duration::from_secs(2), Duration::from_millis(300))
                    <= Duration::from_millis(300)
            );
        }
        assert_eq!(
            full_jitter(Duration::from_secs(2), Duration::ZERO),
            Duration::ZERO
        );
    }
}
