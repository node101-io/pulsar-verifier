use std::{collections::HashSet, sync::Arc};

use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    store::{ProofStore, ProofStoreEvent, ProofStoreSubscription},
};

use super::{DriverClient, DriverEvent};

/// Processes network/store events while the driver remains the sole Swarm owner.
pub(super) struct Worker {
    driver: DriverClient,
    driver_events: mpsc::Receiver<DriverEvent>,
    store: Arc<ProofStore>,
    store_events: ProofStoreSubscription,
}

impl Worker {
    pub(super) fn new(
        driver: DriverClient,
        driver_events: mpsc::Receiver<DriverEvent>,
        store: Arc<ProofStore>,
    ) -> Self {
        let store_events = store.subscribe();
        Self {
            driver,
            driver_events,
            store,
            store_events,
        }
    }

    pub(super) async fn run(mut self, stop: CancellationToken) -> Result<()> {
        self.reconcile_local_proofs().await?;
        loop {
            tokio::select! {
                () = stop.cancelled() => {
                    tracing::info!("p2p worker drain started");
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
            }
        }
    }

    async fn drain(&mut self) -> Result<()> {
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

    async fn handle_driver_event(&self, event: DriverEvent, draining: bool) -> Result<()> {
        match event {
            DriverEvent::ProofRequested {
                request_id,
                proof_hash,
                ..
            } => {
                let content = self.store.get_content(proof_hash).await;
                self.driver.respond_proof(request_id, content).await?;
            }
            DriverEvent::ProofReceived { peer, content, .. } => {
                if let Err(error) = self
                    .store
                    .attach_downloaded_proof(content.proof_hash, content.proof, peer)
                    .await
                {
                    match error {
                        Error::ProofNotObserved(_)
                        | Error::ProofHashMismatch(_)
                        | Error::ProofTooLarge { .. } => {
                            tracing::debug!(%error, %peer, "downloaded proof was not stored");
                        }
                        error => return Err(error),
                    }
                }
            }
            DriverEvent::PeerConnected { .. } if !draining => {
                self.reconcile_local_proofs().await?;
            }
            event => tracing::debug!(?event, "P2P event"),
        }
        Ok(())
    }

    async fn handle_store_result(
        &self,
        event: std::result::Result<ProofStoreEvent, broadcast::error::RecvError>,
        draining: bool,
    ) -> Result<()> {
        match event {
            Ok(event) => self.handle_store_event(event, draining).await,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(
                    skipped,
                    "proof store subscriber lagged; reconciling local proofs"
                );
                if !draining {
                    self.reconcile_local_proofs().await?;
                }
                Ok(())
            }
            Err(broadcast::error::RecvError::Closed) => Err(Error::ProofStore(
                "proof store event channel closed".to_owned(),
            )),
        }
    }

    async fn handle_store_event(&self, event: ProofStoreEvent, draining: bool) -> Result<()> {
        if draining {
            tracing::debug!(?event, "proof store event drained during shutdown");
            return Ok(());
        }
        match event {
            ProofStoreEvent::ChainProofObserved { proof_hash } => {
                // TODO: Start provider discovery and proof retrieval after an
                // on-chain observation when local proof content is missing.
                tracing::debug!(?proof_hash, "chain proof observation received");
            }
            ProofStoreEvent::ProofStored { proof_hash, .. } => {
                self.driver.announce(proof_hash).await?;
            }
            ProofStoreEvent::ProofEvicted { proof_hash, .. } => {
                self.driver.forget_local_proof(proof_hash).await?;
            }
            event @ ProofStoreEvent::VerificationChanged { .. } => {
                tracing::debug!(?event, "proof store event");
            }
        }
        Ok(())
    }

    async fn reconcile_local_proofs(&self) -> Result<()> {
        let proofs = self.store.locally_available_proofs();
        let hashes = proofs
            .iter()
            .map(|proof| proof.hash)
            .collect::<HashSet<_>>();
        self.driver.replace_local_proofs(hashes).await?;
        for proof in proofs {
            self.driver.announce(proof.hash).await?;
        }
        Ok(())
    }
}
