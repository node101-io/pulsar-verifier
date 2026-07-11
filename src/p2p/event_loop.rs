use std::{collections::HashSet, sync::Arc};

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    store::{ProofStore, ProofStoreEvent, ProofStoreSubscription},
};

use super::{P2pEvent, P2pHandle};

type CommandReply = oneshot::Sender<Result<()>>;

enum EventLoopCommand {
    Shutdown { reply: CommandReply },
}

/// Lifecycle facade for the task that joins P2P events with the proof store.
#[derive(Clone)]
pub(crate) struct P2pEventLoopHandle {
    commands: mpsc::Sender<EventLoopCommand>,
}

impl P2pEventLoopHandle {
    pub(crate) async fn shutdown(&self) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(EventLoopCommand::Shutdown { reply })
            .await
            .map_err(|_| Error::P2pEventLoopClosed)?;
        response.await.map_err(|_| Error::P2pEventLoopClosed)?
    }
}

/// Processes network/store events while the driver remains the sole Swarm owner.
pub(crate) struct P2pEventLoop {
    handle: P2pHandle,
    p2p_events: mpsc::Receiver<P2pEvent>,
    store: Arc<ProofStore>,
    store_events: ProofStoreSubscription,
    commands: mpsc::Receiver<EventLoopCommand>,
}

impl P2pEventLoop {
    pub(crate) fn new(
        handle: P2pHandle,
        p2p_events: mpsc::Receiver<P2pEvent>,
        store: Arc<ProofStore>,
        event_buffer: usize,
    ) -> (Self, P2pEventLoopHandle) {
        let store_events = store.subscribe();
        let (command_tx, commands) = mpsc::channel(event_buffer.max(1));
        (
            Self {
                handle,
                p2p_events,
                store,
                store_events,
                commands,
            },
            P2pEventLoopHandle {
                commands: command_tx,
            },
        )
    }

    pub(crate) async fn run(mut self, force_cancel: CancellationToken) -> Result<()> {
        self.reconcile_local_proofs().await?;
        loop {
            tokio::select! {
                () = force_cancel.cancelled() => return Ok(()),
                command = self.commands.recv() => {
                    let command = command.ok_or(Error::P2pEventLoopClosed)?;
                    match command {
                        EventLoopCommand::Shutdown { reply } => {
                            let result = self.drain().await;
                            let should_stop = result.is_ok();
                            let _ = reply.send(result);
                            if should_stop {
                                tracing::info!("p2p event loop drained");
                                return Ok(());
                            }
                        }
                    }
                }
                event = self.p2p_events.recv() => {
                    let event = event.ok_or(Error::P2pDriverClosed)?;
                    self.handle_p2p_event(event, false).await?;
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
                match self.p2p_events.try_recv() {
                    Ok(event) => {
                        progressed = true;
                        self.handle_p2p_event(event, true).await?;
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

    async fn handle_p2p_event(&self, event: P2pEvent, draining: bool) -> Result<()> {
        match event {
            P2pEvent::ProofRequested {
                request_id,
                proof_hash,
                ..
            } => {
                let content = self.store.get_content(proof_hash).await;
                self.handle.respond_proof(request_id, content).await?;
            }
            P2pEvent::ProofReceived { peer, content, .. } => {
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
            P2pEvent::PeerConnected { .. } if !draining => {
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
            ProofStoreEvent::ProofStored { proof_hash, .. } => {
                self.handle.announce(proof_hash).await?;
            }
            ProofStoreEvent::ProofEvicted { proof_hash, .. } => {
                self.handle.forget_local_proof(proof_hash).await?;
            }
            event => tracing::debug!(?event, "proof store event"),
        }
        Ok(())
    }

    async fn reconcile_local_proofs(&self) -> Result<()> {
        let proofs = self.store.locally_available_proofs();
        let hashes = proofs
            .iter()
            .map(|proof| proof.hash)
            .collect::<HashSet<_>>();
        self.handle.replace_local_proofs(hashes).await?;
        for proof in proofs {
            self.handle.announce(proof.hash).await?;
        }
        Ok(())
    }
}
