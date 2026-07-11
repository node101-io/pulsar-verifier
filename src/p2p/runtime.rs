use std::{sync::Arc, time::Duration};

use tokio::{task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result, config::P2pConfig, store::ProofStore};

use super::{
    P2pDriver, P2pEventLoop, P2pEventLoopHandle, P2pHandle, ValidatorSetClient,
    load_validator_identity,
};

const DRIVER_TASK: &str = "p2p driver";
const EVENT_LOOP_TASK: &str = "p2p event loop";

/// Result of a long-running P2P task completing outside ordered shutdown.
pub(crate) struct TaskExit {
    task: &'static str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
}

impl TaskExit {
    pub(crate) fn into_error(self) -> Error {
        match self.result {
            Ok(Ok(())) => Error::TaskExitedUnexpectedly(self.task),
            Ok(Err(error)) => error,
            Err(error) => Error::Task(error),
        }
    }
}

/// Owns P2P task dependencies and their ordered graceful-shutdown protocol.
pub(crate) struct P2pRuntime {
    handle: P2pHandle,
    event_loop: P2pEventLoopHandle,
    driver_task: Option<JoinHandle<Result<()>>>,
    event_loop_task: Option<JoinHandle<Result<()>>>,
    force_cancel: CancellationToken,
}

impl P2pRuntime {
    pub(crate) async fn start(config: &P2pConfig, store: Arc<ProofStore>) -> Result<Self> {
        let identity = load_validator_identity(&config.validator_key_path)?;
        let local_peer_id = identity.public().to_peer_id();
        let validator_client = ValidatorSetClient::new(
            config.comet_rpc_url.clone(),
            config.chain_id.clone(),
            config.comet_rpc_timeout,
        )?;
        let authorized_peers = validator_client.load().await?;
        if !authorized_peers.contains(&local_peer_id) {
            return Err(Error::P2pAuthorization(format!(
                "local peer {local_peer_id} is not in the active validator set"
            )));
        }

        let (driver, handle, events, ready) =
            P2pDriver::new(config.clone(), identity, authorized_peers)?;
        let (event_loop, event_loop_handle) =
            P2pEventLoop::new(handle.clone(), events, store, config.event_buffer);
        let force_cancel = CancellationToken::new();
        let driver_task = tokio::spawn(driver.run(force_cancel.child_token()));
        let event_loop_task = tokio::spawn(event_loop.run(force_cancel.child_token()));
        let mut runtime = Self {
            handle,
            event_loop: event_loop_handle,
            driver_task: Some(driver_task),
            event_loop_task: Some(event_loop_task),
            force_cancel,
        };

        let Ok(readiness) = ready.await else {
            runtime.force_shutdown().await;
            return Err(Error::P2pDriver(
                "driver exited before becoming ready".to_owned(),
            ));
        };
        if let Err(error) = readiness {
            runtime.force_shutdown().await;
            return Err(error);
        }

        // TODO: Pulsar Listener should reload and replace validator authorization
        // only after observing an on-chain validator-set change event.
        tracing::info!(%local_peer_id, "validator-authorized P2P network is ready");
        Ok(runtime)
    }

    pub(crate) async fn wait_for_exit(&mut self) -> TaskExit {
        let driver = self
            .driver_task
            .as_mut()
            .expect("driver task exists while runtime is active");
        let event_loop = self
            .event_loop_task
            .as_mut()
            .expect("event loop task exists while runtime is active");
        tokio::select! {
            result = driver => {
                self.driver_task.take();
                TaskExit { task: DRIVER_TASK, result }
            }
            result = event_loop => {
                self.event_loop_task.take();
                TaskExit { task: EVENT_LOOP_TASK, result }
            }
        }
    }

    pub(crate) async fn shutdown(&mut self, shutdown_timeout: Duration) -> Result<()> {
        match timeout(shutdown_timeout, self.shutdown_ordered()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.force_shutdown().await;
                Err(error)
            }
            Err(_) => {
                self.log_pending_phase();
                self.force_shutdown().await;
                Err(Error::ShutdownTimeout(shutdown_timeout))
            }
        }
    }

    async fn shutdown_ordered(&mut self) -> Result<()> {
        // TODO: Stop future RPC, Listener, requester, and verifier producers before P2P drain.
        let handle = self.handle.clone();
        tokio::select! {
            result = handle.drain() => result?,
            exit = self.wait_for_exit() => return Err(exit.into_error()),
        }
        self.event_loop.shutdown().await?;
        join_task(&mut self.event_loop_task, EVENT_LOOP_TASK).await?;
        self.handle.shutdown().await?;
        join_task(&mut self.driver_task, DRIVER_TASK).await?;
        tracing::info!("p2p shutdown complete");
        Ok(())
    }

    pub(crate) async fn force_shutdown(&mut self) {
        self.force_cancel.cancel();
        abort_task(&mut self.event_loop_task).await;
        abort_task(&mut self.driver_task).await;
    }

    fn log_pending_phase(&self) {
        tracing::error!(
            driver_running = self.driver_task.is_some(),
            event_loop_running = self.event_loop_task.is_some(),
            "p2p shutdown timed out"
        );
    }
}

async fn join_task(task: &mut Option<JoinHandle<Result<()>>>, name: &'static str) -> Result<()> {
    let task = task.take().ok_or(Error::TaskExitedUnexpectedly(name))?;
    match task.await {
        Ok(result) => result,
        Err(error) => Err(Error::Task(error)),
    }
}

async fn abort_task(task: &mut Option<JoinHandle<Result<()>>>) {
    let Some(task) = task.take() else {
        return;
    };
    task.abort();
    if let Err(error) = task.await
        && !error.is_cancelled()
    {
        tracing::warn!(%error, "failed to join force-stopped P2P task");
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, future::pending, path::PathBuf};

    use libp2p::{Multiaddr, identity};
    use reqwest::Url;

    use super::*;
    use crate::config::ProofStoreConfig;

    #[tokio::test]
    async fn ordered_shutdown_is_repeatable() {
        for _ in 0..10 {
            let mut runtime = test_runtime(false).await;
            runtime.shutdown(Duration::from_secs(2)).await.unwrap();
        }
    }

    #[tokio::test]
    async fn shutdown_timeout_force_stops_remaining_tasks() {
        let mut runtime = test_runtime(true).await;
        assert!(matches!(
            runtime.shutdown(Duration::from_millis(50)).await,
            Err(Error::ShutdownTimeout(_))
        ));
        assert!(runtime.driver_task.is_none());
        assert!(runtime.event_loop_task.is_none());
    }

    async fn test_runtime(stall_event_loop: bool) -> P2pRuntime {
        let identity = identity::Keypair::generate_ed25519();
        let local_peer = identity.public().to_peer_id();
        let config = test_config();
        let store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
        let (driver, handle, events, ready) =
            P2pDriver::new(config.clone(), identity, HashSet::from([local_peer])).unwrap();
        let (event_loop, event_loop_handle) =
            P2pEventLoop::new(handle.clone(), events, store, config.event_buffer);
        let force_cancel = CancellationToken::new();
        let driver_task = tokio::spawn(driver.run(force_cancel.child_token()));
        let event_loop_task = if stall_event_loop {
            tokio::spawn(async move {
                pending::<()>().await;
                drop(event_loop);
                Ok(())
            })
        } else {
            tokio::spawn(event_loop.run(force_cancel.child_token()))
        };
        ready.await.unwrap().unwrap();

        P2pRuntime {
            handle,
            event_loop: event_loop_handle,
            driver_task: Some(driver_task),
            event_loop_task: Some(event_loop_task),
            force_cancel,
        }
    }

    fn test_config() -> P2pConfig {
        P2pConfig {
            enabled: true,
            chain_id: "pulsar-shutdown-test".to_owned(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap()],
            bootnodes: Vec::new(),
            validator_key_path: PathBuf::from("/unused"),
            comet_rpc_url: Url::parse("http://127.0.0.1:26657").unwrap(),
            comet_rpc_timeout: Duration::from_secs(1),
            max_availability_message_bytes: 64 * 1024,
            max_proof_bytes: 8 * 1024 * 1024,
            proof_request_timeout: Duration::from_secs(1),
            command_buffer: 32,
            event_buffer: 32,
        }
    }
}
