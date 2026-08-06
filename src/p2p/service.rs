use std::{sync::Arc, time::Duration};

use tokio::{task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result, config::P2pConfig, store::ProofStore};

use super::{
    Driver, DriverClient, DriverParts, ValidatorSetClient, Worker, WorkerHandle,
    load_validator_identity,
};

const DRIVER_TASK: &str = "p2p driver";
const WORKER_TASK: &str = "p2p worker";

/// Result of a long-running P2P task completing outside ordered shutdown.
pub(crate) struct P2pExit {
    task: &'static str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
}

impl P2pExit {
    pub(crate) fn into_error(self) -> Error {
        match self.result {
            Ok(Ok(())) => Error::TaskExitedUnexpectedly(self.task),
            Ok(Err(error)) => error,
            Err(error) => Error::Task(error),
        }
    }
}

/// Owns P2P task dependencies and their ordered graceful-shutdown protocol.
pub(crate) struct P2pService {
    driver: DriverClient,
    worker: WorkerHandle,
    driver_task: Option<JoinHandle<Result<()>>>,
    worker_task: Option<JoinHandle<Result<()>>>,
    force_cancel: CancellationToken,
}

impl P2pService {
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

        let DriverParts {
            driver,
            client,
            events,
            ready,
        } = Driver::build(config.clone(), identity, authorized_peers)?;
        let (worker, worker_handle) =
            Worker::new(client.clone(), events, store, config.event_buffer);
        let force_cancel = CancellationToken::new();
        let driver_task = tokio::spawn(driver.run(force_cancel.child_token()));
        let worker_task = tokio::spawn(worker.run(force_cancel.child_token()));
        let mut service = Self {
            driver: client,
            worker: worker_handle,
            driver_task: Some(driver_task),
            worker_task: Some(worker_task),
            force_cancel,
        };

        let Ok(readiness) = ready.await else {
            service.force_shutdown().await;
            return Err(Error::P2pDriver(
                "driver exited before becoming ready".to_owned(),
            ));
        };
        if let Err(error) = readiness {
            service.force_shutdown().await;
            return Err(error);
        }

        // TODO: Pulsar Listener should reload and replace validator authorization
        // only after observing an on-chain validator-set change event.
        tracing::info!(%local_peer_id, "validator-authorized P2P network is ready");
        Ok(service)
    }

    pub(crate) async fn wait_for_exit(&mut self) -> P2pExit {
        let driver = self
            .driver_task
            .as_mut()
            .expect("driver task exists while service is active");
        let worker = self
            .worker_task
            .as_mut()
            .expect("worker task exists while service is active");
        tokio::select! {
            result = driver => {
                self.driver_task.take();
                P2pExit { task: DRIVER_TASK, result }
            }
            result = worker => {
                self.worker_task.take();
                P2pExit { task: WORKER_TASK, result }
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
        // TODO: Stop future RPC, Listener, and verifier producers before P2P drain.
        let driver = self.driver.clone();
        tokio::select! {
            result = driver.drain() => result?,
            exit = self.wait_for_exit() => return Err(exit.into_error()),
        }
        self.worker.shutdown().await?;
        join_task(&mut self.worker_task, WORKER_TASK).await?;
        self.driver.shutdown().await?;
        join_task(&mut self.driver_task, DRIVER_TASK).await?;
        tracing::info!("p2p shutdown complete");
        Ok(())
    }

    pub(crate) async fn force_shutdown(&mut self) {
        self.force_cancel.cancel();
        abort_task(&mut self.worker_task).await;
        abort_task(&mut self.driver_task).await;
    }

    fn log_pending_phase(&self) {
        tracing::error!(
            driver_running = self.driver_task.is_some(),
            worker_running = self.worker_task.is_some(),
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
            let mut service = test_service(false).await;
            service.shutdown(Duration::from_secs(2)).await.unwrap();
        }
    }

    #[tokio::test]
    async fn shutdown_timeout_force_stops_remaining_tasks() {
        let mut service = test_service(true).await;
        assert!(matches!(
            service.shutdown(Duration::from_millis(50)).await,
            Err(Error::ShutdownTimeout(_))
        ));
        assert!(service.driver_task.is_none());
        assert!(service.worker_task.is_none());
    }

    async fn test_service(stall_worker: bool) -> P2pService {
        let identity = identity::Keypair::generate_ed25519();
        let local_peer = identity.public().to_peer_id();
        let config = test_config();
        let store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
        let DriverParts {
            driver,
            client,
            events,
            ready,
        } = Driver::build(config.clone(), identity, HashSet::from([local_peer])).unwrap();
        let (worker, worker_handle) =
            Worker::new(client.clone(), events, store, config.event_buffer);
        let force_cancel = CancellationToken::new();
        let driver_task = tokio::spawn(driver.run(force_cancel.child_token()));
        let worker_task = if stall_worker {
            tokio::spawn(async move {
                pending::<()>().await;
                drop(worker);
                Ok(())
            })
        } else {
            tokio::spawn(worker.run(force_cancel.child_token()))
        };
        ready.await.unwrap().unwrap();

        P2pService {
            driver: client,
            worker: worker_handle,
            driver_task: Some(driver_task),
            worker_task: Some(worker_task),
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
