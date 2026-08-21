use std::{sync::Arc, time::Duration};

use tokio::{task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    chain::PulsarClient,
    config::{ChainConfig, P2pConfig},
    store::ProofStore,
};

use super::{
    Driver, DriverClient, DriverParts, ValidatorSetClient, Worker, load_validator_identity,
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
    driver: Option<DriverClient>,
    worker_stop: CancellationToken,
    driver_task: Option<JoinHandle<Result<()>>>,
    worker_task: Option<JoinHandle<Result<()>>>,
}

impl P2pService {
    pub(crate) async fn start(
        config: &P2pConfig,
        chain_config: &ChainConfig,
        store: Arc<ProofStore>,
    ) -> Result<Self> {
        let identity = load_validator_identity(&config.validator_key_path)?;
        let local_peer_id = identity.public().to_peer_id();
        let validator_client = ValidatorSetClient::new(PulsarClient::new(chain_config)?);
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
        let worker = Worker::new(client.clone(), events, store);
        let worker_stop = CancellationToken::new();
        let driver_task = tokio::spawn(driver.run());
        let worker_task = tokio::spawn(worker.run(worker_stop.clone()));
        let service = Self {
            driver: Some(client),
            worker_stop,
            driver_task: Some(driver_task),
            worker_task: Some(worker_task),
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

    pub(crate) async fn shutdown(mut self, shutdown_timeout: Duration) -> Result<()> {
        match timeout(shutdown_timeout, self.shutdown_ordered()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.abort_and_join().await;
                Err(error)
            }
            Err(_) => {
                self.log_pending_phase();
                self.abort_and_join().await;
                Err(Error::ShutdownTimeout(shutdown_timeout))
            }
        }
    }

    async fn shutdown_ordered(&mut self) -> Result<()> {
        // TODO: Stop future RPC, Listener, and verifier producers before P2P drain.
        {
            let driver = self.driver.as_ref().ok_or(Error::P2pDriverClosed)?.clone();
            tokio::select! {
                result = driver.drain() => result?,
                exit = self.wait_for_exit() => return Err(exit.into_error()),
            }
        }

        self.worker_stop.cancel();
        self.wait_for_worker_shutdown().await?;
        drop(self.driver.take());
        join_task(&mut self.driver_task, DRIVER_TASK).await?;
        tracing::info!("p2p shutdown complete");
        Ok(())
    }

    pub(crate) async fn force_shutdown(mut self) {
        self.abort_and_join().await;
    }

    async fn abort_and_join(&mut self) {
        abort_task(&mut self.worker_task).await;
        abort_task(&mut self.driver_task).await;
    }

    async fn wait_for_worker_shutdown(&mut self) -> Result<()> {
        let driver = self
            .driver_task
            .as_mut()
            .ok_or(Error::TaskExitedUnexpectedly(DRIVER_TASK))?;
        let worker = self
            .worker_task
            .as_mut()
            .ok_or(Error::TaskExitedUnexpectedly(WORKER_TASK))?;

        tokio::select! {
            result = worker => {
                self.worker_task.take();
                task_result(result)
            }
            result = driver => {
                self.driver_task.take();
                Err(P2pExit { task: DRIVER_TASK, result }.into_error())
            }
        }
    }

    fn log_pending_phase(&self) {
        tracing::error!(
            driver_running = self.driver_task.is_some(),
            worker_running = self.worker_task.is_some(),
            "p2p shutdown timed out"
        );
    }
}

impl Drop for P2pService {
    fn drop(&mut self) {
        let active = self.driver_task.is_some() || self.worker_task.is_some();
        if active {
            tracing::warn!("P2P service dropped before task shutdown completed");
        }
        if let Some(task) = &self.worker_task {
            task.abort();
        }
        if let Some(task) = &self.driver_task {
            task.abort();
        }
    }
}

async fn join_task(task: &mut Option<JoinHandle<Result<()>>>, name: &'static str) -> Result<()> {
    // Keep the handle owned by the service while awaiting so timeout cancellation
    // cannot detach the underlying task before force shutdown can abort it.
    let result = task
        .as_mut()
        .ok_or(Error::TaskExitedUnexpectedly(name))?
        .await;
    task.take();
    task_result(result)
}

fn task_result(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    match result {
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
    use std::{
        collections::HashSet,
        future::pending,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use super::*;
    use crate::config::ProofStoreConfig;
    use libp2p::{Multiaddr, identity};

    #[tokio::test]
    async fn ordered_shutdown_is_repeatable() {
        for _ in 0..10 {
            let (service, worker_dropped) = test_service(false).await;
            service.shutdown(Duration::from_secs(2)).await.unwrap();
            assert!(worker_dropped.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn shutdown_timeout_force_stops_remaining_tasks() {
        let (service, worker_dropped) = test_service(true).await;
        assert!(matches!(
            service.shutdown(Duration::from_millis(50)).await,
            Err(Error::ShutdownTimeout(_))
        ));
        assert!(worker_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dropping_service_aborts_owned_tasks() {
        let (service, worker_dropped) = test_service(true).await;
        drop(service);

        timeout(Duration::from_secs(1), async {
            while !worker_dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn unexpected_driver_exit_is_reported_and_sibling_is_stopped() {
        let (mut service, worker_dropped) = test_service(true).await;
        service.driver_task.as_ref().unwrap().abort();

        let exit = service.wait_for_exit().await;
        assert_eq!(exit.task, DRIVER_TASK);
        assert!(matches!(exit.into_error(), Error::Task(_)));
        service.force_shutdown().await;
        assert!(worker_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unexpected_worker_exit_is_reported_and_driver_is_stopped() {
        let (mut service, _) = test_service(true).await;
        service.worker_task.as_ref().unwrap().abort();

        let exit = service.wait_for_exit().await;
        assert_eq!(exit.task, WORKER_TASK);
        assert!(matches!(exit.into_error(), Error::Task(_)));
        service.force_shutdown().await;
    }

    #[tokio::test]
    async fn driver_failure_during_worker_drain_remains_the_root_error() {
        let (mut service, worker_dropped) = test_service(true).await;
        service.driver.as_ref().unwrap().drain().await.unwrap();
        service.worker_stop.cancel();
        service.driver_task.as_ref().unwrap().abort();

        let error = service.wait_for_worker_shutdown().await.unwrap_err();
        assert!(matches!(error, Error::Task(_)));
        service.force_shutdown().await;
        assert!(worker_dropped.load(Ordering::SeqCst));
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    async fn test_service(stall_worker: bool) -> (P2pService, Arc<AtomicBool>) {
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
        let worker = Worker::new(client.clone(), events, store);
        let worker_stop = CancellationToken::new();
        let driver_task = tokio::spawn(driver.run());
        let worker_dropped = Arc::new(AtomicBool::new(false));
        let drop_flag = DropFlag(Arc::clone(&worker_dropped));
        let worker_task = if stall_worker {
            tokio::spawn(async move {
                let _drop_flag = drop_flag;
                pending::<()>().await;
                drop(worker);
                Ok(())
            })
        } else {
            let stop = worker_stop.clone();
            tokio::spawn(async move {
                let _drop_flag = drop_flag;
                worker.run(stop).await
            })
        };
        ready.await.unwrap().unwrap();

        (
            P2pService {
                driver: Some(client),
                worker_stop,
                driver_task: Some(driver_task),
                worker_task: Some(worker_task),
            },
            worker_dropped,
        )
    }

    fn test_config() -> P2pConfig {
        P2pConfig {
            enabled: true,
            chain_id: "pulsar-shutdown-test".to_owned(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap()],
            bootnodes: Vec::new(),
            validator_key_path: PathBuf::from("/unused"),
            max_availability_message_bytes: 64 * 1024,
            max_proof_bytes: 8 * 1024 * 1024,
            proof_request_timeout: Duration::from_secs(1),
            command_buffer: 32,
            event_buffer: 32,
        }
    }
}
