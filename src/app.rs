use std::{sync::Arc, time::Duration};

use tokio::{signal, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    config::Config,
    control::ControlServer,
    listener::{ListenerExit, PulsarListener},
    p2p::{P2pExit, P2pService, ValidatorSetUpdater},
    rpc::{RpcExit, RpcServer},
    store::ProofStore,
    verification::{VerificationWorker, VerifierRegistry},
};

const VERIFICATION_TASK: &str = "verification worker";

/// Owns the verifier process lifecycle and future long-running components.
pub(crate) struct App;

impl App {
    pub(crate) async fn run(config: Config) -> Result<()> {
        let control = ControlServer::bind(&config.runtime.control_socket).await?;
        let proof_store = Arc::new(ProofStore::new(config.proof_store)?);
        // Subscribe before network producers can publish Store transitions.
        // TODO: Register the production Noir backend after its runtime is implemented.
        let verification_worker = VerificationWorker::new(
            Arc::clone(&proof_store),
            VerifierRegistry::new([])?,
            config.verification,
        );
        let mut p2p = if config.p2p.enabled {
            Some(P2pService::start(&config.p2p, &config.chain, Arc::clone(&proof_store)).await?)
        } else {
            None
        };
        let validator_updates = p2p.as_ref().map(P2pService::validator_set_updater);
        let verification_stop = CancellationToken::new();
        let mut verification_task = Some(tokio::spawn(
            verification_worker.run(verification_stop.clone()),
        ));
        let mut listener = match start_listener(
            config.listener,
            config.chain.clone(),
            Arc::clone(&proof_store),
            validator_updates,
            &control,
        )
        .await
        {
            Ok(ListenerStartup::Started(listener)) => Some(listener),
            Ok(ListenerStartup::Disabled) => None,
            Ok(ListenerStartup::Shutdown(request_result)) => {
                cleanup_startup(&mut p2p, None, &verification_stop, &mut verification_task).await;
                drop(control);
                request_result?;
                tracing::info!("pulsar verifier stopped during startup");
                return Ok(());
            }
            Err(error) => {
                cleanup_startup(&mut p2p, None, &verification_stop, &mut verification_task).await;
                return Err(error);
            }
        };
        let mut rpc = if config.rpc.enabled {
            match RpcServer::start(config.rpc, Arc::clone(&proof_store)).await {
                Ok(server) => Some(server),
                Err(error) => {
                    cleanup_startup(
                        &mut p2p,
                        listener.take(),
                        &verification_stop,
                        &mut verification_task,
                    )
                    .await;
                    return Err(error);
                }
            }
        } else {
            None
        };

        tracing::info!(
            socket = %config.runtime.control_socket.display(),
            "pulsar verifier started"
        );

        let exit = wait_for_app_exit(
            &control,
            &mut p2p,
            &mut listener,
            &mut verification_task,
            &mut rpc,
        )
        .await;

        let runtime_result = handle_app_exit(
            exit,
            &mut p2p,
            &mut listener,
            &verification_stop,
            &mut verification_task,
            &mut rpc,
            config.runtime.shutdown_timeout,
        )
        .await;
        drop(control);

        runtime_result?;
        tracing::info!("pulsar verifier stopped");
        Ok(())
    }
}

enum ListenerStartup {
    Started(PulsarListener),
    Disabled,
    Shutdown(Result<()>),
}

async fn start_listener(
    config: crate::config::ListenerConfig,
    chain: crate::config::ChainConfig,
    store: Arc<ProofStore>,
    validator_updates: Option<ValidatorSetUpdater>,
    control: &ControlServer,
) -> Result<ListenerStartup> {
    if !config.enabled {
        return Ok(ListenerStartup::Disabled);
    }

    tokio::select! {
        result = PulsarListener::start(config, chain, store, validator_updates) => {
            result.map(ListenerStartup::Started)
        }
        result = control.wait_for_shutdown() => {
            tracing::info!("shutdown requested through control socket during startup");
            Ok(ListenerStartup::Shutdown(result))
        }
        result = wait_for_signal() => {
            tracing::info!("shutdown requested by process signal during startup");
            Ok(ListenerStartup::Shutdown(result))
        }
    }
}

async fn handle_app_exit(
    exit: AppExit,
    p2p: &mut Option<P2pService>,
    listener: &mut Option<PulsarListener>,
    verification_stop: &CancellationToken,
    verification_task: &mut Option<JoinHandle<Result<()>>>,
    rpc: &mut Option<RpcServer>,
    shutdown_timeout: Duration,
) -> Result<()> {
    let error = match exit {
        AppExit::Requested(request_result) => {
            return shutdown_requested(
                request_result,
                p2p,
                listener,
                rpc,
                verification_stop,
                verification_task,
                shutdown_timeout,
            )
            .await;
        }
        AppExit::P2pTask(task) => task.into_error(),
        AppExit::ListenerTask(task) => task.into_error(),
        AppExit::VerificationTask(task) => {
            verification_task.take();
            unexpected_task_error(VERIFICATION_TASK, task)
        }
        AppExit::RpcTask(task) => task.into_error(),
    };
    force_shutdown_components(p2p, listener, verification_stop, verification_task, rpc).await;
    Err(error)
}

async fn wait_for_app_exit(
    control: &ControlServer,
    p2p: &mut Option<P2pService>,
    listener: &mut Option<PulsarListener>,
    verification_task: &mut Option<JoinHandle<Result<()>>>,
    rpc: &mut Option<RpcServer>,
) -> AppExit {
    tokio::select! {
        result = control.wait_for_shutdown() => {
            tracing::info!("shutdown requested through control socket");
            AppExit::Requested(result)
        }
        result = wait_for_signal() => {
            tracing::info!("shutdown requested by process signal");
            AppExit::Requested(result)
        }
        task = wait_for_p2p_exit(p2p) => AppExit::P2pTask(task),
        task = wait_for_listener_exit(listener) => AppExit::ListenerTask(task),
        task = wait_for_verification_exit(verification_task) => AppExit::VerificationTask(task),
        task = wait_for_rpc_exit(rpc) => AppExit::RpcTask(task),
    }
}

async fn shutdown_requested(
    request_result: Result<()>,
    p2p: &mut Option<P2pService>,
    listener: &mut Option<PulsarListener>,
    rpc: &mut Option<RpcServer>,
    verification_stop: &CancellationToken,
    verification_task: &mut Option<JoinHandle<Result<()>>>,
    shutdown_timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + shutdown_timeout;
    let mut first_error = request_result.err();
    if let Some(server) = rpc.as_mut() {
        server.mark_not_serving().await;
    }
    // TODO: Stop the future consumer submission RPC before P2P drain.
    if let Some(listener) = listener.take() {
        preserve_first_error(
            &mut first_error,
            shutdown_listener(listener, deadline, shutdown_timeout).await,
        );
    }
    if let Some(service) = p2p.take() {
        preserve_first_error(
            &mut first_error,
            shutdown_p2p(service, deadline, shutdown_timeout).await,
        );
    }
    preserve_first_error(
        &mut first_error,
        shutdown_verification(
            verification_stop,
            verification_task,
            deadline,
            shutdown_timeout,
        )
        .await,
    );
    if let Some(server) = rpc.take() {
        preserve_first_error(
            &mut first_error,
            shutdown_rpc(server, deadline, shutdown_timeout).await,
        );
    }
    first_error.map_or(Ok(()), Err)
}

enum AppExit {
    Requested(Result<()>),
    P2pTask(P2pExit),
    ListenerTask(ListenerExit),
    VerificationTask(std::result::Result<Result<()>, tokio::task::JoinError>),
    RpcTask(RpcExit),
}

async fn wait_for_listener_exit(listener: &mut Option<PulsarListener>) -> ListenerExit {
    match listener {
        Some(listener) => listener.wait_for_exit().await,
        None => std::future::pending().await,
    }
}

async fn wait_for_rpc_exit(server: &mut Option<RpcServer>) -> RpcExit {
    match server {
        Some(server) => server.wait_for_exit().await,
        None => std::future::pending().await,
    }
}

async fn wait_for_p2p_exit(service: &mut Option<P2pService>) -> P2pExit {
    match service {
        Some(service) => service.wait_for_exit().await,
        None => std::future::pending().await,
    }
}

async fn wait_for_verification_exit(
    task: &mut Option<JoinHandle<Result<()>>>,
) -> std::result::Result<Result<()>, tokio::task::JoinError> {
    task.as_mut()
        .expect("verification task exists while App is active")
        .await
}

async fn shutdown_p2p(
    service: P2pService,
    deadline: Instant,
    total_timeout: Duration,
) -> Result<()> {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        service.force_shutdown().await;
        return Err(Error::ShutdownTimeout(total_timeout));
    };
    service
        .shutdown(remaining)
        .await
        .map_err(|error| match error {
            Error::ShutdownTimeout(_) => Error::ShutdownTimeout(total_timeout),
            error => error,
        })
}

async fn shutdown_listener(
    listener: PulsarListener,
    deadline: Instant,
    total_timeout: Duration,
) -> Result<()> {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        listener.force_shutdown().await;
        return Err(Error::ShutdownTimeout(total_timeout));
    };
    listener
        .shutdown(remaining)
        .await
        .map_err(|error| match error {
            Error::ShutdownTimeout(_) => Error::ShutdownTimeout(total_timeout),
            error => error,
        })
}

async fn force_shutdown_components(
    p2p: &mut Option<P2pService>,
    listener: &mut Option<PulsarListener>,
    verification_stop: &CancellationToken,
    verification_task: &mut Option<JoinHandle<Result<()>>>,
    rpc: &mut Option<RpcServer>,
) {
    if let Some(listener) = listener.take() {
        listener.force_shutdown().await;
    }
    if let Some(service) = p2p.take() {
        service.force_shutdown().await;
    }
    verification_stop.cancel();
    abort_task(verification_task).await;
    if let Some(server) = rpc.take() {
        server.force_shutdown().await;
    }
}

async fn cleanup_startup(
    p2p: &mut Option<P2pService>,
    listener: Option<PulsarListener>,
    verification_stop: &CancellationToken,
    verification_task: &mut Option<JoinHandle<Result<()>>>,
) {
    if let Some(listener) = listener {
        listener.force_shutdown().await;
    }
    if let Some(service) = p2p.take() {
        service.force_shutdown().await;
    }
    verification_stop.cancel();
    abort_task(verification_task).await;
}

async fn shutdown_rpc(server: RpcServer, deadline: Instant, total_timeout: Duration) -> Result<()> {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        server.force_shutdown().await;
        return Err(Error::ShutdownTimeout(total_timeout));
    };
    server
        .shutdown(remaining)
        .await
        .map_err(|error| match error {
            Error::ShutdownTimeout(_) => Error::ShutdownTimeout(total_timeout),
            error => error,
        })
}

async fn shutdown_verification(
    stop: &CancellationToken,
    task: &mut Option<JoinHandle<Result<()>>>,
    deadline: Instant,
    total_timeout: Duration,
) -> Result<()> {
    stop.cancel();
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        abort_task(task).await;
        return Err(Error::ShutdownTimeout(total_timeout));
    };
    let result = tokio::time::timeout(
        remaining,
        task.as_mut()
            .ok_or(Error::TaskExitedUnexpectedly(VERIFICATION_TASK))?,
    )
    .await;
    if let Ok(result) = result {
        task.take();
        task_result(result)
    } else {
        abort_task(task).await;
        Err(Error::ShutdownTimeout(total_timeout))
    }
}

fn preserve_first_error(first: &mut Option<Error>, result: Result<()>) {
    if let Err(error) = result {
        if first.is_none() {
            *first = Some(error);
        } else {
            tracing::warn!(%error, "additional shutdown error");
        }
    }
}

fn unexpected_task_error(
    name: &'static str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Error {
    match result {
        Ok(Ok(())) => Error::TaskExitedUnexpectedly(name),
        Ok(Err(error)) => error,
        Err(error) => Error::Task(error),
    }
}

fn task_result(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    result.map_err(Error::Task)?
}

async fn abort_task(task: &mut Option<JoinHandle<Result<()>>>) {
    let Some(task) = task.take() else {
        return;
    };
    task.abort();
    if let Err(error) = task.await
        && !error.is_cancelled()
    {
        tracing::warn!(%error, "failed to join force-stopped verification task");
    }
}

async fn wait_for_signal() -> Result<()> {
    let mut terminate =
        signal::unix::signal(signal::unix::SignalKind::terminate()).map_err(Error::Signal)?;

    tokio::select! {
        result = signal::ctrl_c() => result.map_err(Error::Signal),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::{
            ChainConfig, ListenerConfig, P2pConfig, ProofStoreConfig, RpcConfig, RuntimeConfig,
            VerificationConfig,
        },
        control::request_shutdown,
    };

    fn test_config(temp_dir: &TempDir) -> Config {
        Config {
            runtime: RuntimeConfig {
                control_socket: temp_dir.path().join("runtime/control.sock"),
                shutdown_timeout: Duration::from_secs(2),
            },
            chain: ChainConfig {
                chain_id: String::new(),
                comet_rpc_url: "http://127.0.0.1:26657".to_owned(),
                request_timeout: Duration::from_secs(1),
            },
            listener: ListenerConfig {
                enabled: false,
                reconnect_initial_backoff: Duration::from_millis(250),
                reconnect_max_backoff: Duration::from_secs(30),
            },
            proof_store: ProofStoreConfig::test_default(),
            p2p: P2pConfig::disabled(),
            verification: VerificationConfig {
                max_concurrent_jobs: 2,
                job_timeout: Duration::from_secs(1),
                max_retries: 0,
                retry_backoff: Duration::ZERO,
            },
            rpc: RpcConfig::disabled(),
        }
    }

    #[tokio::test]
    async fn app_stops_through_control_socket() {
        let temp_dir = TempDir::new().unwrap();
        let config = test_config(&temp_dir);
        let client_config = config.runtime.clone();

        let app = tokio::spawn(App::run(config));
        for _ in 0..40 {
            if client_config.control_socket.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        request_shutdown(&client_config).await.unwrap();
        app.await.unwrap().unwrap();
        assert!(!client_config.control_socket.exists());
    }

    #[tokio::test]
    async fn app_can_stop_while_listener_is_reconnecting_during_startup() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = test_config(&temp_dir);
        config.chain.chain_id = "pulsar-test-1".to_owned();
        config.chain.comet_rpc_url = "http://127.0.0.1:9".to_owned();
        config.chain.request_timeout = Duration::from_millis(50);
        config.listener.enabled = true;
        config.listener.reconnect_initial_backoff = Duration::from_millis(10);
        config.listener.reconnect_max_backoff = Duration::from_millis(20);
        let client_config = config.runtime.clone();

        let app = tokio::spawn(App::run(config));
        for _ in 0..40 {
            if client_config.control_socket.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        request_shutdown(&client_config).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), app)
            .await
            .expect("application should stop while listener startup retries")
            .unwrap()
            .unwrap();
        assert!(!client_config.control_socket.exists());
    }
}
