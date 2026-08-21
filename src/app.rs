use std::{sync::Arc, time::Duration};

use tokio::{signal, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    config::Config,
    control::ControlServer,
    p2p::{P2pExit, P2pService},
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
            Some(P2pService::start(&config.p2p, Arc::clone(&proof_store)).await?)
        } else {
            None
        };
        let verification_stop = CancellationToken::new();
        let mut verification_task = Some(tokio::spawn(
            verification_worker.run(verification_stop.clone()),
        ));

        // TODO: Start the RPC server with a child cancellation token.

        tracing::info!(
            socket = %config.runtime.control_socket.display(),
            "pulsar verifier started"
        );

        let exit = tokio::select! {
            result = control.wait_for_shutdown() => {
                tracing::info!("shutdown requested through control socket");
                AppExit::Requested(result)
            }
            result = wait_for_signal() => {
                tracing::info!("shutdown requested by process signal");
                AppExit::Requested(result)
            }
            task = wait_for_p2p_exit(&mut p2p) => {
                AppExit::P2pTask(task)
            }
            task = wait_for_verification_exit(&mut verification_task) => {
                AppExit::VerificationTask(task)
            }
        };

        let runtime_result = match exit {
            AppExit::Requested(request_result) => {
                let deadline = Instant::now() + config.runtime.shutdown_timeout;
                let mut first_error = request_result.err();
                if let Some(service) = p2p.take() {
                    preserve_first_error(
                        &mut first_error,
                        shutdown_p2p(service, deadline, config.runtime.shutdown_timeout).await,
                    );
                }
                preserve_first_error(
                    &mut first_error,
                    shutdown_verification(
                        &verification_stop,
                        &mut verification_task,
                        deadline,
                        config.runtime.shutdown_timeout,
                    )
                    .await,
                );
                first_error.map_or(Ok(()), Err)
            }
            AppExit::P2pTask(task) => {
                let error = task.into_error();
                if let Some(service) = p2p.take() {
                    service.force_shutdown().await;
                }
                verification_stop.cancel();
                abort_task(&mut verification_task).await;
                Err(error)
            }
            AppExit::VerificationTask(task) => {
                verification_task.take();
                let error = unexpected_task_error(VERIFICATION_TASK, task);
                if let Some(service) = p2p.take() {
                    service.force_shutdown().await;
                }
                Err(error)
            }
        };
        drop(control);

        runtime_result?;
        tracing::info!("pulsar verifier stopped");
        Ok(())
    }
}

enum AppExit {
    Requested(Result<()>),
    P2pTask(P2pExit),
    VerificationTask(std::result::Result<Result<()>, tokio::task::JoinError>),
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
        config::{P2pConfig, ProofStoreConfig, RuntimeConfig, VerificationConfig},
        control::request_shutdown,
    };

    #[tokio::test]
    async fn app_stops_through_control_socket() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config {
            runtime: RuntimeConfig {
                control_socket: temp_dir.path().join("runtime/control.sock"),
                shutdown_timeout: Duration::from_secs(2),
            },
            proof_store: ProofStoreConfig::test_default(),
            p2p: P2pConfig::disabled(),
            verification: VerificationConfig {
                max_concurrent_jobs: 2,
                job_timeout: Duration::from_secs(1),
                max_retries: 0,
                retry_backoff: Duration::ZERO,
            },
        };
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
}
