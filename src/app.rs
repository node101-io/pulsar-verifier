use std::sync::Arc;

use tokio::signal;

use crate::{
    Error, Result,
    config::Config,
    control::ControlServer,
    p2p::{P2pExit, P2pService},
    store::ProofStore,
};

/// Owns the verifier process lifecycle and future long-running components.
pub(crate) struct App;

impl App {
    pub(crate) async fn run(config: Config) -> Result<()> {
        let control = ControlServer::bind(&config.runtime.control_socket).await?;
        let proof_store = Arc::new(ProofStore::new(config.proof_store)?);
        let mut p2p = if config.p2p.enabled {
            Some(P2pService::start(&config.p2p, proof_store).await?)
        } else {
            None
        };

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
        };

        let runtime_result = match exit {
            AppExit::Requested(request_result) => {
                let shutdown_result = match p2p.as_mut() {
                    Some(service) => service.shutdown(config.runtime.shutdown_timeout).await,
                    None => Ok(()),
                };
                request_result?;
                shutdown_result
            }
            AppExit::P2pTask(task) => {
                let error = task.into_error();
                if let Some(service) = p2p.as_mut() {
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
}

async fn wait_for_p2p_exit(service: &mut Option<P2pService>) -> P2pExit {
    match service {
        Some(service) => service.wait_for_exit().await,
        None => std::future::pending().await,
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
        config::{P2pConfig, ProofStoreConfig, RuntimeConfig},
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
