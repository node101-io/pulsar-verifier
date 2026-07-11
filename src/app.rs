use tokio::{signal, sync::mpsc, task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    config::{Config, P2pConfig},
    control::ControlServer,
    p2p::{P2pDriver, P2pEvent, P2pHandle, ValidatorSetClient, load_validator_identity},
};

/// Owns the verifier process lifecycle and future long-running components.
pub(crate) struct App;

impl App {
    pub(crate) async fn run(config: Config) -> Result<()> {
        let control = ControlServer::bind(&config.runtime.control_socket).await?;
        let cancellation = CancellationToken::new();
        let mut tasks = JoinSet::new();

        if config.p2p.enabled {
            start_p2p(&config.p2p, &cancellation, &mut tasks).await?;
        }

        // TODO: Start the RPC server with a child cancellation token.

        tracing::info!(
            socket = %config.runtime.control_socket.display(),
            "pulsar verifier started"
        );

        let runtime_result = tokio::select! {
            result = control.wait_for_shutdown() => {
                tracing::info!("shutdown requested through control socket");
                result
            }
            result = wait_for_signal() => {
                tracing::info!("shutdown requested by process signal");
                result
            }
            task = tasks.join_next(), if !tasks.is_empty() => {
                match task {
                    Some(Ok(Ok(()))) => Err(Error::P2pDriver(
                        "long-running runtime task exited unexpectedly".to_owned(),
                    )),
                    Some(Ok(Err(error))) => Err(error),
                    Some(Err(error)) => Err(Error::Task(error)),
                    None => Err(Error::P2pDriver("runtime task set became empty".to_owned())),
                }
            }
        };

        // Every future component observes the same token before its task is joined.
        cancellation.cancel();
        let drain_result = drain_tasks(&mut tasks, config.runtime.shutdown_timeout).await;
        drop(control);

        runtime_result?;
        drain_result?;
        tracing::info!("pulsar verifier stopped");
        Ok(())
    }
}

async fn start_p2p(
    config: &P2pConfig,
    cancellation: &CancellationToken,
    tasks: &mut JoinSet<Result<()>>,
) -> Result<()> {
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
    tasks.spawn(driver.run(cancellation.child_token()));
    tasks.spawn(run_p2p_events(handle, events, cancellation.child_token()));

    ready
        .await
        .map_err(|_| Error::P2pDriver("driver exited before becoming ready".to_owned()))??;

    // TODO: Pulsar Listener should call ValidatorSetClient::load and replace_authorized_peers
    // only after observing an on-chain validator-set change event.
    tracing::info!(%local_peer_id, "validator-authorized P2P network is ready");
    Ok(())
}

async fn run_p2p_events(
    handle: P2pHandle,
    mut events: mpsc::Receiver<P2pEvent>,
    cancellation: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            event = events.recv() => {
                let event = event.ok_or_else(|| {
                    Error::P2pDriver("P2P event channel closed".to_owned())
                })?;
                match event {
                    P2pEvent::ProofRequested { request_id, .. } => {
                        // TODO: Replace this cache miss with ProofService-backed content lookup.
                        handle.respond_proof(request_id, None).await?;
                    }
                    event => tracing::debug!(?event, "P2P event"),
                }
            }
        }
    }
}

async fn drain_tasks(
    tasks: &mut JoinSet<Result<()>>,
    shutdown_timeout: std::time::Duration,
) -> Result<()> {
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok(())
    };

    timeout(shutdown_timeout, drain)
        .await
        .map_err(|_| Error::ShutdownTimeout(shutdown_timeout))?
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
        config::{P2pConfig, RuntimeConfig},
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
