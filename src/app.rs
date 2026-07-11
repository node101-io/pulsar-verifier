use tokio::{signal, task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result, config::Config, control::ControlServer};

/// Owns the verifier process lifecycle and future long-running components.
pub(crate) struct App;

impl App {
    pub(crate) async fn run(config: Config) -> Result<()> {
        let control = ControlServer::bind(&config.runtime.control_socket).await?;
        let cancellation = CancellationToken::new();
        let mut tasks = JoinSet::new();

        // TODO: Start the P2P driver and RPC server with child cancellation tokens.

        tracing::info!(
            socket = %config.runtime.control_socket.display(),
            "pulsar verifier started"
        );

        tokio::select! {
            result = control.wait_for_shutdown() => {
                result?;
                tracing::info!("shutdown requested through control socket");
            }
            result = wait_for_signal() => {
                result?;
                tracing::info!("shutdown requested by process signal");
            }
        }

        // Every future component observes the same token before its task is joined.
        cancellation.cancel();
        drain_tasks(&mut tasks, config.runtime.shutdown_timeout).await?;
        drop(control);

        tracing::info!("pulsar verifier stopped");
        Ok(())
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
    use crate::{config::RuntimeConfig, control::request_shutdown};

    #[tokio::test]
    async fn app_stops_through_control_socket() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config {
            runtime: RuntimeConfig {
                control_socket: temp_dir.path().join("runtime/control.sock"),
                shutdown_timeout: Duration::from_secs(2),
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
