use std::sync::Arc;

use tokio::signal;

use crate::{
    Error, Result,
    config::Config,
    control::ControlServer,
    p2p::{P2pRuntime, TaskExit},
    store::ProofStore,
};

/// Owns the verifier process lifecycle and future long-running components.
pub(crate) struct App;

impl App {
    pub(crate) async fn run(config: Config) -> Result<()> {
        let control = ControlServer::bind(&config.runtime.control_socket).await?;
        let proof_store = Arc::new(ProofStore::new(config.proof_store)?);
        let mut p2p = if config.p2p.enabled {
            Some(P2pRuntime::start(&config.p2p, proof_store).await?)
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
                    Some(runtime) => runtime.shutdown(config.runtime.shutdown_timeout).await,
                    None => Ok(()),
                };
                request_result?;
                shutdown_result
            }
            AppExit::P2pTask(task) => {
                let error = task.into_error();
                if let Some(runtime) = p2p.as_mut() {
                    runtime.force_shutdown().await;
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
    P2pTask(TaskExit),
}

async fn wait_for_p2p_exit(runtime: &mut Option<P2pRuntime>) -> TaskExit {
    match runtime {
        Some(runtime) => runtime.wait_for_exit().await,
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
    use std::{collections::HashSet, net::TcpListener, path::PathBuf, time::Duration};

    use bytes::Bytes;
    use libp2p::{Multiaddr, identity};
    use reqwest::Url;
    use tempfile::TempDir;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        config::{P2pConfig, ProofStoreConfig, RuntimeConfig},
        control::request_shutdown,
        p2p::{P2pDriver, P2pEventLoop, P2pEventLoopHandle, P2pHandle},
        proof::{ProofHash, ProofType},
        store::ProofSource,
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

    #[tokio::test]
    async fn p2p_event_loop_serves_and_stores_chain_observed_proof() {
        let server_identity = identity::Keypair::generate_ed25519();
        let client_identity = identity::Keypair::generate_ed25519();
        let server_peer = server_identity.public().to_peer_id();
        let client_peer = client_identity.public().to_peer_id();
        let authorized = HashSet::from([server_peer, client_peer]);
        let server_address = free_tcp_address();

        let server_store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
        let client_store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
        let proof = Bytes::from_static(b"store-backed-proof");
        let proof_hash = ProofHash::digest(&proof);
        let proof_type = ProofType::new("mock").unwrap();
        server_store
            .insert_local_proof(
                proof_hash,
                proof_type.clone(),
                proof.clone(),
                ProofSource::Rpc,
            )
            .await
            .unwrap();
        client_store
            .observe_chain_proof(proof_hash, proof_type)
            .await
            .unwrap();

        let (server_driver, server_handle, server_events, server_ready) = P2pDriver::new(
            test_p2p_config(server_address.clone()),
            server_identity,
            authorized.clone(),
        )
        .unwrap();
        let (client_driver, client_handle, client_events, client_ready) = P2pDriver::new(
            test_p2p_config("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
            client_identity,
            authorized,
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        let (server_event_loop, server_event_loop_handle) = P2pEventLoop::new(
            server_handle.clone(),
            server_events,
            Arc::clone(&server_store),
            32,
        );
        let (client_event_loop, client_event_loop_handle) = P2pEventLoop::new(
            client_handle.clone(),
            client_events,
            Arc::clone(&client_store),
            32,
        );
        let server_driver_task = tokio::spawn(server_driver.run(cancellation.child_token()));
        let server_event_loop_task =
            tokio::spawn(server_event_loop.run(cancellation.child_token()));
        let client_driver_task = tokio::spawn(client_driver.run(cancellation.child_token()));
        let client_event_loop_task =
            tokio::spawn(client_event_loop.run(cancellation.child_token()));
        wait_ready(server_ready).await;
        wait_ready(client_ready).await;

        client_handle
            .dial(server_peer, vec![server_address])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        client_handle
            .request_proof(server_peer, proof_hash)
            .await
            .unwrap();

        timeout(Duration::from_secs(5), async {
            loop {
                if client_store
                    .get_content(proof_hash)
                    .await
                    .is_some_and(|content| content.proof == proof)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        shutdown_test_p2p(
            server_handle,
            server_event_loop_handle,
            server_driver_task,
            server_event_loop_task,
        )
        .await;
        shutdown_test_p2p(
            client_handle,
            client_event_loop_handle,
            client_driver_task,
            client_event_loop_task,
        )
        .await;
    }

    async fn shutdown_test_p2p(
        handle: P2pHandle,
        event_loop: P2pEventLoopHandle,
        driver_task: tokio::task::JoinHandle<Result<()>>,
        event_loop_task: tokio::task::JoinHandle<Result<()>>,
    ) {
        handle.drain().await.unwrap();
        event_loop.shutdown().await.unwrap();
        timeout(Duration::from_secs(5), event_loop_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        handle.shutdown().await.unwrap();
        timeout(Duration::from_secs(5), driver_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    async fn wait_ready(ready: tokio::sync::oneshot::Receiver<Result<()>>) {
        timeout(Duration::from_secs(5), ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn free_tcp_address() -> Multiaddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap()
    }

    fn test_p2p_config(listen_address: Multiaddr) -> P2pConfig {
        P2pConfig {
            enabled: true,
            chain_id: "pulsar-store-test".to_owned(),
            listen_addresses: vec![listen_address],
            bootnodes: Vec::new(),
            validator_key_path: PathBuf::from("/unused"),
            comet_rpc_url: Url::parse("http://127.0.0.1:26657").unwrap(),
            comet_rpc_timeout: Duration::from_secs(1),
            max_availability_message_bytes: 64 * 1024,
            max_proof_bytes: 8 * 1024 * 1024,
            proof_request_timeout: Duration::from_secs(3),
            command_buffer: 32,
            event_buffer: 128,
        }
    }
}
