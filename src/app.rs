use std::{collections::HashSet, sync::Arc};

use tokio::{signal, sync::mpsc, task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    config::{Config, P2pConfig},
    control::ControlServer,
    p2p::{P2pDriver, P2pEvent, P2pHandle, ValidatorSetClient, load_validator_identity},
    store::{ProofStore, ProofStoreEvent, ProofStoreSubscription},
};

/// Owns the verifier process lifecycle and future long-running components.
pub(crate) struct App;

impl App {
    pub(crate) async fn run(config: Config) -> Result<()> {
        let control = ControlServer::bind(&config.runtime.control_socket).await?;
        let cancellation = CancellationToken::new();
        let mut tasks = JoinSet::new();
        let proof_store = Arc::new(ProofStore::new(config.proof_store)?);

        if config.p2p.enabled {
            start_p2p(&config.p2p, proof_store, &cancellation, &mut tasks).await?;
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
    proof_store: Arc<ProofStore>,
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
    let store_events = proof_store.subscribe();
    tasks.spawn(driver.run(cancellation.child_token()));
    tasks.spawn(run_p2p_events(
        handle,
        events,
        proof_store,
        store_events,
        cancellation.child_token(),
    ));

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
    proof_store: Arc<ProofStore>,
    mut store_events: ProofStoreSubscription,
    cancellation: CancellationToken,
) -> Result<()> {
    reconcile_local_proofs(&handle, &proof_store).await?;
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            event = events.recv() => {
                let event = event.ok_or_else(|| {
                    Error::P2pDriver("P2P event channel closed".to_owned())
                })?;
                match event {
                    P2pEvent::ProofRequested { request_id, proof_hash, .. } => {
                        let content = proof_store.get_content(proof_hash).await;
                        tolerate_shutdown(
                            handle.respond_proof(request_id, content).await,
                            &cancellation,
                        )?;
                    }
                    P2pEvent::ProofReceived { peer, content, .. } => {
                        if let Err(error) = proof_store
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
                    P2pEvent::PeerConnected { .. } => {
                        tolerate_shutdown(
                            reconcile_local_proofs(&handle, &proof_store).await,
                            &cancellation,
                        )?;
                    }
                    event => tracing::debug!(?event, "P2P event"),
                }
            }
            event = store_events.recv() => {
                match event {
                    Ok(ProofStoreEvent::ProofStored { proof_hash, .. }) => {
                        tolerate_shutdown(handle.announce(proof_hash).await, &cancellation)?;
                    }
                    Ok(ProofStoreEvent::ProofEvicted { proof_hash, .. }) => {
                        tolerate_shutdown(
                            handle.forget_local_proof(proof_hash).await,
                            &cancellation,
                        )?;
                    }
                    Ok(event) => tracing::debug!(?event, "proof store event"),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "proof store subscriber lagged; reconciling local proofs");
                        tolerate_shutdown(
                            reconcile_local_proofs(&handle, &proof_store).await,
                            &cancellation,
                        )?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err(Error::ProofStore("proof store event channel closed".to_owned()));
                    }
                }
            }
        }
    }
}

fn tolerate_shutdown(result: Result<()>, cancellation: &CancellationToken) -> Result<()> {
    match result {
        Err(_) if cancellation.is_cancelled() => Ok(()),
        result => result,
    }
}

async fn reconcile_local_proofs(handle: &P2pHandle, store: &ProofStore) -> Result<()> {
    let proofs = store.locally_available_proofs();
    let hashes = proofs
        .iter()
        .map(|proof| proof.hash)
        .collect::<HashSet<_>>();
    handle.replace_local_proofs(hashes).await?;
    for proof in proofs {
        handle.announce(proof.hash).await?;
    }
    Ok(())
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
    use std::{collections::HashSet, net::TcpListener, path::PathBuf, time::Duration};

    use bytes::Bytes;
    use libp2p::{Multiaddr, identity};
    use reqwest::Url;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::{P2pConfig, ProofStoreConfig, RuntimeConfig},
        control::request_shutdown,
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
    async fn p2p_bridge_serves_and_stores_chain_observed_proof() {
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
        let server_driver_task = tokio::spawn(server_driver.run(cancellation.child_token()));
        let server_bridge_task = tokio::spawn(run_p2p_events(
            server_handle,
            server_events,
            Arc::clone(&server_store),
            server_store.subscribe(),
            cancellation.child_token(),
        ));
        let client_driver_task = tokio::spawn(client_driver.run(cancellation.child_token()));
        let client_bridge_task = tokio::spawn(run_p2p_events(
            client_handle.clone(),
            client_events,
            Arc::clone(&client_store),
            client_store.subscribe(),
            cancellation.child_token(),
        ));
        timeout(Duration::from_secs(5), server_ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(5), client_ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

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

        cancellation.cancel();
        for task in [
            server_driver_task,
            server_bridge_task,
            client_driver_task,
            client_bridge_task,
        ] {
            timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
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
