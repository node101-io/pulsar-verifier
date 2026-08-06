use std::{collections::HashSet, net::TcpListener, path::PathBuf, sync::Arc, time::Duration};

use bytes::Bytes;
use libp2p::{Multiaddr, PeerId, identity};
use reqwest::Url;
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

use super::{Driver, DriverClient, DriverEvent, DriverParts, Worker, WorkerHandle};
use crate::{
    Result,
    config::{P2pConfig, ProofStoreConfig},
    proof::{ProofContent, ProofHash, ProofType},
    store::{ProofSource, ProofStore},
};

const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

struct TestNode {
    peer_id: PeerId,
    client: DriverClient,
    events: mpsc::Receiver<DriverEvent>,
    cancellation: CancellationToken,
    task: JoinHandle<Result<()>>,
    listen_address: Multiaddr,
}

impl TestNode {
    async fn start(
        identity: identity::Keypair,
        authorized: HashSet<PeerId>,
        listen_address: Multiaddr,
    ) -> Self {
        let peer_id = identity.public().to_peer_id();
        let config = test_config(listen_address);
        let DriverParts {
            driver,
            client,
            mut events,
            ready,
        } = Driver::build(config, identity, authorized).unwrap();
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(driver.run(cancellation.clone()));
        timeout(EVENT_TIMEOUT, ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let listen_address = loop {
            if let DriverEvent::Listening { address } = next_event(&mut events).await {
                break address;
            }
        };

        Self {
            peer_id,
            client,
            events,
            cancellation,
            task,
            listen_address,
        }
    }

    async fn wait_connected(&mut self, expected: PeerId) {
        loop {
            if matches!(
                next_event(&mut self.events).await,
                DriverEvent::PeerConnected { peer } if peer == expected
            ) {
                return;
            }
        }
    }

    async fn stop(self) {
        self.cancellation.cancel();
        timeout(EVENT_TIMEOUT, self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

fn test_config(listen_address: Multiaddr) -> P2pConfig {
    P2pConfig {
        enabled: true,
        chain_id: "pulsar-p2p-test".to_owned(),
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

async fn next_event(events: &mut mpsc::Receiver<DriverEvent>) -> DriverEvent {
    timeout(EVENT_TIMEOUT, events.recv())
        .await
        .expect("timed out waiting for P2P event")
        .expect("P2P event channel closed")
}

async fn connected_nodes(transport: &str) -> (TestNode, TestNode) {
    let first_identity = identity::Keypair::generate_ed25519();
    let second_identity = identity::Keypair::generate_ed25519();
    let first_peer = first_identity.public().to_peer_id();
    let second_peer = second_identity.public().to_peer_id();
    let authorized = HashSet::from([first_peer, second_peer]);
    let first_listen = transport.parse().unwrap();
    let second_listen = transport.parse().unwrap();
    let mut first = TestNode::start(first_identity, authorized.clone(), first_listen).await;
    let mut second = TestNode::start(second_identity, authorized, second_listen).await;

    first
        .client
        .dial(second.peer_id, vec![second.listen_address.clone()])
        .await
        .unwrap();
    first.wait_connected(second.peer_id).await;
    second.wait_connected(first.peer_id).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    (first, second)
}

#[tokio::test]
async fn tcp_propagates_availability_and_transfers_opaque_proof() {
    let (mut first, mut second) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;
    let proof = b"opaque-zk-proof".to_vec();
    let proof_hash = ProofHash::digest(&proof);

    second.client.announce(proof_hash).await.unwrap();
    loop {
        if matches!(
            next_event(&mut first.events).await,
            DriverEvent::AvailabilityAnnounced { peer, proof_hash: hash }
                if peer == second.peer_id && hash == proof_hash
        ) {
            break;
        }
    }
    assert_eq!(
        first.client.providers(proof_hash).await.unwrap(),
        vec![second.peer_id]
    );

    let query_id = first.client.query_availability(proof_hash).await.unwrap();
    loop {
        if let DriverEvent::ProvidersDiscovered {
            query_id: observed,
            proof_hash: hash,
            providers,
        } = next_event(&mut first.events).await
        {
            assert_eq!(observed, query_id);
            assert_eq!(hash, proof_hash);
            assert_eq!(providers, vec![second.peer_id]);
            break;
        }
    }

    let outbound_id = first
        .client
        .request_proof(second.peer_id, proof_hash)
        .await
        .unwrap();
    let inbound_id = loop {
        if let DriverEvent::ProofRequested {
            request_id,
            peer,
            proof_hash: hash,
        } = next_event(&mut second.events).await
        {
            assert_eq!(peer, first.peer_id);
            assert_eq!(hash, proof_hash);
            break request_id;
        }
    };
    second
        .client
        .respond_proof(
            inbound_id,
            Some(ProofContent {
                proof_hash,
                proof: proof.clone().into(),
            }),
        )
        .await
        .unwrap();

    loop {
        if let DriverEvent::ProofReceived {
            request_id,
            peer,
            content,
        } = next_event(&mut first.events).await
        {
            assert_eq!(request_id, outbound_id);
            assert_eq!(peer, second.peer_id);
            assert_eq!(content.proof, proof);
            break;
        }
    }

    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn proof_not_found_clears_duplicate_in_flight_request() {
    let (mut first, mut second) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;
    let missing_hash = ProofHash::digest(b"missing-proof");
    let missing_request = first
        .client
        .request_proof(second.peer_id, missing_hash)
        .await
        .unwrap();
    assert!(
        first
            .client
            .request_proof(second.peer_id, missing_hash)
            .await
            .is_err()
    );
    let missing_inbound = loop {
        if let DriverEvent::ProofRequested {
            request_id,
            proof_hash: hash,
            ..
        } = next_event(&mut second.events).await
        {
            assert_eq!(hash, missing_hash);
            break request_id;
        }
    };
    second
        .client
        .respond_proof(missing_inbound, None)
        .await
        .unwrap();
    loop {
        if matches!(
            next_event(&mut first.events).await,
            DriverEvent::ProofNotFound { request_id, proof_hash: hash, .. }
                if request_id == missing_request && hash == missing_hash
        ) {
            break;
        }
    }

    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn quic_connects_authorized_validators() {
    let (first, second) = connected_nodes("/ip4/127.0.0.1/udp/0/quic-v1").await;

    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn bad_quic_address_falls_back_to_tcp() {
    let first_identity = identity::Keypair::generate_ed25519();
    let second_identity = identity::Keypair::generate_ed25519();
    let first_peer = first_identity.public().to_peer_id();
    let second_peer = second_identity.public().to_peer_id();
    let authorized = HashSet::from([first_peer, second_peer]);
    let mut first = TestNode::start(
        first_identity,
        authorized.clone(),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;
    let mut second = TestNode::start(
        second_identity,
        authorized,
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;

    first
        .client
        .dial(
            second_peer,
            vec![
                "/ip4/127.0.0.1/udp/9/quic-v1".parse().unwrap(),
                second.listen_address.clone(),
            ],
        )
        .await
        .unwrap();
    first.wait_connected(second_peer).await;
    second.wait_connected(first_peer).await;

    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn replacing_allow_list_disconnects_removed_validator() {
    let (mut first, second) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;

    first
        .client
        .replace_authorized_peers(HashSet::from([first.peer_id]))
        .await
        .unwrap();
    loop {
        if matches!(
            next_event(&mut first.events).await,
            DriverEvent::PeerDisconnected { peer } if peer == second.peer_id
        ) {
            break;
        }
    }

    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn unauthorized_outbound_dial_is_rejected_before_transport() {
    let identity = identity::Keypair::generate_ed25519();
    let peer_id = identity.public().to_peer_id();
    let node = TestNode::start(
        identity,
        HashSet::from([peer_id]),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;

    let result = node
        .client
        .dial(
            PeerId::random(),
            vec!["/ip4/127.0.0.1/tcp/9".parse().unwrap()],
        )
        .await;
    assert!(result.is_err());

    node.stop().await;
}

#[tokio::test]
async fn forgetting_local_proof_removes_it_from_provider_queries() {
    let (first, second) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;
    let proof_hash = ProofHash::digest(b"evicted-proof");

    first.client.announce(proof_hash).await.unwrap();
    assert_eq!(
        first.client.providers(proof_hash).await.unwrap(),
        vec![first.peer_id]
    );
    first.client.forget_local_proof(proof_hash).await.unwrap();
    assert!(first.client.providers(proof_hash).await.unwrap().is_empty());

    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn driver_requires_drain_and_rejects_new_work_afterward() {
    let identity = identity::Keypair::generate_ed25519();
    let peer_id = identity.public().to_peer_id();
    let node = TestNode::start(
        identity,
        HashSet::from([peer_id]),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;

    assert!(matches!(
        node.client.shutdown().await,
        Err(crate::Error::P2pNotDrained)
    ));
    node.client.drain().await.unwrap();
    assert!(matches!(
        node.client
            .dial(
                PeerId::random(),
                vec!["/ip4/127.0.0.1/tcp/9".parse().unwrap()],
            )
            .await,
        Err(crate::Error::P2pDraining)
    ));
    assert!(matches!(
        node.client
            .query_availability(ProofHash::digest(b"proof"))
            .await,
        Err(crate::Error::P2pDraining)
    ));
    node.client.shutdown().await.unwrap();
    timeout(EVENT_TIMEOUT, node.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn drain_waits_for_an_accepted_inbound_proof_response() {
    let (mut server, client) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;
    let proof = b"accepted-before-drain".to_vec();
    let proof_hash = ProofHash::digest(&proof);

    client
        .client
        .request_proof(server.peer_id, proof_hash)
        .await
        .unwrap();
    let request_id = loop {
        if let DriverEvent::ProofRequested {
            request_id,
            proof_hash: requested,
            ..
        } = next_event(&mut server.events).await
        {
            assert_eq!(requested, proof_hash);
            break request_id;
        }
    };

    let drain_client = server.client.clone();
    let mut drain = tokio::spawn(async move { drain_client.drain().await });
    assert!(
        timeout(Duration::from_millis(100), &mut drain)
            .await
            .is_err()
    );
    server
        .client
        .respond_proof(
            request_id,
            Some(ProofContent {
                proof_hash,
                proof: proof.into(),
            }),
        )
        .await
        .unwrap();
    timeout(EVENT_TIMEOUT, drain)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    server.client.shutdown().await.unwrap();
    timeout(EVENT_TIMEOUT, server.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    client.stop().await;
}

#[tokio::test]
async fn drain_waits_for_an_accepted_outbound_proof_response() {
    let (mut server, mut client) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;
    let proof_hash = ProofHash::digest(b"outbound-before-drain");

    client
        .client
        .request_proof(server.peer_id, proof_hash)
        .await
        .unwrap();
    let request_id = loop {
        if let DriverEvent::ProofRequested { request_id, .. } = next_event(&mut server.events).await
        {
            break request_id;
        }
    };
    let drain_client = client.client.clone();
    let mut drain = tokio::spawn(async move { drain_client.drain().await });
    assert!(
        timeout(Duration::from_millis(100), &mut drain)
            .await
            .is_err()
    );

    server.client.respond_proof(request_id, None).await.unwrap();
    loop {
        if matches!(
            next_event(&mut client.events).await,
            DriverEvent::ProofNotFound { proof_hash: missing, .. } if missing == proof_hash
        ) {
            break;
        }
    }
    timeout(EVENT_TIMEOUT, drain)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    client.client.shutdown().await.unwrap();
    timeout(EVENT_TIMEOUT, client.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.stop().await;
}

#[tokio::test]
async fn inbound_request_after_drain_returns_not_found_without_application_event() {
    let (mut server, mut client) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;
    let proof_hash = ProofHash::digest(b"requested-after-drain");
    server.client.drain().await.unwrap();

    client
        .client
        .request_proof(server.peer_id, proof_hash)
        .await
        .unwrap();
    loop {
        if matches!(
            next_event(&mut client.events).await,
            DriverEvent::ProofNotFound { proof_hash: missing, .. } if missing == proof_hash
        ) {
            break;
        }
    }
    assert!(
        timeout(Duration::from_millis(100), server.events.recv())
            .await
            .is_err()
    );

    server.client.shutdown().await.unwrap();
    timeout(EVENT_TIMEOUT, server.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    client.stop().await;
}

#[tokio::test]
async fn unauthorized_inbound_peer_never_reaches_protocol_events() {
    let first_identity = identity::Keypair::generate_ed25519();
    let second_identity = identity::Keypair::generate_ed25519();
    let first_peer = first_identity.public().to_peer_id();
    let second_peer = second_identity.public().to_peer_id();
    let mut first = TestNode::start(
        first_identity,
        HashSet::from([first_peer]),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;
    let second = TestNode::start(
        second_identity,
        HashSet::from([first_peer, second_peer]),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;

    second
        .client
        .dial(first_peer, vec![first.listen_address.clone()])
        .await
        .unwrap();
    let connected = timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                first.events.recv().await,
                Some(DriverEvent::PeerConnected { peer }) if peer == second_peer
            ) {
                return true;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(!connected);

    first.stop().await;
    second.stop().await;
}

struct StoreBackedNode {
    peer_id: PeerId,
    client: DriverClient,
    worker: WorkerHandle,
    driver_task: JoinHandle<Result<()>>,
    worker_task: JoinHandle<Result<()>>,
}

impl StoreBackedNode {
    async fn start(
        identity: identity::Keypair,
        authorized: HashSet<PeerId>,
        address: Multiaddr,
        store: Arc<ProofStore>,
    ) -> Self {
        let peer_id = identity.public().to_peer_id();
        let config = test_config(address);
        let DriverParts {
            driver,
            client,
            events,
            ready,
        } = Driver::build(config.clone(), identity, authorized).unwrap();
        let (worker, worker_handle) =
            Worker::new(client.clone(), events, store, config.event_buffer);
        let cancellation = CancellationToken::new();
        let driver_task = tokio::spawn(driver.run(cancellation.child_token()));
        let worker_task = tokio::spawn(worker.run(cancellation.child_token()));
        timeout(EVENT_TIMEOUT, ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        Self {
            peer_id,
            client,
            worker: worker_handle,
            driver_task,
            worker_task,
        }
    }

    async fn stop(self) {
        self.client.drain().await.unwrap();
        self.worker.shutdown().await.unwrap();
        timeout(EVENT_TIMEOUT, self.worker_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.client.shutdown().await.unwrap();
        timeout(EVENT_TIMEOUT, self.driver_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn worker_reconciles_store_ownership_with_availability() {
    let identity = identity::Keypair::generate_ed25519();
    let peer_id = identity.public().to_peer_id();
    let store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
    let node = StoreBackedNode::start(
        identity,
        HashSet::from([peer_id]),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
        Arc::clone(&store),
    )
    .await;
    let proof = Bytes::from_static(b"locally-available-proof");
    let proof_hash = ProofHash::digest(&proof);

    store
        .insert_local_proof(
            proof_hash,
            ProofType::new("mock").unwrap(),
            proof,
            ProofSource::Rpc,
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if node.client.providers(proof_hash).await.unwrap() == vec![node.peer_id] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    store.invalidate(proof_hash).await;
    timeout(Duration::from_secs(2), async {
        loop {
            if node.client.providers(proof_hash).await.unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    node.stop().await;
}

#[tokio::test]
async fn worker_serves_and_stores_chain_observed_proof() {
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

    let server = StoreBackedNode::start(
        server_identity,
        authorized.clone(),
        server_address.clone(),
        server_store,
    )
    .await;
    let client = StoreBackedNode::start(
        client_identity,
        authorized,
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
        Arc::clone(&client_store),
    )
    .await;

    client
        .client
        .dial(server_peer, vec![server_address])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    client
        .client
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

    server.stop().await;
    client.stop().await;
}

fn free_tcp_address() -> Multiaddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap()
}
