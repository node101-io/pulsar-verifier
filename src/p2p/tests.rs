use std::{collections::HashSet, net::TcpListener, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use libp2p::{Multiaddr, PeerId, identity};
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

use super::{Driver, DriverClient, DriverEvent, DriverParts, Worker};
use crate::{
    Result,
    config::{P2pConfig, ProofStoreConfig, VerificationConfig},
    proof::{Proof, ProofType, VerificationId},
    store::{
        ProofSource, ProofStore, VerificationFailure, VerificationStatus, VerificationVerdict,
    },
    verification::{VerificationWorker, Verifier, VerifierRegistry},
};

const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

struct ValidVerifier;

#[async_trait]
impl Verifier for ValidVerifier {
    async fn verify(
        &self,
        _proof: &Proof,
    ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
        Ok(VerificationVerdict::Valid)
    }
}

fn proof(bytes: impl Into<Bytes>) -> Proof {
    Proof {
        proof_type: ProofType::MinaPickles,
        proof: bytes.into(),
        public_inputs: Bytes::from_static(b"inputs"),
        verification_key: Bytes::from_static(b"key"),
    }
}

fn verification_id(bytes: &'static [u8]) -> VerificationId {
    proof(Bytes::from_static(bytes)).verification_id()
}

struct TestNode {
    peer_id: PeerId,
    client: DriverClient,
    events: mpsc::Receiver<DriverEvent>,
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
        let task = tokio::spawn(driver.run());
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
        self.client.drain().await.unwrap();
        drop(self.client);
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
        max_availability_message_bytes: 64 * 1024,
        max_proof_bytes: 8 * 1024 * 1024,
        proof_request_timeout: Duration::from_secs(3),
        max_concurrent_retrievals: 16,
        retrieval_timeout: Duration::from_secs(10),
        retrieval_initial_backoff: Duration::from_millis(50),
        retrieval_max_backoff: Duration::from_millis(200),
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
async fn tcp_propagates_availability_and_transfers_complete_proof() {
    let (mut first, mut second) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;
    let proof = proof(Bytes::from_static(b"opaque-zk-proof"));
    let verification_id = proof.verification_id();

    second.client.announce(verification_id).await.unwrap();
    loop {
        if matches!(
            next_event(&mut first.events).await,
            DriverEvent::AvailabilityAnnounced { peer, verification_id: id }
                if peer == second.peer_id && id == verification_id
        ) {
            break;
        }
    }
    assert_eq!(
        first.client.providers(verification_id).await.unwrap(),
        vec![second.peer_id]
    );

    let query_id = first
        .client
        .query_availability(verification_id)
        .await
        .unwrap();
    loop {
        if let DriverEvent::ProvidersDiscovered {
            query_id: observed,
            verification_id: id,
            providers,
        } = next_event(&mut first.events).await
        {
            assert_eq!(observed, query_id);
            assert_eq!(id, verification_id);
            assert_eq!(providers, vec![second.peer_id]);
            break;
        }
    }

    let outbound_id = first
        .client
        .request_proof(second.peer_id, verification_id)
        .await
        .unwrap();
    let inbound_id = loop {
        if let DriverEvent::ProofRequested {
            request_id,
            peer,
            verification_id: id,
        } = next_event(&mut second.events).await
        {
            assert_eq!(peer, first.peer_id);
            assert_eq!(id, verification_id);
            break request_id;
        }
    };
    second
        .client
        .respond_proof(inbound_id, Some(proof.clone()))
        .await
        .unwrap();

    loop {
        if let DriverEvent::ProofReceived {
            request_id,
            peer,
            verification_id: received_id,
            proof: received,
        } = next_event(&mut first.events).await
        {
            assert_eq!(request_id, outbound_id);
            assert_eq!(peer, second.peer_id);
            assert_eq!(received_id, verification_id);
            assert_eq!(received, proof);
            break;
        }
    }

    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn proof_not_found_clears_duplicate_in_flight_request() {
    let (mut first, mut second) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;
    let missing_id = verification_id(b"missing-proof");
    let missing_request = first
        .client
        .request_proof(second.peer_id, missing_id)
        .await
        .unwrap();
    assert!(
        first
            .client
            .request_proof(second.peer_id, missing_id)
            .await
            .is_err()
    );
    let missing_inbound = loop {
        if let DriverEvent::ProofRequested {
            request_id,
            verification_id: id,
            ..
        } = next_event(&mut second.events).await
        {
            assert_eq!(id, missing_id);
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
            DriverEvent::ProofNotFound { request_id, verification_id: id, .. }
                if request_id == missing_request && id == missing_id
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
    let verification_id = verification_id(b"evicted-proof");

    first.client.announce(verification_id).await.unwrap();
    assert_eq!(
        first.client.providers(verification_id).await.unwrap(),
        vec![first.peer_id]
    );
    first
        .client
        .forget_local_proof(verification_id)
        .await
        .unwrap();
    assert!(
        first
            .client
            .providers(verification_id)
            .await
            .unwrap()
            .is_empty()
    );

    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn driver_rejects_new_work_after_drain_and_exits_when_mailbox_closes() {
    let identity = identity::Keypair::generate_ed25519();
    let peer_id = identity.public().to_peer_id();
    let node = TestNode::start(
        identity,
        HashSet::from([peer_id]),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;

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
            .query_availability(verification_id(b"proof"))
            .await,
        Err(crate::Error::P2pDraining)
    ));
    drop(node.client);
    timeout(EVENT_TIMEOUT, node.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn driver_treats_mailbox_closure_before_drain_as_fatal() {
    let identity = identity::Keypair::generate_ed25519();
    let peer_id = identity.public().to_peer_id();
    let node = TestNode::start(
        identity,
        HashSet::from([peer_id]),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;

    drop(node.client);
    let result = timeout(EVENT_TIMEOUT, node.task).await.unwrap().unwrap();
    assert!(matches!(result, Err(crate::Error::P2pDriverClosed)));
}

#[tokio::test]
async fn drain_waits_for_an_accepted_inbound_proof_response() {
    let (mut server, client) = connected_nodes("/ip4/127.0.0.1/tcp/0").await;
    let proof = proof(Bytes::from_static(b"accepted-before-drain"));
    let verification_id = proof.verification_id();

    client
        .client
        .request_proof(server.peer_id, verification_id)
        .await
        .unwrap();
    let request_id = loop {
        if let DriverEvent::ProofRequested {
            request_id,
            verification_id: requested,
            ..
        } = next_event(&mut server.events).await
        {
            assert_eq!(requested, verification_id);
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
        .respond_proof(request_id, Some(proof))
        .await
        .unwrap();
    timeout(EVENT_TIMEOUT, drain)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    drop(server.client);
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
    let verification_id = verification_id(b"outbound-before-drain");

    client
        .client
        .request_proof(server.peer_id, verification_id)
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
            DriverEvent::ProofNotFound { verification_id: missing, .. }
                if missing == verification_id
        ) {
            break;
        }
    }
    timeout(EVENT_TIMEOUT, drain)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    drop(client.client);
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
    let verification_id = verification_id(b"requested-after-drain");
    server.client.drain().await.unwrap();

    client
        .client
        .request_proof(server.peer_id, verification_id)
        .await
        .unwrap();
    loop {
        if matches!(
            next_event(&mut client.events).await,
            DriverEvent::ProofNotFound { verification_id: missing, .. }
                if missing == verification_id
        ) {
            break;
        }
    }
    assert!(
        timeout(Duration::from_millis(100), server.events.recv())
            .await
            .is_err()
    );

    drop(server.client);
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
    worker_stop: CancellationToken,
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
        let config = test_config(address);
        Self::start_with_config(identity, authorized, config, store).await
    }

    async fn start_with_config(
        identity: identity::Keypair,
        authorized: HashSet<PeerId>,
        config: P2pConfig,
        store: Arc<ProofStore>,
    ) -> Self {
        let peer_id = identity.public().to_peer_id();
        let DriverParts {
            driver,
            client,
            events,
            ready,
        } = Driver::build(config.clone(), identity, authorized).unwrap();
        let worker = Worker::new(client.clone(), events, store, peer_id, &config);
        let worker_stop = CancellationToken::new();
        let driver_task = tokio::spawn(driver.run());
        let worker_task = tokio::spawn(worker.run(worker_stop.clone()));
        timeout(EVENT_TIMEOUT, ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        Self {
            peer_id,
            client,
            worker_stop,
            driver_task,
            worker_task,
        }
    }

    async fn stop(self) {
        self.client.drain().await.unwrap();
        self.worker_stop.cancel();
        timeout(EVENT_TIMEOUT, self.worker_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(self.client);
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
    let proof = proof(Bytes::from_static(b"locally-available-proof"));
    let verification_id = proof.verification_id();

    store
        .insert_local_proof(proof, ProofSource::Rpc)
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if node.client.providers(verification_id).await.unwrap() == vec![node.peer_id] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    store.invalidate(verification_id).await;
    timeout(Duration::from_secs(2), async {
        loop {
            if node
                .client
                .providers(verification_id)
                .await
                .unwrap()
                .is_empty()
            {
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
    let proof = proof(Bytes::from_static(b"store-backed-proof"));
    let verification_id = proof.verification_id();
    let server = StoreBackedNode::start(
        server_identity,
        authorized.clone(),
        server_address.clone(),
        Arc::clone(&server_store),
    )
    .await;
    let client = StoreBackedNode::start(
        client_identity,
        authorized,
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
        Arc::clone(&client_store),
    )
    .await;
    let verification_stop = CancellationToken::new();
    let verification_worker = VerificationWorker::new(
        Arc::clone(&client_store),
        VerifierRegistry::new([(
            ProofType::MinaPickles,
            Arc::new(ValidVerifier) as Arc<dyn Verifier>,
        )])
        .unwrap(),
        VerificationConfig {
            max_concurrent_jobs: 1,
            job_timeout: Duration::from_secs(1),
            max_retries: 0,
            retry_backoff: Duration::from_millis(10),
        },
    );
    let verification_task = tokio::spawn(verification_worker.run(verification_stop.clone()));

    client
        .client
        .dial(server_peer, vec![server_address])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    server_store
        .insert_local_proof(proof.clone(), ProofSource::Rpc)
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if client.client.providers(verification_id).await.unwrap() == vec![server_peer] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(client_store.get_proof(verification_id).await.is_none());

    client_store
        .observe_chain_verification(verification_id)
        .await
        .unwrap();

    timeout(Duration::from_secs(5), async {
        loop {
            if client_store
                .get_proof(verification_id)
                .await
                .is_some_and(|received| received == proof)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if client_store.statuses(&[verification_id]).await[0].status
                == VerificationStatus::Completed(VerificationVerdict::Valid)
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
    verification_stop.cancel();
    verification_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_falls_back_after_not_found_provider() {
    let liar_identity = identity::Keypair::generate_ed25519();
    let server_identity = identity::Keypair::generate_ed25519();
    let client_identity = identity::Keypair::generate_ed25519();
    let liar_peer = liar_identity.public().to_peer_id();
    let server_peer = server_identity.public().to_peer_id();
    let client_peer = client_identity.public().to_peer_id();
    let authorized = HashSet::from([liar_peer, server_peer, client_peer]);
    let server_address = free_tcp_address();

    let mut liar = TestNode::start(
        liar_identity,
        authorized.clone(),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;
    let server_store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
    let server = StoreBackedNode::start(
        server_identity,
        authorized.clone(),
        server_address.clone(),
        Arc::clone(&server_store),
    )
    .await;
    let client_store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
    let client = StoreBackedNode::start(
        client_identity,
        authorized,
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
        Arc::clone(&client_store),
    )
    .await;

    client
        .client
        .dial(liar_peer, vec![liar.listen_address.clone()])
        .await
        .unwrap();
    client
        .client
        .dial(server_peer, vec![server_address])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let proof = proof(Bytes::from_static(b"fallback-proof"));
    let verification_id = proof.verification_id();
    liar.client.announce(verification_id).await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if client.client.providers(verification_id).await.unwrap() == vec![liar_peer] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    client_store
        .observe_chain_verification(verification_id)
        .await
        .unwrap();
    let request_id = loop {
        if let DriverEvent::ProofRequested {
            request_id,
            verification_id: requested,
            ..
        } = next_event(&mut liar.events).await
        {
            assert_eq!(requested, verification_id);
            break request_id;
        }
    };
    liar.client.respond_proof(request_id, None).await.unwrap();
    server_store
        .insert_local_proof(proof.clone(), ProofSource::Rpc)
        .await
        .unwrap();

    timeout(Duration::from_secs(5), async {
        loop {
            if client_store
                .get_proof(verification_id)
                .await
                .is_some_and(|received| received == proof)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    liar.stop().await;
    server.stop().await;
    client.stop().await;
}

#[tokio::test]
async fn new_announcement_restarts_retrieval_after_deadline() {
    let server_identity = identity::Keypair::generate_ed25519();
    let client_identity = identity::Keypair::generate_ed25519();
    let server_peer = server_identity.public().to_peer_id();
    let client_peer = client_identity.public().to_peer_id();
    let authorized = HashSet::from([server_peer, client_peer]);
    let client_store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
    let proof = proof(Bytes::from_static(b"late-provider-proof"));
    let verification_id = proof.verification_id();
    client_store
        .observe_chain_verification(verification_id)
        .await
        .unwrap();

    let mut client_config = test_config("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    client_config.proof_request_timeout = Duration::from_millis(100);
    client_config.retrieval_timeout = Duration::from_millis(300);
    client_config.retrieval_initial_backoff = Duration::from_millis(10);
    client_config.retrieval_max_backoff = Duration::from_millis(50);
    let client = StoreBackedNode::start_with_config(
        client_identity,
        authorized.clone(),
        client_config,
        Arc::clone(&client_store),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_address = free_tcp_address();
    let server_store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
    let server = StoreBackedNode::start(
        server_identity,
        authorized,
        server_address.clone(),
        Arc::clone(&server_store),
    )
    .await;
    client
        .client
        .dial(server_peer, vec![server_address])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    server_store
        .insert_local_proof(proof.clone(), ProofSource::Rpc)
        .await
        .unwrap();

    timeout(Duration::from_secs(5), async {
        loop {
            if client_store
                .get_proof(verification_id)
                .await
                .is_some_and(|received| received == proof)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    client.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn worker_limits_two_hundred_fifty_six_retrievals_to_sixteen_requests() {
    const PROOF_COUNT: usize = 256;
    const CONCURRENCY: usize = 16;

    let server_identity = identity::Keypair::generate_ed25519();
    let client_identity = identity::Keypair::generate_ed25519();
    let server_peer = server_identity.public().to_peer_id();
    let client_peer = client_identity.public().to_peer_id();
    let authorized = HashSet::from([server_peer, client_peer]);
    let mut server = TestNode::start(
        server_identity,
        authorized.clone(),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
    )
    .await;
    let client_store = Arc::new(ProofStore::new(ProofStoreConfig::test_default()).unwrap());
    let mut config = test_config("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    config.max_concurrent_retrievals = CONCURRENCY;
    config.retrieval_timeout = Duration::from_secs(120);
    let client = StoreBackedNode::start_with_config(
        client_identity,
        authorized,
        config,
        Arc::clone(&client_store),
    )
    .await;
    client
        .client
        .dial(server_peer, vec![server.listen_address.clone()])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let verification_ids = (0..PROOF_COUNT)
        .map(|index| proof(format!("bounded-retrieval-{index}")).verification_id())
        .collect::<Vec<_>>();
    for verification_id in &verification_ids {
        server.client.announce(*verification_id).await.unwrap();
    }
    timeout(Duration::from_secs(10), async {
        loop {
            let mut all_known = true;
            for verification_id in &verification_ids {
                if client.client.providers(*verification_id).await.unwrap() != vec![server_peer] {
                    all_known = false;
                    break;
                }
            }
            if all_known {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    for verification_id in &verification_ids {
        client_store
            .observe_chain_verification(*verification_id)
            .await
            .unwrap();
    }

    let mut requests = Vec::with_capacity(PROOF_COUNT);
    while requests.len() < CONCURRENCY {
        if let DriverEvent::ProofRequested { request_id, .. } = next_event(&mut server.events).await
        {
            requests.push(request_id);
        }
    }
    let seventeenth = timeout(Duration::from_millis(200), async {
        loop {
            if let DriverEvent::ProofRequested { request_id, .. } =
                next_event(&mut server.events).await
            {
                return request_id;
            }
        }
    })
    .await;
    assert!(seventeenth.is_err());

    let mut completed = 0;
    while completed < PROOF_COUNT {
        for request_id in requests.drain(..) {
            server.client.respond_proof(request_id, None).await.unwrap();
            completed += 1;
        }
        while completed + requests.len() < PROOF_COUNT && requests.len() < CONCURRENCY {
            if let DriverEvent::ProofRequested { request_id, .. } =
                next_event(&mut server.events).await
            {
                requests.push(request_id);
            }
        }
    }

    client.stop().await;
    server.stop().await;
}

fn free_tcp_address() -> Multiaddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap()
}
