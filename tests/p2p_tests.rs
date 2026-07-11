use std::{collections::HashSet, path::PathBuf, time::Duration};

use libp2p::{Multiaddr, PeerId, identity};
use pulsar_verifier::{
    config::P2pConfig,
    p2p::{P2pDriver, P2pEvent, P2pHandle, ProofContent, ProofHash},
};
use reqwest::Url;
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

struct TestNode {
    peer_id: PeerId,
    handle: P2pHandle,
    events: mpsc::Receiver<P2pEvent>,
    cancellation: CancellationToken,
    task: JoinHandle<pulsar_verifier::Result<()>>,
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
        let (driver, handle, mut events, ready) =
            P2pDriver::new(config, identity, authorized).unwrap();
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(driver.run(cancellation.clone()));
        timeout(EVENT_TIMEOUT, ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let listen_address = loop {
            if let P2pEvent::Listening { address } = next_event(&mut events).await {
                break address;
            }
        };

        Self {
            peer_id,
            handle,
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
                P2pEvent::PeerConnected { peer } if peer == expected
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

async fn next_event(events: &mut mpsc::Receiver<P2pEvent>) -> P2pEvent {
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
        .handle
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

    second.handle.announce(proof_hash).await.unwrap();
    loop {
        if matches!(
            next_event(&mut first.events).await,
            P2pEvent::AvailabilityAnnounced { peer, proof_hash: hash }
                if peer == second.peer_id && hash == proof_hash
        ) {
            break;
        }
    }
    assert_eq!(
        first.handle.providers(proof_hash).await.unwrap(),
        vec![second.peer_id]
    );

    let query_id = first.handle.query_availability(proof_hash).await.unwrap();
    loop {
        if let P2pEvent::ProvidersDiscovered {
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
        .handle
        .request_proof(second.peer_id, proof_hash)
        .await
        .unwrap();
    let inbound_id = loop {
        if let P2pEvent::ProofRequested {
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
        .handle
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
        if let P2pEvent::ProofReceived {
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
        .handle
        .request_proof(second.peer_id, missing_hash)
        .await
        .unwrap();
    assert!(
        first
            .handle
            .request_proof(second.peer_id, missing_hash)
            .await
            .is_err()
    );
    let missing_inbound = loop {
        if let P2pEvent::ProofRequested {
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
        .handle
        .respond_proof(missing_inbound, None)
        .await
        .unwrap();
    loop {
        if matches!(
            next_event(&mut first.events).await,
            P2pEvent::ProofNotFound { request_id, proof_hash: hash, .. }
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
        .handle
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
        .handle
        .replace_authorized_peers(HashSet::from([first.peer_id]))
        .await
        .unwrap();
    loop {
        if matches!(
            next_event(&mut first.events).await,
            P2pEvent::PeerDisconnected { peer } if peer == second.peer_id
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
        .handle
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

    first.handle.announce(proof_hash).await.unwrap();
    assert_eq!(
        first.handle.providers(proof_hash).await.unwrap(),
        vec![first.peer_id]
    );
    first.handle.forget_local_proof(proof_hash).await.unwrap();
    assert!(first.handle.providers(proof_hash).await.unwrap().is_empty());

    first.stop().await;
    second.stop().await;
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
        .handle
        .dial(first_peer, vec![first.listen_address.clone()])
        .await
        .unwrap();
    let connected = timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                first.events.recv().await,
                Some(P2pEvent::PeerConnected { peer }) if peer == second_peer
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
