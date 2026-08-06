use libp2p::{Multiaddr, PeerId};

use crate::proof::{ProofContent, ProofHash};

pub(super) const QUERY_ID_LEN: usize = 16;

/// Correlates one broadcast availability query with provider responses.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct QueryId(pub(super) [u8; QUERY_ID_LEN]);

impl QueryId {
    pub(super) fn random() -> Self {
        Self(rand::random())
    }

    #[must_use]
    pub(super) const fn as_bytes(&self) -> &[u8; QUERY_ID_LEN] {
        &self.0
    }
}

/// Stable application ID independent of libp2p's outbound request IDs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct ProofRequestId(pub(super) u64);

/// Opaque token used to answer one inbound proof request exactly once.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct InboundProofRequestId(pub(super) u64);

#[derive(Debug)]
#[allow(dead_code)]
pub(super) enum DriverEvent {
    Listening {
        address: Multiaddr,
    },
    PeerConnected {
        peer: PeerId,
    },
    PeerDisconnected {
        peer: PeerId,
    },
    AvailabilityAnnounced {
        peer: PeerId,
        proof_hash: ProofHash,
    },
    ProvidersDiscovered {
        query_id: QueryId,
        proof_hash: ProofHash,
        providers: Vec<PeerId>,
    },
    ProofRequested {
        request_id: InboundProofRequestId,
        peer: PeerId,
        proof_hash: ProofHash,
    },
    ProofReceived {
        request_id: ProofRequestId,
        peer: PeerId,
        content: ProofContent,
    },
    ProofNotFound {
        request_id: ProofRequestId,
        peer: PeerId,
        proof_hash: ProofHash,
    },
    ProofRequestFailed {
        request_id: ProofRequestId,
        peer: PeerId,
        reason: String,
    },
}
