use libp2p::{Multiaddr, PeerId};

use crate::proof::{ProofContent, ProofHash};

pub const QUERY_ID_LEN: usize = 16;

/// Correlates one broadcast availability query with provider responses.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryId(pub(crate) [u8; QUERY_ID_LEN]);

impl QueryId {
    pub(crate) fn random() -> Self {
        Self(rand::random())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; QUERY_ID_LEN] {
        &self.0
    }
}

/// Stable application ID independent of libp2p's outbound request IDs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProofRequestId(pub(crate) u64);

/// Opaque token used to answer one inbound proof request exactly once.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InboundProofRequestId(pub(crate) u64);

#[derive(Debug)]
pub enum P2pEvent {
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
