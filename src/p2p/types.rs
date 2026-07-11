use std::fmt;

use libp2p::{Multiaddr, PeerId};

use crate::{Error, Result};

pub const PROOF_HASH_LEN: usize = 32;
pub const QUERY_ID_LEN: usize = 16;

/// Canonical BLAKE3 identifier used by all P2P protocol boundaries.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProofHash([u8; PROOF_HASH_LEN]);

impl ProofHash {
    #[must_use]
    pub fn digest(proof: &[u8]) -> Self {
        Self(*blake3::hash(proof).as_bytes())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; PROOF_HASH_LEN] {
        &self.0
    }
}

impl TryFrom<&[u8]> for ProofHash {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        let bytes = value.try_into().map_err(|_| {
            Error::P2pProtocol(format!(
                "proof hash must be {PROOF_HASH_LEN} bytes, got {}",
                value.len()
            ))
        })?;
        Ok(Self(bytes))
    }
}

impl fmt::Debug for ProofHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ProofHash({})",
            blake3::Hash::from_bytes(self.0).to_hex()
        )
    }
}

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

/// Opaque proof content; proof-system metadata intentionally remains external.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProofContent {
    pub proof_hash: ProofHash,
    pub proof: Vec<u8>,
}

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
