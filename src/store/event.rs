use libp2p::PeerId;
use tokio::sync::broadcast;

use crate::proof::ProofHash;

use super::record::VerificationState;

/// Trusted origin of locally stored proof bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofSource {
    Rpc,
    Peer(PeerId),
}

/// Reason a complete record left the process-local cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofEvictionCause {
    Expired,
    Explicit,
    Size,
}

/// Committed storage facts consumed by independent runtime components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProofStoreEvent {
    ChainProofObserved {
        proof_hash: ProofHash,
    },
    ProofStored {
        proof_hash: ProofHash,
        source: ProofSource,
    },
    VerificationChanged {
        proof_hash: ProofHash,
        state: VerificationState,
    },
    ProofEvicted {
        proof_hash: ProofHash,
        cause: ProofEvictionCause,
    },
}

/// Bounded event receiver; lagged consumers must rebuild from store snapshots.
pub struct ProofStoreSubscription {
    pub(crate) receiver: broadcast::Receiver<ProofStoreEvent>,
}

impl ProofStoreSubscription {
    /// Waits for the next committed store transition.
    ///
    /// # Errors
    ///
    /// Returns `Lagged` when reconciliation is required or `Closed` when the
    /// store event publisher has been dropped.
    pub async fn recv(
        &mut self,
    ) -> std::result::Result<ProofStoreEvent, broadcast::error::RecvError> {
        self.receiver.recv().await
    }
}
