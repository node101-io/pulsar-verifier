use libp2p::PeerId;
use tokio::sync::broadcast;

use crate::proof::VerificationId;

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
    VerificationObserved {
        verification_id: VerificationId,
    },
    ProofStored {
        verification_id: VerificationId,
        source: ProofSource,
    },
    VerificationChanged {
        verification_id: VerificationId,
        state: VerificationState,
    },
    ProofEvicted {
        verification_id: VerificationId,
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

    pub(crate) fn try_recv(
        &mut self,
    ) -> std::result::Result<ProofStoreEvent, broadcast::error::TryRecvError> {
        self.receiver.try_recv()
    }
}
