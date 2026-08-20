use std::{sync::Arc, time::Instant};

use crate::proof::{Proof, VerificationId};

use super::event::ProofSource;

/// Internal verification lifecycle; availability is derived from record content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationState {
    NotStarted,
    Verifying,
    Verified,
    Wrong,
}

#[derive(Clone, Debug)]
pub struct ProofMetadata {
    pub chain_observed_at: Option<Instant>,
    pub content_source: Option<ProofSource>,
    pub content_stored_at: Option<Instant>,
    pub verification: VerificationState,
    pub completed_at: Option<Instant>,
}

/// Immutable cache value replaced atomically for every lifecycle transition.
#[derive(Clone, Debug)]
pub struct ProofRecord {
    pub metadata: ProofMetadata,
    pub proof: Option<Proof>,
}

#[derive(Clone, Debug)]
pub struct StoredProof {
    pub verification_id: VerificationId,
    pub record: Arc<ProofRecord>,
}

#[derive(Clone, Debug)]
pub struct VerificationJob {
    pub verification_id: VerificationId,
    pub proof: Proof,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationResult {
    Verified,
    Wrong,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainProofStatus {
    Verified,
    Wrong,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreChange {
    Inserted,
    Updated,
    Unchanged,
}
