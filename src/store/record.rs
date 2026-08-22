use std::{sync::Arc, time::Instant};

use crate::proof::{Proof, VerificationId};

use super::event::ProofSource;

use crate::{Error, Result};

pub const FAILURE_CODE_MAX_BYTES: usize = 64;
pub const FAILURE_MESSAGE_MAX_BYTES: usize = 256;

/// Completed cryptographic verdict. Operational failures are represented separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationVerdict {
    Valid,
    Invalid,
}

/// Stable local diagnostic for a verifier backend or runtime failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationFailure {
    code: String,
    message: String,
    retryable: bool,
}

impl VerificationFailure {
    /// Validates the chain-facing failure bounds and machine-readable code format.
    ///
    /// # Errors
    ///
    /// Returns an error when the code format or either wire-size bound is invalid.
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Result<Self> {
        let code = code.into();
        let message = message.into();
        if code.is_empty() || code.len() > FAILURE_CODE_MAX_BYTES {
            return Err(Error::InvalidVerificationFailure(format!(
                "code must contain 1 to {FAILURE_CODE_MAX_BYTES} bytes"
            )));
        }
        if !code.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || (index > 0 && (byte.is_ascii_digit() || byte == b'_'))
        }) {
            return Err(Error::InvalidVerificationFailure(
                "code must start with a lowercase ASCII letter and contain only lowercase ASCII letters, digits, or underscores"
                    .to_owned(),
            ));
        }
        if message.len() > FAILURE_MESSAGE_MAX_BYTES {
            return Err(Error::InvalidVerificationFailure(format!(
                "message must contain at most {FAILURE_MESSAGE_MAX_BYTES} bytes"
            )));
        }
        Ok(Self {
            code,
            message,
            retryable,
        })
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

/// Internal verification lifecycle; unavailable is derived from missing prerequisites.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VerificationState {
    Queued,
    Verifying,
    Completed(VerificationVerdict),
    Failed(VerificationFailure),
}

#[derive(Clone, Debug)]
pub struct ProofMetadata {
    pub chain_observed_at: Option<Instant>,
    pub content_source: Option<ProofSource>,
    pub content_stored_at: Option<Instant>,
    pub verification: Option<VerificationState>,
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
    pub(crate) claim: Arc<ProofRecord>,
}

#[cfg(test)]
impl VerificationJob {
    pub(crate) fn detached(verification_id: VerificationId, proof: Proof) -> Self {
        Self {
            verification_id,
            proof: proof.clone(),
            claim: Arc::new(ProofRecord {
                metadata: ProofMetadata {
                    chain_observed_at: None,
                    content_source: None,
                    content_stored_at: None,
                    verification: Some(VerificationState::Verifying),
                    completed_at: None,
                },
                proof: Some(proof),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VerificationOutcome {
    Completed(VerificationVerdict),
    Failed(VerificationFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VerificationStatus {
    Unavailable,
    Queued,
    Verifying,
    Completed(VerificationVerdict),
    Failed(VerificationFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredVerificationStatus {
    pub verification_id: VerificationId,
    pub status: VerificationStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletedVerification {
    pub verification_id: VerificationId,
    pub verdict: VerificationVerdict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreChange {
    Inserted,
    Updated,
    Unchanged,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_enforces_wire_bounds_and_stable_code_format() {
        let failure = VerificationFailure::new("backend_timeout", "timed out", true).unwrap();
        assert_eq!(failure.code(), "backend_timeout");
        assert_eq!(failure.message(), "timed out");
        assert!(failure.retryable());

        for code in ["", "BackendTimeout", "backend-timeout", "1backend"] {
            assert!(VerificationFailure::new(code, "", false).is_err());
        }
        assert!(VerificationFailure::new("a".repeat(65), "", false).is_err());
        assert!(VerificationFailure::new("backend", "m".repeat(257), false).is_err());
    }
}
