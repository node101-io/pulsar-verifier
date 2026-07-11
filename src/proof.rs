use std::fmt;

use bytes::Bytes;

use crate::{Error, Result};

pub const PROOF_HASH_LEN: usize = 32;

/// Canonical BLAKE3 content identifier shared by storage and network boundaries.
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
            Error::InvalidProofHash(format!(
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

/// Opaque proof content transferred over P2P after its hash is known on-chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProofContent {
    pub proof_hash: ProofHash,
    pub proof: Bytes,
}

/// Chain-owned identifier selecting the cryptographic verifier implementation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProofType(String);

impl ProofType {
    /// Preserves the chain-provided identifier after rejecting an empty value.
    ///
    /// # Errors
    ///
    /// Returns an error when the identifier is empty or contains only whitespace.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(Error::InvalidProofType(
                "proof type must not be empty".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProofType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}
