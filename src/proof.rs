use std::fmt;

use bytes::Bytes;
use pulsar_verifier_proto::v1::{Proof as WireProof, ProofType as WireProofType};
use sha2::{Digest as _, Sha256};

use crate::{Error, Result};

pub const VERIFICATION_ID_LEN: usize = 32;
const VERIFICATION_ID_DOMAIN: &[u8] = b"pulsar/verification/v1\0";

/// Consensus-compatible identifier binding a proof, statement, key, and verifier family.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VerificationId([u8; VERIFICATION_ID_LEN]);

impl VerificationId {
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; VERIFICATION_ID_LEN] {
        &self.0
    }

    #[must_use]
    pub fn from_component_hashes(
        proof_type: ProofType,
        proof_hash: &[u8; 32],
        public_inputs_hash: &[u8; 32],
        verification_key_hash: &[u8; 32],
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(VERIFICATION_ID_DOMAIN);
        hasher.update(u32::from(proof_type).to_be_bytes());
        hasher.update(proof_hash);
        hasher.update(public_inputs_hash);
        hasher.update(verification_key_hash);
        Self(hasher.finalize().into())
    }
}

impl TryFrom<&[u8]> for VerificationId {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        let bytes = value.try_into().map_err(|_| {
            Error::InvalidVerificationId(format!(
                "verification ID must be {VERIFICATION_ID_LEN} bytes, got {}",
                value.len()
            ))
        })?;
        Ok(Self(bytes))
    }
}

impl fmt::Display for VerificationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for VerificationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "VerificationId({self})")
    }
}

/// Proof systems supported by every validator in the current chain version.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u32)]
pub enum ProofType {
    MinaPickles = 1,
    NoirBarretenberg = 2,
}

impl From<ProofType> for u32 {
    fn from(value: ProofType) -> Self {
        value as Self
    }
}

impl From<ProofType> for i32 {
    fn from(value: ProofType) -> Self {
        match value {
            ProofType::MinaPickles => 1,
            ProofType::NoirBarretenberg => 2,
        }
    }
}

impl TryFrom<i32> for ProofType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self> {
        match WireProofType::try_from(value) {
            Ok(WireProofType::MinaPickles) => Ok(Self::MinaPickles),
            Ok(WireProofType::NoirBarretenberg) => Ok(Self::NoirBarretenberg),
            Ok(WireProofType::Unspecified) | Err(_) => Err(Error::UnsupportedProofType(value)),
        }
    }
}

/// Complete immutable input required by a cryptographic verifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Proof {
    pub proof_type: ProofType,
    pub proof: Bytes,
    pub public_inputs: Bytes,
    pub verification_key: Bytes,
}

impl Proof {
    #[must_use]
    pub fn verification_id(&self) -> VerificationId {
        let proof_hash: [u8; 32] = Sha256::digest(&self.proof).into();
        let public_inputs_hash: [u8; 32] = Sha256::digest(&self.public_inputs).into();
        let verification_key_hash: [u8; 32] = Sha256::digest(&self.verification_key).into();
        VerificationId::from_component_hashes(
            self.proof_type,
            &proof_hash,
            &public_inputs_hash,
            &verification_key_hash,
        )
    }

    /// Returns the actual protobuf size used by every ingress boundary.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        // The supported enum values encode as a one-byte value after their tag.
        2 + encoded_bytes_len(&self.proof)
            + encoded_bytes_len(&self.public_inputs)
            + encoded_bytes_len(&self.verification_key)
    }

    #[must_use]
    pub fn payload_len(&self) -> Option<usize> {
        self.proof
            .len()
            .checked_add(self.public_inputs.len())?
            .checked_add(self.verification_key.len())
    }
}

fn encoded_bytes_len(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        return 0;
    }
    1 + prost::encoding::encoded_len_varint(bytes.len() as u64) + bytes.len()
}

impl From<&Proof> for WireProof {
    fn from(value: &Proof) -> Self {
        Self {
            proof_type: value.proof_type.into(),
            proof: value.proof.to_vec(),
            public_inputs: value.public_inputs.to_vec(),
            verification_key: value.verification_key.to_vec(),
        }
    }
}

impl TryFrom<WireProof> for Proof {
    type Error = Error;

    fn try_from(value: WireProof) -> Result<Self> {
        Ok(Self {
            proof_type: value.proof_type.try_into()?,
            proof: value.proof.into(),
            public_inputs: value.public_inputs.into(),
            verification_key: value.verification_key.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::*;

    fn proof() -> Proof {
        Proof {
            proof_type: ProofType::MinaPickles,
            proof: Bytes::from_static(b"proof"),
            public_inputs: Bytes::from_static(b"public-inputs"),
            verification_key: Bytes::from_static(b"verification-key"),
        }
    }

    #[test]
    fn matches_chain_verification_id_vector() {
        let id = VerificationId::from_component_hashes(
            ProofType::MinaPickles,
            &[1; 32],
            &[2; 32],
            &[3; 32],
        );

        assert_eq!(
            id.to_string(),
            "d2548149a7d662657a2a4b4a15b18f6f49fd85c114f6958c4db423edb4bd3e35"
        );
    }

    #[test]
    fn every_proof_component_changes_the_id() {
        let original = proof();
        let id = original.verification_id();

        let mut changed = original.clone();
        changed.proof = Bytes::from_static(b"other-proof");
        assert_ne!(changed.verification_id(), id);
        changed = original.clone();
        changed.public_inputs = Bytes::from_static(b"other-inputs");
        assert_ne!(changed.verification_id(), id);
        changed = original.clone();
        changed.verification_key = Bytes::from_static(b"other-key");
        assert_ne!(changed.verification_id(), id);
        changed.proof_type = ProofType::NoirBarretenberg;
        assert_ne!(changed.verification_id(), id);
    }

    #[test]
    fn wire_round_trip_preserves_complete_proof() {
        let original = proof();
        let wire = WireProof::from(&original);

        assert_eq!(original.encoded_len(), wire.encoded_len());
        assert_eq!(Proof::try_from(wire).unwrap(), original);
    }

    #[test]
    fn rejects_invalid_ids_and_proof_types() {
        assert!(matches!(
            VerificationId::try_from(&[0; 31][..]),
            Err(Error::InvalidVerificationId(_))
        ));
        assert!(matches!(
            ProofType::try_from(0),
            Err(Error::UnsupportedProofType(0))
        ));
        assert!(matches!(
            ProofType::try_from(99),
            Err(Error::UnsupportedProofType(99))
        ));
    }
}
