use pulsar_verifier_proto::chain_v1::{
    FinalProofResult, ProofRecord, QueryProofResponse, query_proof_response,
};

use crate::{
    Error, Result,
    proof::{ProofType, VerificationId},
};

const COMPONENT_HASH_LEN: usize = 32;
const MAX_PROOFS_PER_BLOCK: u32 = 256;

/// A committed verification request that still matters to the sidecar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChainProof {
    pub(crate) verification_id: VerificationId,
    pub(crate) submission_height: u64,
    pub(crate) index_in_block: u32,
}

/// Validates one query record and returns pending requests only.
pub(crate) fn validate_query_proof(
    response: QueryProofResponse,
    requested_height: u64,
) -> Result<Option<ChainProof>> {
    let key = response.proof_key.ok_or_else(|| {
        Error::InvalidChainContract("proof query response is missing proof_key".to_owned())
    })?;
    validate_position(key.submission_height, key.index_in_block, requested_height)?;

    let (id, pending) = match response.state {
        Some(query_proof_response::State::Pending(record)) => {
            (validate_pending_descriptor(&record)?, true)
        }
        Some(query_proof_response::State::FinalResult(record)) => {
            validate_final_position(&record, key.submission_height, key.index_in_block)?;
            (validate_final_descriptor(&record)?, false)
        }
        None => {
            return Err(Error::InvalidChainContract(
                "proof query response has no state".to_owned(),
            ));
        }
    };

    Ok(pending.then_some(ChainProof {
        verification_id: id,
        submission_height: key.submission_height,
        index_in_block: key.index_in_block,
    }))
}

/// Applies the chain's canonical descriptor invariants to query and event data.
pub(crate) fn validate_descriptor(
    proof_type: i32,
    proof_hash: &[u8],
    public_inputs_hash: &[u8],
    verification_key_hash: &[u8],
    verification_id: &[u8],
) -> Result<VerificationId> {
    let proof_type = ProofType::try_from(proof_type)?;
    let proof_hash = component_hash("proof_hash", proof_hash)?;
    let public_inputs_hash = component_hash("public_inputs_hash", public_inputs_hash)?;
    let verification_key_hash = component_hash("verification_key_hash", verification_key_hash)?;
    let expected = VerificationId::try_from(verification_id)?;
    let computed = VerificationId::from_component_hashes(
        proof_type,
        &proof_hash,
        &public_inputs_hash,
        &verification_key_hash,
    );
    if computed != expected {
        return Err(Error::VerificationIdMismatch(expected));
    }
    Ok(expected)
}

pub(crate) fn validate_position(
    submission_height: u64,
    index_in_block: u32,
    expected_height: u64,
) -> Result<()> {
    if submission_height == 0 || submission_height != expected_height {
        return Err(Error::InvalidChainContract(format!(
            "invalid proof submission height {submission_height}; expected {expected_height}"
        )));
    }
    if index_in_block >= MAX_PROOFS_PER_BLOCK {
        return Err(Error::InvalidChainContract(format!(
            "proof index {index_in_block} exceeds the per-block contract limit"
        )));
    }
    Ok(())
}

fn validate_pending_descriptor(record: &ProofRecord) -> Result<VerificationId> {
    validate_descriptor(
        record.proof_type,
        &record.proof_hash,
        &record.public_inputs_hash,
        &record.verification_key_hash,
        &record.verification_id,
    )
}

fn validate_final_descriptor(record: &FinalProofResult) -> Result<VerificationId> {
    validate_descriptor(
        record.proof_type,
        &record.proof_hash,
        &record.public_inputs_hash,
        &record.verification_key_hash,
        &record.verification_id,
    )
}

fn validate_final_position(
    record: &FinalProofResult,
    submission_height: u64,
    index_in_block: u32,
) -> Result<()> {
    if record.submission_height != submission_height {
        return Err(Error::InvalidChainContract(
            "final proof result disagrees with its proof key height".to_owned(),
        ));
    }
    // Final results do not repeat the index; the enclosing ProofKey remains authoritative.
    let _ = index_in_block;
    Ok(())
}

fn component_hash(name: &str, bytes: &[u8]) -> Result<[u8; COMPONENT_HASH_LEN]> {
    bytes.try_into().map_err(|_| {
        Error::InvalidChainContract(format!(
            "{name} must be {COMPONENT_HASH_LEN} bytes, got {}",
            bytes.len()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_chain_descriptor() {
        let id = VerificationId::from_component_hashes(
            ProofType::NoirBarretenberg,
            &[1; 32],
            &[2; 32],
            &[3; 32],
        );

        assert_eq!(
            validate_descriptor(2, &[1; 32], &[2; 32], &[3; 32], id.as_bytes()).unwrap(),
            id
        );
    }

    #[test]
    fn rejects_malformed_or_mismatched_descriptor() {
        assert!(matches!(
            validate_descriptor(2, &[1; 31], &[2; 32], &[3; 32], &[4; 32]),
            Err(Error::InvalidChainContract(_))
        ));
        assert!(matches!(
            validate_descriptor(2, &[1; 32], &[2; 32], &[3; 32], &[4; 32]),
            Err(Error::VerificationIdMismatch(_))
        ));
    }
}
