mod noir;
mod worker;

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    proof::{Proof, ProofType},
    store::{VerificationFailure, VerificationVerdict},
};

pub(crate) use noir::NoirVerifier;
pub(crate) use worker::VerificationWorker;

/// Proof-system backend contract. Each backend owns its execution strategy.
#[async_trait]
pub(crate) trait Verifier: Send + Sync {
    async fn verify(
        &self,
        proof: &Proof,
        cancel: CancellationToken,
    ) -> std::result::Result<VerificationVerdict, VerificationFailure>;
}

/// Static proof-type routing table assembled during application startup.
#[derive(Default)]
pub(crate) struct VerifierRegistry {
    verifiers: HashMap<ProofType, Arc<dyn Verifier>>,
}

impl VerifierRegistry {
    pub(crate) fn new(
        verifiers: impl IntoIterator<Item = (ProofType, Arc<dyn Verifier>)>,
    ) -> Result<Self> {
        let mut registry = Self::default();
        for (proof_type, verifier) in verifiers {
            if registry.verifiers.insert(proof_type, verifier).is_some() {
                return Err(Error::DuplicateVerifier(proof_type));
            }
        }
        Ok(registry)
    }

    fn get(&self, proof_type: ProofType) -> Option<Arc<dyn Verifier>> {
        self.verifiers.get(&proof_type).cloned()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    struct ValidVerifier;

    #[async_trait]
    impl Verifier for ValidVerifier {
        async fn verify(
            &self,
            _proof: &Proof,
            _cancel: CancellationToken,
        ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
            Ok(VerificationVerdict::Valid)
        }
    }

    #[test]
    fn registry_rejects_duplicate_proof_types() {
        let verifier = Arc::new(ValidVerifier) as Arc<dyn Verifier>;
        assert!(matches!(
            VerifierRegistry::new([
                (ProofType::NoirBarretenberg, Arc::clone(&verifier)),
                (ProofType::NoirBarretenberg, verifier),
            ]),
            Err(Error::DuplicateVerifier(ProofType::NoirBarretenberg))
        ));
    }

    #[tokio::test]
    async fn registry_routes_by_numeric_proof_type() {
        let registry = VerifierRegistry::new([(
            ProofType::NoirBarretenberg,
            Arc::new(ValidVerifier) as Arc<dyn Verifier>,
        )])
        .unwrap();
        let proof = Proof {
            proof_type: ProofType::NoirBarretenberg,
            proof: Bytes::new(),
            public_inputs: Bytes::new(),
            verification_key: Bytes::new(),
        };

        assert_eq!(
            registry
                .get(proof.proof_type)
                .unwrap()
                .verify(&proof, CancellationToken::new())
                .await,
            Ok(VerificationVerdict::Valid)
        );
        assert!(registry.get(ProofType::MinaPickles).is_none());
    }
}
