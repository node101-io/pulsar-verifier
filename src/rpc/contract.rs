use std::collections::HashSet;

use pulsar_verifier_proto::v1::{
    GetProofStatusesRequest, GetProofStatusesResponse, GetVerificationResultsRequest,
    GetVerificationResultsResponse, ProofVerificationStatus, VerificationFailure as WireFailure,
    VerificationPhase as WirePhase, VerificationResult as WireResult,
    VerificationVerdict as WireVerdict,
};

use crate::{
    Error, Result,
    proof::VerificationId,
    store::{ProofStore, StoredVerificationStatus, VerificationStatus, VerificationVerdict},
};

pub(crate) const MAX_VERIFICATION_IDS: usize = 512;

/// Maps the completed subset of a bounded chain request into the consensus-facing response.
pub(crate) async fn get_verification_results(
    store: &ProofStore,
    request: GetVerificationResultsRequest,
) -> Result<GetVerificationResultsResponse> {
    let ids = parse_ids(request.verification_ids)?;
    let results = store
        .completed_results(&ids)
        .await
        .into_iter()
        .map(|result| WireResult {
            verification_id: result.verification_id.as_bytes().to_vec(),
            verdict: wire_verdict(result.verdict),
        })
        .collect();
    Ok(GetVerificationResultsResponse { results })
}

/// Maps every requested ID into a local diagnostic status, including unknown IDs.
pub(crate) async fn get_proof_statuses(
    store: &ProofStore,
    request: GetProofStatusesRequest,
) -> Result<GetProofStatusesResponse> {
    let ids = parse_ids(request.verification_ids)?;
    let statuses = store
        .statuses(&ids)
        .await
        .into_iter()
        .map(wire_status)
        .collect();
    Ok(GetProofStatusesResponse { statuses })
}

fn parse_ids(raw_ids: Vec<Vec<u8>>) -> Result<Vec<VerificationId>> {
    if raw_ids.len() > MAX_VERIFICATION_IDS {
        return Err(Error::InvalidVerificationRequest(format!(
            "at most {MAX_VERIFICATION_IDS} verification IDs are allowed"
        )));
    }

    let mut seen = HashSet::with_capacity(raw_ids.len());
    let mut ids = Vec::with_capacity(raw_ids.len());
    for raw_id in raw_ids {
        let id = VerificationId::try_from(raw_id.as_slice())?;
        if !seen.insert(id) {
            return Err(Error::InvalidVerificationRequest(format!(
                "duplicate verification ID {id}"
            )));
        }
        ids.push(id);
    }
    Ok(ids)
}

fn wire_status(status: StoredVerificationStatus) -> ProofVerificationStatus {
    let (phase, verdict, failure) = match status.status {
        VerificationStatus::Unavailable => (WirePhase::Unavailable, None, None),
        VerificationStatus::Queued => (WirePhase::Queued, None, None),
        VerificationStatus::Verifying => (WirePhase::Verifying, None, None),
        VerificationStatus::Completed(verdict) => {
            (WirePhase::Completed, Some(wire_verdict(verdict)), None)
        }
        VerificationStatus::Failed(failure) => (
            WirePhase::Failed,
            None,
            Some(WireFailure {
                code: failure.code().to_owned(),
                message: failure.message().to_owned(),
                retryable: failure.retryable(),
            }),
        ),
    };

    ProofVerificationStatus {
        verification_id: status.verification_id.as_bytes().to_vec(),
        phase: phase.into(),
        verdict: verdict.unwrap_or(WireVerdict::Unspecified.into()),
        failure,
    }
}

const fn wire_verdict(verdict: VerificationVerdict) -> i32 {
    match verdict {
        VerificationVerdict::Valid => WireVerdict::Valid as i32,
        VerificationVerdict::Invalid => WireVerdict::Invalid as i32,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use prost::Message as _;

    use super::*;
    use crate::{
        config::ProofStoreConfig,
        proof::{Proof, ProofType},
        store::{
            ProofSource, VerificationFailure, VerificationJob, VerificationOutcome,
            VerificationVerdict,
        },
    };

    const MAX_STATUS_RESPONSE_BYTES: usize = 256 * 1024;

    fn store() -> ProofStore {
        ProofStore::new(ProofStoreConfig {
            max_capacity_bytes: 1024 * 1024,
            max_proof_bytes: 1024,
            terminal_retention: Duration::from_secs(60),
            event_buffer: 16,
        })
        .unwrap()
    }

    fn proof(seed: u8) -> Proof {
        Proof {
            proof_type: ProofType::NoirBarretenberg,
            proof: Bytes::from(vec![seed]),
            public_inputs: Bytes::from_static(b"inputs"),
            verification_key: Bytes::from_static(b"key"),
        }
    }

    async fn ready(store: &ProofStore, proof: Proof) -> (VerificationId, VerificationJob) {
        let id = proof.verification_id();
        store.observe_chain_verification(id).await.unwrap();
        store
            .insert_local_proof(proof, ProofSource::Rpc)
            .await
            .unwrap();
        let job = store.begin_verification(id).await.unwrap().unwrap();
        (id, job)
    }

    #[tokio::test]
    async fn results_include_only_completed_verdicts() {
        let store = store();
        let (valid, valid_job) = ready(&store, proof(1)).await;
        let (failed, failed_job) = ready(&store, proof(2)).await;
        let missing = proof(3).verification_id();
        store
            .finish_verification(
                &valid_job,
                VerificationOutcome::Completed(VerificationVerdict::Valid),
            )
            .await
            .unwrap();
        store
            .finish_verification(
                &failed_job,
                VerificationOutcome::Failed(
                    VerificationFailure::new("backend_timeout", "timed out", true).unwrap(),
                ),
            )
            .await
            .unwrap();

        let response = get_verification_results(
            &store,
            GetVerificationResultsRequest {
                verification_ids: vec![
                    missing.as_bytes().to_vec(),
                    failed.as_bytes().to_vec(),
                    valid.as_bytes().to_vec(),
                ],
            },
        )
        .await
        .unwrap();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].verification_id, valid.as_bytes());
        assert_eq!(response.results[0].verdict, WireVerdict::Valid as i32);
    }

    #[tokio::test]
    async fn statuses_cover_every_requested_id_and_preserve_phase_invariants() {
        let store = store();
        let (failed, failed_job) = ready(&store, proof(4)).await;
        let unavailable = proof(5).verification_id();
        let queued_proof = proof(6);
        let queued = queued_proof.verification_id();
        store.observe_chain_verification(queued).await.unwrap();
        store
            .insert_local_proof(queued_proof, ProofSource::Rpc)
            .await
            .unwrap();
        let (verifying, _verifying_job) = ready(&store, proof(7)).await;
        let (completed, completed_job) = ready(&store, proof(8)).await;
        store
            .finish_verification(
                &completed_job,
                VerificationOutcome::Completed(VerificationVerdict::Invalid),
            )
            .await
            .unwrap();
        store
            .finish_verification(
                &failed_job,
                VerificationOutcome::Failed(
                    VerificationFailure::new("backend_crash", "worker exited", false).unwrap(),
                ),
            )
            .await
            .unwrap();

        let response = get_proof_statuses(
            &store,
            GetProofStatusesRequest {
                verification_ids: vec![
                    unavailable.as_bytes().to_vec(),
                    queued.as_bytes().to_vec(),
                    verifying.as_bytes().to_vec(),
                    completed.as_bytes().to_vec(),
                    failed.as_bytes().to_vec(),
                ],
            },
        )
        .await
        .unwrap();

        assert_eq!(response.statuses.len(), 5);
        assert_eq!(response.statuses[0].phase, WirePhase::Unavailable as i32);
        assert_eq!(
            response.statuses[0].verdict,
            WireVerdict::Unspecified as i32
        );
        assert!(response.statuses[0].failure.is_none());
        assert_eq!(response.statuses[1].phase, WirePhase::Queued as i32);
        assert_eq!(
            response.statuses[1].verdict,
            WireVerdict::Unspecified as i32
        );
        assert!(response.statuses[1].failure.is_none());
        assert_eq!(response.statuses[2].phase, WirePhase::Verifying as i32);
        assert_eq!(response.statuses[3].phase, WirePhase::Completed as i32);
        assert_eq!(response.statuses[3].verdict, WireVerdict::Invalid as i32);
        assert!(response.statuses[3].failure.is_none());
        assert_eq!(response.statuses[4].phase, WirePhase::Failed as i32);
        assert_eq!(
            response.statuses[4].verdict,
            WireVerdict::Unspecified as i32
        );
        assert_eq!(
            response.statuses[4].failure.as_ref().unwrap().code,
            "backend_crash"
        );
    }

    #[tokio::test]
    async fn rejects_invalid_or_duplicate_request_ids() {
        let store = store();
        let id = proof(6).verification_id().as_bytes().to_vec();
        for request in [
            vec![vec![0; 31]],
            vec![id.clone(), id],
            vec![vec![0; 32]; MAX_VERIFICATION_IDS + 1],
        ] {
            assert!(
                get_verification_results(
                    &store,
                    GetVerificationResultsRequest {
                        verification_ids: request,
                    },
                )
                .await
                .is_err()
            );
        }
    }

    #[test]
    fn worst_case_status_response_stays_below_the_rpc_budget() {
        let status = ProofVerificationStatus {
            verification_id: vec![0xff; 32],
            phase: WirePhase::Failed as i32,
            verdict: WireVerdict::Unspecified as i32,
            failure: Some(WireFailure {
                code: "a".repeat(64),
                message: "m".repeat(256),
                retryable: true,
            }),
        };
        let response = GetProofStatusesResponse {
            statuses: vec![status; MAX_VERIFICATION_IDS],
        };

        assert!(response.encoded_len() < MAX_STATUS_RESPONSE_BYTES);
    }
}
