use std::sync::Arc;

use pulsar_verifier_proto::v1::{
    GetProofStatusesRequest, GetProofStatusesResponse, GetVerificationResultsRequest,
    GetVerificationResultsResponse, verification_service_server::VerificationService,
};
use tonic::{Request, Response, Status};

use crate::{Error, store::ProofStore};

use super::contract;

/// Thin Tonic boundary over the canonical Store-to-wire contract mappers.
pub(super) struct VerificationRpc {
    store: Arc<ProofStore>,
}

impl VerificationRpc {
    pub(super) fn new(store: Arc<ProofStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl VerificationService for VerificationRpc {
    async fn get_verification_results(
        &self,
        request: Request<GetVerificationResultsRequest>,
    ) -> std::result::Result<Response<GetVerificationResultsResponse>, Status> {
        contract::get_verification_results(&self.store, request.into_inner())
            .await
            .map(Response::new)
            .map_err(rpc_status)
    }

    async fn get_proof_statuses(
        &self,
        request: Request<GetProofStatusesRequest>,
    ) -> std::result::Result<Response<GetProofStatusesResponse>, Status> {
        contract::get_proof_statuses(&self.store, request.into_inner())
            .await
            .map(Response::new)
            .map_err(rpc_status)
    }
}

fn rpc_status(error: Error) -> Status {
    match error {
        error @ (Error::InvalidVerificationId(_) | Error::InvalidVerificationRequest(_)) => {
            Status::invalid_argument(error.to_string())
        }
        error => {
            tracing::error!(%error, "verification RPC request failed");
            Status::internal("verification service request failed")
        }
    }
}
