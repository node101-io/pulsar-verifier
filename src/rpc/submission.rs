use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use pulsar_verifier_proto::v1::{
    SubmitProofRequest, SubmitProofResponse,
    submission_service_server::{
        SubmissionService as SubmissionRpcContract, SubmissionServiceServer,
    },
};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::timeout,
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    Request, Response, Status,
    service::{Interceptor, interceptor::InterceptedService},
    transport::Server,
};
use tonic_health::{ServingStatus, server::HealthReporter};

use crate::{
    Error, Result,
    chain::PulsarClient,
    config::SubmissionConfig,
    store::ProofStore,
    submission::{ProofSubmission, SubmissionService},
};

const SUBMISSION_RPC_TASK: &str = "submission RPC server";
const RESPONSE_MESSAGE_LIMIT: usize = 1_024;
const PROTOBUF_ENVELOPE_OVERHEAD: usize = 16;
const SUBMISSION_SERVICE_NAME: &str = "pulsar.verifier.v1.SubmissionService";

/// Result of the submission task exiting outside graceful shutdown.
pub(crate) struct SubmissionRpcExit {
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
}

impl SubmissionRpcExit {
    pub(crate) fn into_error(self) -> Error {
        match self.result {
            Ok(Ok(())) => Error::TaskExitedUnexpectedly(SUBMISSION_RPC_TASK),
            Ok(Err(error)) => error,
            Err(error) => Error::Task(error),
        }
    }
}

/// Owns the consumer ingress listener, health state, and graceful-stop task.
pub(crate) struct SubmissionRpcServer {
    health: HealthReporter,
    stop: CancellationToken,
    task: Option<JoinHandle<Result<()>>>,
    local_address: std::net::SocketAddr,
}

impl SubmissionRpcServer {
    pub(crate) async fn start(
        config: SubmissionConfig,
        store: Arc<ProofStore>,
        chain: PulsarClient,
        max_proof_bytes: usize,
    ) -> Result<Self> {
        let request_limit = max_proof_bytes
            .checked_add(config.max_transaction_bytes)
            .and_then(|limit| limit.checked_add(PROTOBUF_ENVELOPE_OVERHEAD))
            .ok_or_else(|| {
                Error::InvalidConfig("submission request limit overflows usize".to_owned())
            })?;

        // TODO: Add mTLS or equivalent client authentication before allowing non-loopback binds.
        let listener = TcpListener::bind(config.listen_address)
            .await
            .map_err(|source| Error::RpcBind {
                address: config.listen_address,
                source,
            })?;
        let local_address = listener.local_addr().map_err(|source| Error::RpcBind {
            address: config.listen_address,
            source,
        })?;
        let service = SubmissionRpc::new(SubmissionService::new(
            store,
            chain,
            max_proof_bytes,
            config.max_transaction_bytes,
        ));
        let submission_service = SubmissionServiceServer::new(service)
            .max_decoding_message_size(request_limit)
            .max_encoding_message_size(RESPONSE_MESSAGE_LIMIT);
        let submission_service = InterceptedService::new(
            submission_service,
            SubmissionAdmission::new(config.max_concurrent_requests),
        );
        let (mut health, health_service) = tonic_health::server::health_reporter();
        health
            .set_service_status(SUBMISSION_SERVICE_NAME, ServingStatus::Serving)
            .await;
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(health_service)
                .add_service(submission_service)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown.cancelled_owned(),
                )
                .await
                .map_err(Error::RpcServer)
        });

        tracing::info!(address = %local_address, "submission RPC server is ready");
        Ok(Self {
            health,
            stop,
            task: Some(task),
            local_address,
        })
    }

    pub(crate) async fn mark_not_serving(&mut self) {
        self.health
            .set_service_status(SUBMISSION_SERVICE_NAME, ServingStatus::NotServing)
            .await;
        self.health
            .set_service_status("", ServingStatus::NotServing)
            .await;
    }

    pub(crate) async fn wait_for_exit(&mut self) -> SubmissionRpcExit {
        let result = self
            .task
            .as_mut()
            .expect("submission RPC task exists while server is active")
            .await;
        self.task.take();
        SubmissionRpcExit { result }
    }

    pub(crate) async fn shutdown(mut self, shutdown_timeout: Duration) -> Result<()> {
        self.stop.cancel();
        let task = self
            .task
            .as_mut()
            .ok_or(Error::TaskExitedUnexpectedly(SUBMISSION_RPC_TASK))?;
        if let Ok(result) = timeout(shutdown_timeout, task).await {
            self.task.take();
            task_result(result)
        } else {
            self.abort_and_join().await;
            Err(Error::ShutdownTimeout(shutdown_timeout))
        }
    }

    pub(crate) async fn force_shutdown(mut self) {
        self.abort_and_join().await;
    }

    #[cfg(test)]
    pub(super) const fn local_address(&self) -> std::net::SocketAddr {
        self.local_address
    }

    async fn abort_and_join(&mut self) {
        let Some(task) = self.task.take() else {
            return;
        };
        task.abort();
        if let Err(error) = task.await
            && !error.is_cancelled()
        {
            tracing::warn!(%error, "failed to join force-stopped submission RPC task");
        }
    }
}

impl Drop for SubmissionRpcServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            tracing::warn!(
                address = %self.local_address,
                "submission RPC server dropped before shutdown"
            );
            task.abort();
        }
    }
}

#[derive(Clone)]
struct SubmissionRpc {
    service: SubmissionService,
}

impl SubmissionRpc {
    fn new(service: SubmissionService) -> Self {
        Self { service }
    }
}

#[derive(Clone)]
struct SubmissionAdmission {
    permits: Arc<Semaphore>,
}

impl SubmissionAdmission {
    fn new(max_concurrent_requests: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_concurrent_requests)),
        }
    }
}

impl Interceptor for SubmissionAdmission {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("submission concurrency limit reached"))?;
        request.extensions_mut().insert(Arc::new(permit));
        Ok(request)
    }
}

#[tonic::async_trait]
impl SubmissionRpcContract for SubmissionRpc {
    async fn submit_proof(
        &self,
        mut request: Request<SubmitProofRequest>,
    ) -> std::result::Result<Response<SubmitProofResponse>, Status> {
        let _permit = request
            .extensions_mut()
            .remove::<Arc<OwnedSemaphorePermit>>()
            .ok_or_else(|| Status::internal("submission admission permit is missing"))?;
        let request = request.into_inner();
        let proof = request
            .proof
            .ok_or_else(|| Status::invalid_argument("proof is required"))?
            .try_into()
            .map_err(|error| submission_status(&error))?;
        let receipt = self
            .service
            .submit(ProofSubmission {
                proof,
                tx_raw: Bytes::from(request.tx_raw),
            })
            .await
            .map_err(|error| submission_status(&error))?;

        Ok(Response::new(SubmitProofResponse {
            verification_id: receipt.verification_id.as_bytes().to_vec(),
            transaction_hash: receipt.transaction_hash.to_vec(),
        }))
    }
}

fn submission_status(error: &Error) -> Status {
    match error {
        Error::InvalidSubmission(_)
        | Error::UnsupportedProofType(_)
        | Error::VerificationIdMismatch(_) => Status::invalid_argument(error.to_string()),
        Error::ProofTooLarge { .. } | Error::TransactionTooLarge { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        Error::CheckTxRejected { .. } => Status::failed_precondition(error.to_string()),
        Error::Chain(_) => Status::unavailable("local CometBFT RPC is unavailable"),
        error => {
            tracing::error!(%error, "submission RPC request failed");
            Status::internal("submission service request failed")
        }
    }
}

fn task_result(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    result.map_err(Error::Task)?
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prost::Message as _;
    use prost_types::Any;
    use pulsar_verifier_proto::{
        chain_v1::{MsgSubmitProof, ProofType as WireProofType},
        cosmos::tx::v1beta1::{AuthInfo, SignerInfo, TxBody, TxRaw},
        v1::{Proof as WireProof, submission_service_client::SubmissionServiceClient},
    };
    use serde_json::Value;
    use sha2::{Digest as _, Sha256};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        task::JoinHandle,
    };
    use tokio_util::sync::CancellationToken;
    use tonic::Code;
    use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};

    use super::*;
    use crate::{
        config::{ChainConfig, ProofStoreConfig},
        proof::{Proof, ProofType},
    };

    const SERVICE_NAME: &str = "pulsar.verifier.v1.SubmissionService";
    const MESSAGE_TYPE_URL: &str = "/pulsarchain.verification.v1.MsgSubmitProof";

    #[tokio::test]
    async fn serves_submission_and_health() {
        let proof = proof();
        let tx_raw = transaction(&proof);
        let transaction_hash: [u8; 32] = Sha256::digest(&tx_raw).into();
        let handler: Handler = Arc::new(move |request| {
            rpc_result(
                &request,
                &serde_json::json!({
                    "code": 0,
                    "data": "",
                    "log": "",
                    "codespace": "",
                    "hash": hex::encode_upper(transaction_hash),
                }),
            )
        });
        let (url, chain_stop, chain_task) = spawn_rpc_server(handler).await;
        let (mut server, mut client, mut health) = start_server(url).await;

        let response = client
            .submit_proof(SubmitProofRequest {
                proof: Some(WireProof::from(&proof)),
                tx_raw: tx_raw.to_vec(),
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.verification_id, proof.verification_id().as_bytes());
        assert_eq!(response.transaction_hash, transaction_hash);
        assert_eq!(
            health
                .check(HealthCheckRequest {
                    service: SERVICE_NAME.to_owned(),
                })
                .await
                .unwrap()
                .into_inner()
                .status,
            1
        );
        assert_eq!(
            client
                .submit_proof(SubmitProofRequest {
                    proof: None,
                    tx_raw: Vec::new(),
                })
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            client
                .submit_proof(SubmitProofRequest {
                    proof: Some(WireProof::from(&proof)),
                    tx_raw: vec![0; 1_025],
                })
                .await
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );

        server.mark_not_serving().await;
        assert_eq!(
            health
                .check(HealthCheckRequest {
                    service: SERVICE_NAME.to_owned(),
                })
                .await
                .unwrap()
                .into_inner()
                .status,
            2
        );
        server.shutdown(Duration::from_secs(1)).await.unwrap();
        chain_stop.cancel();
        chain_task.await.unwrap();
    }

    #[test]
    fn admission_limit_runs_before_request_decoding() {
        let mut admission = SubmissionAdmission::new(1);
        let accepted = admission.call(Request::new(())).unwrap();
        assert!(
            accepted
                .extensions()
                .get::<Arc<OwnedSemaphorePermit>>()
                .is_some()
        );
        let error = admission.call(Request::new(())).unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
        drop(accepted);
        assert!(admission.call(Request::new(())).is_ok());
    }

    #[test]
    fn maps_submission_failures_to_stable_grpc_codes() {
        assert_eq!(
            submission_status(&Error::InvalidSubmission("bad".to_owned())).code(),
            Code::InvalidArgument
        );
        assert_eq!(
            submission_status(&Error::TransactionTooLarge {
                actual_bytes: 2,
                max_bytes: 1,
            })
            .code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            submission_status(&Error::CheckTxRejected {
                transaction_hash: "00".repeat(32),
                codespace: "sdk".to_owned(),
                code: 7,
                log: "bad sequence".to_owned(),
            })
            .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            submission_status(&Error::Chain("offline".to_owned())).code(),
            Code::Unavailable
        );
    }

    async fn start_server(
        comet_rpc_url: String,
    ) -> (
        SubmissionRpcServer,
        SubmissionServiceClient<tonic::transport::Channel>,
        HealthClient<tonic::transport::Channel>,
    ) {
        let server = SubmissionRpcServer::start(
            SubmissionConfig {
                enabled: true,
                listen_address: "127.0.0.1:0".parse().unwrap(),
                max_transaction_bytes: 1_024,
                max_concurrent_requests: 2,
            },
            store(),
            PulsarClient::new(&ChainConfig {
                chain_id: "pulsar-test-1".to_owned(),
                comet_rpc_url,
                request_timeout: Duration::from_secs(1),
            })
            .unwrap(),
            1_024,
        )
        .await
        .unwrap();
        let endpoint = format!("http://{}", server.local_address());
        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = SubmissionServiceClient::new(channel.clone());
        let health = HealthClient::new(channel);
        (server, client, health)
    }

    fn store() -> Arc<ProofStore> {
        Arc::new(
            ProofStore::new(ProofStoreConfig {
                max_capacity_bytes: 4 * 1024 * 1024,
                max_proof_bytes: 1_024,
                terminal_retention: Duration::from_secs(60),
                event_buffer: 32,
            })
            .unwrap(),
        )
    }

    fn proof() -> Proof {
        Proof {
            proof_type: ProofType::NoirBarretenberg,
            proof: Bytes::from_static(b"proof"),
            public_inputs: Bytes::from_static(b"inputs"),
            verification_key: Bytes::from_static(b"key"),
        }
    }

    fn transaction(proof: &Proof) -> Bytes {
        let message = MsgSubmitProof {
            signer: "pulsar1consumer".to_owned(),
            proof_hash: Sha256::digest(&proof.proof).to_vec(),
            proof_type: WireProofType::NoirBarretenberg.into(),
            public_inputs_hash: Sha256::digest(&proof.public_inputs).to_vec(),
            verification_key_hash: Sha256::digest(&proof.verification_key).to_vec(),
        };
        let body = TxBody {
            messages: vec![Any {
                type_url: MESSAGE_TYPE_URL.to_owned(),
                value: message.encode_to_vec(),
            }],
            ..TxBody::default()
        };
        let auth = AuthInfo {
            signer_infos: vec![SignerInfo::default()],
            ..AuthInfo::default()
        };
        TxRaw {
            body_bytes: body.encode_to_vec(),
            auth_info_bytes: auth.encode_to_vec(),
            signatures: vec![vec![1; 64]],
        }
        .encode_to_vec()
        .into()
    }

    type Handler = Arc<dyn Fn(Value) -> Value + Send + Sync>;

    fn rpc_result(request: &Value, result: &Value) -> Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": request["id"].clone(),
            "result": result,
        })
    }

    async fn spawn_rpc_server(handler: Handler) -> (String, CancellationToken, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = task_cancellation.cancelled() => return,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let handler = Arc::clone(&handler);
                        tokio::spawn(async move { serve_connection(stream, handler).await });
                    }
                }
            }
        });
        (format!("http://{address}"), cancellation, task)
    }

    async fn serve_connection(mut stream: TcpStream, handler: Handler) {
        let mut bytes = Vec::new();
        let header_end = loop {
            let mut chunk = [0_u8; 1_024];
            let count = stream.read(&mut chunk).await.unwrap();
            if count == 0 {
                return;
            }
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("content-length: ")
                    .or_else(|| line.strip_prefix("Content-Length: "))
            })
            .unwrap()
            .trim()
            .parse::<usize>()
            .unwrap();
        while bytes.len() < header_end + content_length {
            let mut chunk = vec![0_u8; header_end + content_length - bytes.len()];
            let count = stream.read(&mut chunk).await.unwrap();
            bytes.extend_from_slice(&chunk[..count]);
        }
        let request: Value =
            serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap();
        let body = handler(request).to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    }
}
