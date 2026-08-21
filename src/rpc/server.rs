use std::{sync::Arc, time::Duration};

use pulsar_verifier_proto::v1::verification_service_server::VerificationServiceServer;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic_health::{ServingStatus, server::HealthReporter};

use crate::{Error, Result, config::RpcConfig, store::ProofStore};

use super::service::VerificationRpc;

const RPC_TASK: &str = "verification RPC server";
const MAX_RPC_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_CONCURRENT_REQUESTS_PER_CONNECTION: usize = 64;

/// Result of the RPC task exiting outside graceful shutdown.
pub(crate) struct RpcExit {
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
}

impl RpcExit {
    pub(crate) fn into_error(self) -> Error {
        match self.result {
            Ok(Ok(())) => Error::TaskExitedUnexpectedly(RPC_TASK),
            Ok(Err(error)) => error,
            Err(error) => Error::Task(error),
        }
    }
}

/// Owns the bound Tonic server, health state, and graceful-stop task handle.
pub(crate) struct RpcServer {
    health: HealthReporter,
    stop: CancellationToken,
    task: Option<JoinHandle<Result<()>>>,
    local_address: std::net::SocketAddr,
}

impl RpcServer {
    pub(crate) async fn start(config: RpcConfig, store: Arc<ProofStore>) -> Result<Self> {
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
        let (mut health, health_service) = tonic_health::server::health_reporter();
        health
            .set_serving::<VerificationServiceServer<VerificationRpc>>()
            .await;

        let verification_service = VerificationServiceServer::new(VerificationRpc::new(store))
            .max_decoding_message_size(MAX_RPC_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_RPC_MESSAGE_BYTES);
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            Server::builder()
                .concurrency_limit_per_connection(MAX_CONCURRENT_REQUESTS_PER_CONNECTION)
                .add_service(health_service)
                .add_service(verification_service)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown.cancelled_owned(),
                )
                .await
                .map_err(Error::RpcServer)
        });

        tracing::info!(address = %local_address, "verification RPC server is ready");
        Ok(Self {
            health,
            stop,
            task: Some(task),
            local_address,
        })
    }

    pub(crate) async fn mark_not_serving(&mut self) {
        self.health
            .set_not_serving::<VerificationServiceServer<VerificationRpc>>()
            .await;
        self.health
            .set_service_status("", ServingStatus::NotServing)
            .await;
    }

    pub(crate) async fn wait_for_exit(&mut self) -> RpcExit {
        let result = self
            .task
            .as_mut()
            .expect("RPC task exists while server is active")
            .await;
        self.task.take();
        RpcExit { result }
    }

    pub(crate) async fn shutdown(mut self, shutdown_timeout: Duration) -> Result<()> {
        self.stop.cancel();
        let task = self
            .task
            .as_mut()
            .ok_or(Error::TaskExitedUnexpectedly(RPC_TASK))?;
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
            tracing::warn!(%error, "failed to join force-stopped RPC task");
        }
    }
}

impl Drop for RpcServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            tracing::warn!(address = %self.local_address, "RPC server dropped before shutdown");
            task.abort();
        }
    }
}

fn task_result(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    result.map_err(Error::Task)?
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use pulsar_verifier_proto::v1::{
        GetProofStatusesRequest, GetVerificationResultsRequest, VerificationPhase,
        VerificationVerdict as WireVerdict, verification_service_client::VerificationServiceClient,
    };
    use tonic::{Code, Request};
    use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};

    use super::*;
    use crate::{
        config::ProofStoreConfig,
        proof::{Proof, ProofType, VerificationId},
        store::{ProofSource, VerificationOutcome, VerificationVerdict},
    };

    const SERVICE_NAME: &str = "pulsar.verifier.v1.VerificationService";

    fn store() -> Arc<ProofStore> {
        Arc::new(
            ProofStore::new(ProofStoreConfig {
                max_capacity_bytes: 4 * 1024 * 1024,
                max_proof_bytes: 1024,
                terminal_retention: Duration::from_secs(60),
                event_buffer: 32,
            })
            .unwrap(),
        )
    }

    fn proof(seed: u16) -> Proof {
        Proof {
            proof_type: ProofType::NoirBarretenberg,
            proof: Bytes::copy_from_slice(&seed.to_be_bytes()),
            public_inputs: Bytes::from_static(b"inputs"),
            verification_key: Bytes::from_static(b"key"),
        }
    }

    async fn complete(
        store: &ProofStore,
        proof: Proof,
        verdict: VerificationVerdict,
    ) -> VerificationId {
        let id = proof.verification_id();
        store.observe_chain_verification(id).await.unwrap();
        store
            .insert_local_proof(proof, ProofSource::Rpc)
            .await
            .unwrap();
        let job = store.begin_verification(id).await.unwrap().unwrap();
        store
            .finish_verification(&job, VerificationOutcome::Completed(verdict))
            .await
            .unwrap();
        id
    }

    async fn start(
        store: Arc<ProofStore>,
    ) -> (
        RpcServer,
        VerificationServiceClient<tonic::transport::Channel>,
        HealthClient<tonic::transport::Channel>,
    ) {
        let server = RpcServer::start(
            RpcConfig {
                enabled: true,
                listen_address: "127.0.0.1:0".parse().unwrap(),
            },
            store,
        )
        .await
        .unwrap();
        let endpoint = format!("http://{}", server.local_address());
        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let verification = VerificationServiceClient::new(channel.clone());
        let health = HealthClient::new(channel);
        (server, verification, health)
    }

    #[tokio::test]
    async fn serves_completed_subset_statuses_and_health() {
        let store = store();
        let valid = complete(&store, proof(1), VerificationVerdict::Valid).await;
        let invalid = complete(&store, proof(2), VerificationVerdict::Invalid).await;
        let missing = proof(3).verification_id();
        let (mut server, mut client, mut health) = start(store).await;

        let response = client
            .get_verification_results(GetVerificationResultsRequest {
                verification_ids: vec![
                    missing.as_bytes().to_vec(),
                    valid.as_bytes().to_vec(),
                    invalid.as_bytes().to_vec(),
                ],
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].verification_id, valid.as_bytes());
        assert_eq!(response.results[0].verdict, WireVerdict::Valid as i32);
        assert_eq!(response.results[1].verification_id, invalid.as_bytes());
        assert_eq!(response.results[1].verdict, WireVerdict::Invalid as i32);

        let statuses = client
            .get_proof_statuses(GetProofStatusesRequest {
                verification_ids: vec![missing.as_bytes().to_vec(), valid.as_bytes().to_vec()],
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(statuses.statuses.len(), 2);
        assert_eq!(
            statuses.statuses[0].phase,
            VerificationPhase::Unavailable as i32
        );
        assert_eq!(
            statuses.statuses[1].phase,
            VerificationPhase::Completed as i32
        );

        let health_request = HealthCheckRequest {
            service: SERVICE_NAME.to_owned(),
        };
        assert_eq!(
            health
                .check(health_request.clone())
                .await
                .unwrap()
                .into_inner()
                .status,
            tonic_health::pb::health_check_response::ServingStatus::Serving as i32
        );
        server.mark_not_serving().await;
        assert_eq!(
            health
                .check(health_request)
                .await
                .unwrap()
                .into_inner()
                .status,
            tonic_health::pb::health_check_response::ServingStatus::NotServing as i32
        );
        server.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn rejects_contract_and_transport_limit_violations() {
        let (server, mut client, _) = start(store()).await;
        let duplicate = vec![7; 32];
        let error = client
            .get_verification_results(GetVerificationResultsRequest {
                verification_ids: vec![duplicate.clone(), duplicate],
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);

        let oversized = GetVerificationResultsRequest {
            verification_ids: vec![vec![0; 1024]; 257],
        };
        let error = client
            .get_verification_results(Request::new(oversized))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::OutOfRange);
        server.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn returns_full_batch_within_the_chain_client_deadline() {
        let store = store();
        let mut ids = Vec::with_capacity(512);
        for seed in 0..512 {
            ids.push(complete(&store, proof(seed), VerificationVerdict::Valid).await);
        }
        let (server, client, _) = start(store).await;
        let request = GetVerificationResultsRequest {
            verification_ids: ids.iter().map(|id| id.as_bytes().to_vec()).collect(),
        };

        let responses = tokio::time::timeout(Duration::from_millis(100), async {
            futures::future::try_join_all((0..8).map(|_| {
                let mut client = client.clone();
                let request = request.clone();
                async move {
                    client
                        .get_verification_results(request)
                        .await
                        .map(tonic::Response::into_inner)
                }
            }))
            .await
        })
        .await
        .expect("local result RPC exceeded the chain client deadline")
        .unwrap();

        assert!(
            responses
                .iter()
                .all(|response| response.results.len() == 512)
        );
        server.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn reports_unexpected_rpc_task_exit() {
        let (mut server, _, _) = start(store()).await;
        server.task.as_ref().unwrap().abort();

        assert!(matches!(
            server.wait_for_exit().await.into_error(),
            Error::Task(_)
        ));
    }
}
