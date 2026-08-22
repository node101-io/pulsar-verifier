use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use moka::future::Cache;
use prost::Message as _;
use pulsar_verifier_proto::{
    chain_v1::MsgSubmitProof,
    cosmos::tx::v1beta1::{AuthInfo, TxBody, TxRaw},
};
use sha2::{Digest as _, Sha256};

use crate::{
    Error,
    chain::PulsarClient,
    proof::{Proof, ProofType, VerificationId},
    store::{ProofSource, ProofStore},
};

const MSG_SUBMIT_PROOF_TYPE_URL: &str = "/pulsarchain.verification.v1.MsgSubmitProof";
const COMPONENT_HASH_LEN: usize = 32;
const RECEIPT_CACHE_CAPACITY: u64 = 4_096;
const RECEIPT_CACHE_TTL: Duration = Duration::from_secs(15 * 60);

/// Consumer-owned proof material paired with the exact signed transaction bytes.
#[derive(Clone, Debug)]
pub(crate) struct ProofSubmission {
    pub(crate) proof: Proof,
    pub(crate) tx_raw: Bytes,
}

/// A successful `CheckTx` receipt. Commitment is observed separately by the Listener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SubmissionReceipt {
    pub(crate) verification_id: VerificationId,
    pub(crate) transaction_hash: [u8; 32],
}

#[derive(Debug)]
struct ValidatedSubmission {
    verification_id: VerificationId,
    transaction_hash: [u8; 32],
    proof: Proof,
    tx_raw: Bytes,
}

/// Validates proof-to-transaction binding, stores content, and relays signed transactions.
#[derive(Clone)]
pub(crate) struct SubmissionService {
    store: Arc<ProofStore>,
    chain: PulsarClient,
    max_proof_bytes: usize,
    max_transaction_bytes: usize,
    receipts: Cache<[u8; 32], SubmissionReceipt>,
}

impl SubmissionService {
    pub(crate) fn new(
        store: Arc<ProofStore>,
        chain: PulsarClient,
        max_proof_bytes: usize,
        max_transaction_bytes: usize,
    ) -> Self {
        let receipts = Cache::builder()
            .max_capacity(RECEIPT_CACHE_CAPACITY)
            .time_to_live(RECEIPT_CACHE_TTL)
            .build();
        Self {
            store,
            chain,
            max_proof_bytes,
            max_transaction_bytes,
            receipts,
        }
    }

    /// Stores valid content before relaying so a committed request is never announced first.
    pub(crate) async fn submit(
        &self,
        submission: ProofSubmission,
    ) -> Result<SubmissionReceipt, Arc<Error>> {
        let submission =
            validate_submission(submission, self.max_proof_bytes, self.max_transaction_bytes)
                .map_err(Arc::new)?;
        let verification_id = submission.verification_id;
        let transaction_hash = submission.transaction_hash;
        let store = Arc::clone(&self.store);
        let chain = self.chain.clone();

        self.receipts
            .try_get_with(transaction_hash, async move {
                let change = store
                    .insert_local_proof(submission.proof, ProofSource::Rpc)
                    .await?;
                tracing::debug!(
                    %verification_id,
                    transaction_hash = %hex::encode(transaction_hash),
                    ?change,
                    "proof stored before transaction relay"
                );

                let response = chain.broadcast_tx_sync(submission.tx_raw).await?;
                if response.transaction_hash != transaction_hash {
                    return Err(Error::TransactionHashMismatch);
                }
                if response.code != 0 {
                    return Err(Error::CheckTxRejected {
                        transaction_hash: hex::encode(transaction_hash),
                        codespace: response.codespace,
                        code: response.code,
                        log: response.log,
                    });
                }

                tracing::info!(
                    %verification_id,
                    transaction_hash = %hex::encode(transaction_hash),
                    "proof transaction accepted by CheckTx"
                );
                Ok(SubmissionReceipt {
                    verification_id,
                    transaction_hash,
                })
            })
            .await
    }
}

fn validate_submission(
    submission: ProofSubmission,
    max_proof_bytes: usize,
    max_transaction_bytes: usize,
) -> crate::Result<ValidatedSubmission> {
    let verification_id = submission.proof.verification_id();
    let proof_bytes = submission.proof.encoded_len();
    if proof_bytes > max_proof_bytes {
        return Err(Error::ProofTooLarge {
            verification_id,
            actual_bytes: proof_bytes,
            max_bytes: max_proof_bytes,
        });
    }
    if submission.tx_raw.len() > max_transaction_bytes {
        return Err(Error::TransactionTooLarge {
            actual_bytes: submission.tx_raw.len(),
            max_bytes: max_transaction_bytes,
        });
    }

    // Decode the exact signed representation without re-encoding bytes used by the signature.
    let tx = TxRaw::decode(submission.tx_raw.as_ref())
        .map_err(|error| invalid(format!("invalid TxRaw: {error}")))?;
    if tx.body_bytes.is_empty() {
        return Err(invalid("TxRaw body_bytes must not be empty"));
    }
    if tx.auth_info_bytes.is_empty() {
        return Err(invalid("TxRaw auth_info_bytes must not be empty"));
    }
    let body = TxBody::decode(tx.body_bytes.as_slice())
        .map_err(|error| invalid(format!("invalid TxBody: {error}")))?;
    let auth = AuthInfo::decode(tx.auth_info_bytes.as_slice())
        .map_err(|error| invalid(format!("invalid AuthInfo: {error}")))?;

    // Perform cheap structural checks; CheckTx remains authoritative for signature semantics.
    if auth.signer_infos.is_empty() {
        return Err(invalid("AuthInfo must contain at least one signer_info"));
    }
    if tx.signatures.len() != auth.signer_infos.len() {
        return Err(invalid(format!(
            "signature count {} does not match signer_info count {}",
            tx.signatures.len(),
            auth.signer_infos.len()
        )));
    }
    if tx.signatures.iter().any(Vec::is_empty) {
        return Err(invalid("transaction signatures must not be empty"));
    }
    if body.messages.len() != 1 {
        return Err(invalid(format!(
            "TxBody must contain exactly one MsgSubmitProof, got {} messages",
            body.messages.len()
        )));
    }

    let message = &body.messages[0];
    if message.type_url != MSG_SUBMIT_PROOF_TYPE_URL {
        return Err(invalid(format!(
            "unexpected transaction message type {}",
            message.type_url
        )));
    }
    let message = MsgSubmitProof::decode(message.value.as_slice())
        .map_err(|error| invalid(format!("invalid MsgSubmitProof: {error}")))?;
    if message.signer.trim().is_empty() {
        return Err(invalid("MsgSubmitProof signer must not be empty"));
    }

    // Bind every off-chain artifact to the hashes and verifier family registered on-chain.
    let message_type = ProofType::try_from(message.proof_type)?;
    if message_type != submission.proof.proof_type {
        return Err(invalid("proof type does not match MsgSubmitProof"));
    }
    let proof_hash = component_hash("proof_hash", &message.proof_hash)?;
    let public_inputs_hash = component_hash("public_inputs_hash", &message.public_inputs_hash)?;
    let verification_key_hash =
        component_hash("verification_key_hash", &message.verification_key_hash)?;
    if proof_hash != Sha256::digest(&submission.proof.proof).as_slice() {
        return Err(invalid("proof bytes do not match proof_hash"));
    }
    if public_inputs_hash != Sha256::digest(&submission.proof.public_inputs).as_slice() {
        return Err(invalid(
            "public input bytes do not match public_inputs_hash",
        ));
    }
    if verification_key_hash != Sha256::digest(&submission.proof.verification_key).as_slice() {
        return Err(invalid(
            "verification key bytes do not match verification_key_hash",
        ));
    }
    let descriptor_id = VerificationId::from_component_hashes(
        message_type,
        &proof_hash,
        &public_inputs_hash,
        &verification_key_hash,
    );
    if descriptor_id != verification_id {
        return Err(Error::VerificationIdMismatch(descriptor_id));
    }

    let transaction_hash = Sha256::digest(&submission.tx_raw).into();
    Ok(ValidatedSubmission {
        verification_id,
        transaction_hash,
        proof: submission.proof,
        tx_raw: submission.tx_raw,
    })
}

fn component_hash(name: &str, bytes: &[u8]) -> crate::Result<[u8; COMPONENT_HASH_LEN]> {
    bytes.try_into().map_err(|_| {
        invalid(format!(
            "{name} must be {COMPONENT_HASH_LEN} bytes, got {}",
            bytes.len()
        ))
    })
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidSubmission(message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use prost_types::Any;
    use pulsar_verifier_proto::{
        chain_v1::ProofType as WireProofType,
        cosmos::tx::v1beta1::{SignerInfo, TxBody, TxRaw},
    };

    use super::*;
    use crate::{
        config::{ChainConfig, ProofStoreConfig},
        store::ProofStoreEvent,
    };
    use serde_json::Value;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        task::JoinHandle,
    };
    use tokio_util::sync::CancellationToken;

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
                type_url: MSG_SUBMIT_PROOF_TYPE_URL.to_owned(),
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

    fn valid_submission() -> ProofSubmission {
        let proof = proof();
        let tx_raw = transaction(&proof);
        ProofSubmission { proof, tx_raw }
    }

    #[test]
    fn validates_complete_proof_binding() {
        let submission = valid_submission();
        let expected_id = submission.proof.verification_id();
        let expected_hash = Sha256::digest(&submission.tx_raw);

        let validated = validate_submission(submission, 1_024, 1_024).unwrap();

        assert_eq!(validated.verification_id, expected_id);
        assert_eq!(
            validated.transaction_hash.as_slice(),
            expected_hash.as_slice()
        );
    }

    #[test]
    fn rejects_component_and_type_mismatches() {
        let mut submission = valid_submission();
        submission.proof.proof = Bytes::from_static(b"different");
        assert!(matches!(
            validate_submission(submission, 1_024, 1_024),
            Err(Error::InvalidSubmission(_))
        ));

        let mut submission = valid_submission();
        submission.proof.proof_type = ProofType::MinaPickles;
        assert!(matches!(
            validate_submission(submission, 1_024, 1_024),
            Err(Error::InvalidSubmission(_))
        ));
    }

    #[test]
    fn rejects_malformed_transaction_structure() {
        let mut submission = valid_submission();
        submission.tx_raw = Bytes::from_static(b"not-protobuf");
        assert!(matches!(
            validate_submission(submission, 1_024, 1_024),
            Err(Error::InvalidSubmission(_))
        ));

        let proof = proof();
        let tx = TxRaw::decode(transaction(&proof)).unwrap();
        let mut body = TxBody::decode(tx.body_bytes.as_slice()).unwrap();
        body.messages.push(body.messages[0].clone());
        let tx = TxRaw {
            body_bytes: body.encode_to_vec(),
            ..tx
        };
        assert!(matches!(
            validate_submission(
                ProofSubmission {
                    proof,
                    tx_raw: tx.encode_to_vec().into(),
                },
                1_024,
                1_024,
            ),
            Err(Error::InvalidSubmission(_))
        ));
    }

    #[test]
    fn rejects_proof_and_transaction_size_limits() {
        let submission = valid_submission();
        assert!(matches!(
            validate_submission(submission.clone(), 1, 1_024),
            Err(Error::ProofTooLarge { .. })
        ));
        assert!(matches!(
            validate_submission(submission, 1_024, 1),
            Err(Error::TransactionTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn stores_relays_and_coalesces_transaction_retries() {
        let submission = valid_submission();
        let transaction_hash: [u8; 32] = Sha256::digest(&submission.tx_raw).into();
        let calls = Arc::new(AtomicUsize::new(0));
        let recorded = Arc::clone(&calls);
        let handler: Handler = Arc::new(move |request| {
            recorded.fetch_add(1, Ordering::SeqCst);
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
        let (url, stop, server) = spawn_rpc_server(handler).await;
        let store = store();
        let mut events = store.subscribe();
        let service = service(Arc::clone(&store), url);

        let (first, second) = tokio::join!(
            service.submit(submission.clone()),
            service.submit(submission)
        );

        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            events.recv().await.unwrap(),
            ProofStoreEvent::ProofStored { .. }
        ));
        assert!(events.try_recv().is_err());

        stop.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn keeps_stored_proof_when_check_tx_rejects() {
        let submission = valid_submission();
        let verification_id = submission.proof.verification_id();
        let transaction_hash: [u8; 32] = Sha256::digest(&submission.tx_raw).into();
        let handler: Handler = Arc::new(move |request| {
            rpc_result(
                &request,
                &serde_json::json!({
                    "code": 7,
                    "data": "",
                    "log": "bad sequence",
                    "codespace": "sdk",
                    "hash": hex::encode_upper(transaction_hash),
                }),
            )
        });
        let (url, stop, server) = spawn_rpc_server(handler).await;
        let store = store();
        let service = service(Arc::clone(&store), url);

        let error = service.submit(submission).await.unwrap_err();

        assert!(matches!(&*error, Error::CheckTxRejected { code: 7, .. }));
        assert!(store.get_proof(verification_id).await.is_some());

        stop.cancel();
        server.await.unwrap();
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

    fn service(store: Arc<ProofStore>, comet_rpc_url: String) -> SubmissionService {
        let chain = PulsarClient::new(&ChainConfig {
            chain_id: "pulsar-test-1".to_owned(),
            comet_rpc_url,
            request_timeout: Duration::from_secs(1),
        })
        .unwrap();
        SubmissionService::new(store, chain, 1_024, 1_024)
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
