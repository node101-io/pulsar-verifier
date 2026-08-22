use std::{os::unix::fs::PermissionsExt as _, path::PathBuf, process::Stdio, time::Duration};

use async_trait::async_trait;
use barretenberg_rs::generated_types::{CircuitVerify, Command, ProofSystemSettings, Response};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    process::{Child, Command as ProcessCommand},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    config::NoirConfig,
    proof::{Proof, ProofType},
    store::{FAILURE_MESSAGE_MAX_BYTES, VerificationFailure, VerificationVerdict},
};

use super::Verifier;

const BB_VERSION: &str = "5.2.0";
const VERSION_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_VERSION_BYTES: usize = 4096;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

enum ExchangeError {
    Io(std::io::Error),
    Oversized,
}

impl From<std::io::Error> for ExchangeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Isolated Noir verifier backed by one Barretenberg process per attempt.
pub(crate) struct NoirVerifier {
    binary_path: PathBuf,
    home_directory: PathBuf,
    threads_per_job: usize,
}

impl NoirVerifier {
    /// Validates all external runtime prerequisites before workers can start.
    pub(crate) async fn initialize(config: &NoirConfig) -> Result<Self> {
        let metadata = tokio::fs::metadata(&config.binary_path)
            .await
            .map_err(|error| initialization(format!("cannot access bb binary: {error}")))?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
            return Err(initialization(
                "bb binary must be a regular executable file".to_owned(),
            ));
        }
        let crs = config.home_directory.join(".bb-crs");
        if !tokio::fs::metadata(&crs)
            .await
            .is_ok_and(|metadata| metadata.is_dir())
        {
            return Err(initialization(format!(
                "Barretenberg CRS directory is missing: {}",
                crs.display()
            )));
        }

        let version = read_version(&config.binary_path).await?;
        if version != BB_VERSION {
            return Err(initialization(format!(
                "expected bb {BB_VERSION}, got {version}"
            )));
        }

        Ok(Self {
            binary_path: config.binary_path.clone(),
            home_directory: config.home_directory.clone(),
            threads_per_job: config.threads_per_job,
        })
    }
}

#[async_trait]
impl Verifier for NoirVerifier {
    async fn verify(
        &self,
        proof: &Proof,
        cancel: CancellationToken,
    ) -> std::result::Result<VerificationVerdict, VerificationFailure> {
        if proof.proof_type != ProofType::NoirBarretenberg {
            return Err(failure(
                "invalid_noir_artifact",
                "Noir verifier received a different proof type",
                false,
            ));
        }
        let proof_fields = fields(&proof.proof)?;
        let public_inputs = fields(&proof.public_inputs)?;
        let request = encode_request(proof, public_inputs, proof_fields)?;

        classify_response(self.exchange(&request, cancel).await?)
    }
}

impl NoirVerifier {
    async fn exchange(
        &self,
        request: &[u8],
        cancel: CancellationToken,
    ) -> std::result::Result<Response, VerificationFailure> {
        let request_len = u32::try_from(request.len()).map_err(|_| {
            failure(
                "barretenberg_protocol",
                "Barretenberg request exceeds the framing limit",
                false,
            )
        })?;
        let mut child = ProcessCommand::new(&self.binary_path)
            .args(["msgpack", "run"])
            .env("HOME", &self.home_directory)
            .env("HARDWARE_CONCURRENCY", self.threads_per_job.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| unavailable(format!("failed to start Barretenberg: {error}")))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| unavailable("Barretenberg stdin pipe is unavailable"))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| unavailable("Barretenberg stdout pipe is unavailable"))?;

        let exchange = async {
            stdin.write_all(&request_len.to_le_bytes()).await?;
            stdin.write_all(request).await?;
            stdin.flush().await?;
            drop(stdin);

            let response_len = stdout.read_u32_le().await? as usize;
            if response_len > MAX_RESPONSE_BYTES {
                return Err(ExchangeError::Oversized);
            }
            let mut response = vec![0; response_len];
            stdout.read_exact(&mut response).await?;
            Ok::<_, ExchangeError>(response)
        };

        let exchange_result = tokio::select! {
            () = cancel.cancelled() => None,
            result = exchange => Some(result),
        };
        cleanup_process(&mut child).await.map_err(|error| {
            failure(
                "barretenberg_cleanup",
                format!("failed to stop Barretenberg: {error}"),
                true,
            )
        })?;

        let response = match exchange_result {
            None => return Err(unavailable("Barretenberg verification was cancelled")),
            Some(Err(ExchangeError::Oversized)) => {
                return Err(failure(
                    "barretenberg_protocol",
                    "Barretenberg response exceeds 1 MiB",
                    false,
                ));
            }
            Some(Err(ExchangeError::Io(error))) => {
                return Err(unavailable(format!(
                    "Barretenberg pipe closed unexpectedly: {error}"
                )));
            }
            Some(Ok(response)) => response,
        };

        rmp_serde::from_slice(&response).map_err(|error| {
            failure(
                "barretenberg_protocol",
                format!("invalid Barretenberg response: {error}"),
                false,
            )
        })
    }
}

fn fields(bytes: &[u8]) -> std::result::Result<Vec<Vec<u8>>, VerificationFailure> {
    if !bytes.len().is_multiple_of(32) {
        return Err(failure(
            "invalid_noir_artifact",
            format!(
                "Noir proof and public inputs must use 32-byte field framing; got {} bytes",
                bytes.len()
            ),
            false,
        ));
    }
    Ok(bytes
        .as_chunks::<32>()
        .0
        .iter()
        .map(|field| field.to_vec())
        .collect())
}

fn encode_request(
    proof: &Proof,
    public_inputs: Vec<Vec<u8>>,
    proof_fields: Vec<Vec<u8>>,
) -> std::result::Result<Vec<u8>, VerificationFailure> {
    rmp_serde::to_vec_named(&vec![build_command(proof, public_inputs, proof_fields)]).map_err(
        |error| {
            failure(
                "barretenberg_protocol",
                format!("failed to encode Barretenberg request: {error}"),
                false,
            )
        },
    )
}

fn build_command(
    proof: &Proof,
    public_inputs: Vec<Vec<u8>>,
    proof_fields: Vec<Vec<u8>>,
) -> Command {
    let settings = ProofSystemSettings {
        ipa_accumulation: false,
        oracle_hash_type: "poseidon2".to_owned(),
        disable_zk: false,
        optimized_solidity_verifier: false,
    };
    Command::CircuitVerify(CircuitVerify::new(
        proof.verification_key.to_vec(),
        public_inputs,
        proof_fields,
        settings,
    ))
}

fn classify_response(
    response: Response,
) -> std::result::Result<VerificationVerdict, VerificationFailure> {
    match response {
        Response::CircuitVerifyResponse(response) => Ok(if response.verified {
            VerificationVerdict::Valid
        } else {
            VerificationVerdict::Invalid
        }),
        Response::ErrorResponse(response) => Err(failure(
            "barretenberg_error",
            format!("Barretenberg error: {}", response.message),
            false,
        )),
        _ => Err(failure(
            "barretenberg_protocol",
            "Barretenberg returned an unexpected response variant",
            false,
        )),
    }
}

async fn read_version(binary_path: &PathBuf) -> Result<String> {
    let mut child = ProcessCommand::new(binary_path)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| initialization(format!("failed to execute bb --version: {error}")))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| initialization("bb version stdout pipe is unavailable".to_owned()))?;
    let mut output = Vec::new();
    let check = async {
        stdout
            .take(u64::try_from(MAX_VERSION_BYTES + 1).expect("version limit fits u64"))
            .read_to_end(&mut output)
            .await?;
        child.wait().await
    };
    let status = match timeout(VERSION_TIMEOUT, check).await {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            cleanup_process(&mut child).await.map_err(|cleanup| {
                initialization(format!("bb version cleanup failed: {cleanup}"))
            })?;
            return Err(initialization(format!("bb --version failed: {error}")));
        }
        Err(_) => {
            cleanup_process(&mut child)
                .await
                .map_err(|error| initialization(format!("bb version cleanup failed: {error}")))?;
            return Err(initialization(
                "bb --version did not finish within 5 seconds".to_owned(),
            ));
        }
    };
    if !status.success() {
        return Err(initialization(format!("bb --version exited with {status}")));
    }
    if output.len() > MAX_VERSION_BYTES {
        return Err(initialization(
            "bb --version output is too large".to_owned(),
        ));
    }
    String::from_utf8(output)
        .map(|version| version.trim().to_owned())
        .map_err(|error| initialization(format!("bb --version output is not UTF-8: {error}")))
}

async fn cleanup_process(child: &mut Child) -> std::io::Result<()> {
    match child.try_wait()? {
        Some(_) => Ok(()),
        None => child.kill().await,
    }
}

fn unavailable(message: impl Into<String>) -> VerificationFailure {
    failure("barretenberg_unavailable", message, true)
}

fn failure(code: &'static str, message: impl Into<String>, retryable: bool) -> VerificationFailure {
    let mut message = message.into();
    if message.len() > FAILURE_MESSAGE_MAX_BYTES {
        let mut end = FAILURE_MESSAGE_MAX_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    VerificationFailure::new(code, message, retryable)
        .expect("Noir failure must satisfy diagnostic bounds")
}

fn initialization(message: String) -> Error {
    Error::NoirInitialization(message)
}

#[cfg(test)]
mod tests {
    use std::{env, fs, sync::Arc};

    use barretenberg_rs::generated_types::{
        CircuitVerifyResponse, ErrorResponse, ShutdownResponse,
    };
    use bytes::Bytes;
    use tempfile::TempDir;

    use super::*;

    fn proof() -> Proof {
        Proof {
            proof_type: ProofType::NoirBarretenberg,
            proof: Bytes::from(vec![1; 64]),
            public_inputs: Bytes::from(vec![2; 32]),
            verification_key: Bytes::from_static(b"raw verification key"),
        }
    }

    fn executable(directory: &TempDir, body: &str) -> PathBuf {
        let path = directory.path().join("bb");
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&path, permissions).unwrap();
        path
    }

    fn config(directory: &TempDir, binary_path: PathBuf) -> NoirConfig {
        fs::create_dir(directory.path().join(".bb-crs")).unwrap();
        NoirConfig {
            enabled: true,
            binary_path,
            home_directory: directory.path().to_path_buf(),
            threads_per_job: 1,
        }
    }

    #[test]
    fn artifact_fields_require_exact_32_byte_framing() {
        assert!(fields(&[]).unwrap().is_empty());
        assert_eq!(fields(&[0; 64]).unwrap().len(), 2);
        let error = fields(&[0; 31]).unwrap_err();
        assert_eq!(error.code(), "invalid_noir_artifact");
        assert!(!error.retryable());
    }

    #[test]
    fn request_preserves_artifacts_and_canonical_settings() {
        let proof = proof();
        let command = build_command(
            &proof,
            fields(&proof.public_inputs).unwrap(),
            fields(&proof.proof).unwrap(),
        );
        let Command::CircuitVerify(request) = command else {
            panic!("expected CircuitVerify command")
        };

        assert_eq!(request.verification_key, proof.verification_key);
        assert_eq!(request.public_inputs, vec![vec![2; 32]]);
        assert_eq!(request.proof, vec![vec![1; 32], vec![1; 32]]);
        assert_eq!(request.settings.oracle_hash_type, "poseidon2");
        assert!(!request.settings.disable_zk);
        assert!(!request.settings.ipa_accumulation);
        assert!(!request.settings.optimized_solidity_verifier);
    }

    #[test]
    fn response_variants_preserve_verdict_and_failure_semantics() {
        assert_eq!(
            classify_response(Response::CircuitVerifyResponse(CircuitVerifyResponse {
                verified: true,
            })),
            Ok(VerificationVerdict::Valid)
        );
        assert_eq!(
            classify_response(Response::CircuitVerifyResponse(CircuitVerifyResponse {
                verified: false,
            })),
            Ok(VerificationVerdict::Invalid)
        );

        let backend_error = classify_response(Response::ErrorResponse(ErrorResponse {
            message: "x".repeat(300),
        }))
        .unwrap_err();
        assert_eq!(backend_error.code(), "barretenberg_error");
        assert_eq!(backend_error.message().len(), FAILURE_MESSAGE_MAX_BYTES);
        assert!(!backend_error.retryable());

        let protocol_error =
            classify_response(Response::ShutdownResponse(ShutdownResponse {})).unwrap_err();
        assert_eq!(protocol_error.code(), "barretenberg_protocol");
    }

    #[tokio::test]
    async fn initialization_validates_version_binary_and_crs() {
        let directory = TempDir::new().unwrap();
        let binary = executable(
            &directory,
            "if [ \"$1\" = \"--version\" ]; then printf '5.2.0\\n'; fi",
        );
        let config = config(&directory, binary);
        NoirVerifier::initialize(&config).await.unwrap();

        let missing_binary = NoirConfig {
            binary_path: directory.path().join("missing"),
            ..config.clone()
        };
        assert!(matches!(
            NoirVerifier::initialize(&missing_binary).await,
            Err(Error::NoirInitialization(_))
        ));

        fs::remove_dir_all(directory.path().join(".bb-crs")).unwrap();
        assert!(matches!(
            NoirVerifier::initialize(&config).await,
            Err(Error::NoirInitialization(_))
        ));
    }

    #[tokio::test]
    async fn initialization_rejects_non_executable_and_wrong_version() {
        let directory = TempDir::new().unwrap();
        let binary = executable(&directory, "printf '5.1.0\\n'");
        let config = config(&directory, binary.clone());
        assert!(matches!(
            NoirVerifier::initialize(&config).await,
            Err(Error::NoirInitialization(message)) if message.contains("expected bb 5.2.0")
        ));

        let mut permissions = fs::metadata(&binary).unwrap().permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(&binary, permissions).unwrap();
        assert!(matches!(
            NoirVerifier::initialize(&config).await,
            Err(Error::NoirInitialization(message)) if message.contains("executable")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn initialization_times_out_and_reaps_version_process() {
        let directory = TempDir::new().unwrap();
        let binary = executable(&directory, "exec sleep 30");
        let config = config(&directory, binary);
        assert!(matches!(
            NoirVerifier::initialize(&config).await,
            Err(Error::NoirInitialization(message)) if message.contains("within 5 seconds")
        ));
    }

    #[tokio::test]
    async fn process_receives_environment_and_is_stopped_on_cancellation() {
        let directory = TempDir::new().unwrap();
        let binary = executable(
            &directory,
            r#"if [ "$1" = "--version" ]; then printf '5.2.0\n'; exit 0; fi
printf '%s\n%s\n' "$HOME" "$HARDWARE_CONCURRENCY" > "$HOME/environment"
exec sleep 30"#,
        );
        let verifier = Arc::new(
            NoirVerifier::initialize(&config(&directory, binary))
                .await
                .unwrap(),
        );
        let cancel = CancellationToken::new();
        let task = {
            let verifier = Arc::clone(&verifier);
            let cancel = cancel.clone();
            tokio::spawn(async move { verifier.verify(&proof(), cancel).await })
        };
        let environment = directory.path().join("environment");
        timeout(Duration::from_secs(2), async {
            while !environment.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        cancel.cancel();
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.code(), "barretenberg_unavailable");
        assert_eq!(
            fs::read_to_string(environment).unwrap(),
            format!("{}\n1\n", directory.path().display())
        );
    }

    #[tokio::test]
    async fn malformed_and_oversized_responses_are_protocol_failures() {
        for (script, expected_fragment) in [
            (
                r"printf '\001\000\000\000\377'",
                "invalid Barretenberg response",
            ),
            (r"printf '\001\000\020\000'", "exceeds 1 MiB"),
        ] {
            let directory = TempDir::new().unwrap();
            let binary = executable(&directory, script);
            let verifier = NoirVerifier {
                binary_path: binary,
                home_directory: directory.path().to_path_buf(),
                threads_per_job: 1,
            };
            let error = verifier
                .verify(&proof(), CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.code(), "barretenberg_protocol");
            assert!(error.message().contains(expected_fragment));
        }
    }

    #[tokio::test]
    async fn unavailable_backend_starts_a_fresh_process_for_each_attempt() {
        let directory = TempDir::new().unwrap();
        let binary = executable(
            &directory,
            r#"count_file="$HOME/count"
count=0
if [ -f "$count_file" ]; then count=$(cat "$count_file"); fi
count=$((count + 1))
printf '%s' "$count" > "$count_file"
exit 1"#,
        );
        let verifier = NoirVerifier {
            binary_path: binary,
            home_directory: directory.path().to_path_buf(),
            threads_per_job: 1,
        };

        for _ in 0..2 {
            let error = verifier
                .verify(&proof(), CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.code(), "barretenberg_unavailable");
            assert!(error.retryable());
        }
        assert_eq!(
            fs::read_to_string(directory.path().join("count")).unwrap(),
            "2"
        );
    }

    #[tokio::test]
    #[ignore = "requires bb 5.2.0 and a pre-provisioned CRS"]
    async fn bb_5_2_0_verifies_pinned_noir_artifacts() {
        const FIXTURE: &str = "tests/fixtures/noir/bb-5.2.0";
        let config = NoirConfig {
            enabled: true,
            binary_path: env::var_os("PULSAR_BB_PATH")
                .map(PathBuf::from)
                .expect("PULSAR_BB_PATH must point to bb 5.2.0"),
            home_directory: env::var_os("PULSAR_BB_HOME")
                .map(PathBuf::from)
                .expect("PULSAR_BB_HOME must contain .bb-crs"),
            threads_per_job: 1,
        };
        let verifier = NoirVerifier::initialize(&config).await.unwrap();
        let candidate = Proof {
            proof_type: ProofType::NoirBarretenberg,
            proof: Bytes::from(tokio::fs::read(format!("{FIXTURE}/proof")).await.unwrap()),
            public_inputs: Bytes::from(
                tokio::fs::read(format!("{FIXTURE}/public_inputs"))
                    .await
                    .unwrap(),
            ),
            verification_key: Bytes::from(tokio::fs::read(format!("{FIXTURE}/vk")).await.unwrap()),
        };

        assert_eq!(
            verifier.verify(&candidate, CancellationToken::new()).await,
            Ok(VerificationVerdict::Valid)
        );
        let mut invalid_proof = candidate.proof.to_vec();
        invalid_proof[0] ^= 1;
        let invalid = Proof {
            proof: Bytes::from(invalid_proof),
            ..candidate.clone()
        };
        assert_eq!(
            verifier.verify(&invalid, CancellationToken::new()).await,
            Ok(VerificationVerdict::Invalid)
        );
        let mut public_inputs = candidate.public_inputs.to_vec();
        public_inputs[0] ^= 1;
        let invalid_inputs = Proof {
            public_inputs: Bytes::from(public_inputs),
            ..candidate
        };
        assert_eq!(
            verifier
                .verify(&invalid_inputs, CancellationToken::new())
                .await,
            Ok(VerificationVerdict::Invalid)
        );

        let malformed = Proof {
            proof: Bytes::from_static(b"not field framed"),
            ..invalid_inputs.clone()
        };
        let malformed_error = verifier
            .verify(&malformed, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(malformed_error.code(), "invalid_noir_artifact");

        let wrong_key = Proof {
            verification_key: Bytes::new(),
            ..invalid_inputs
        };
        assert_eq!(
            verifier.verify(&wrong_key, CancellationToken::new()).await,
            Ok(VerificationVerdict::Invalid)
        );
    }
}
