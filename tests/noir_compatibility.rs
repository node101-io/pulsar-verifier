use std::{env, path::Path, process::Stdio, time::Duration};

use barretenberg_rs::generated_types::{
    CircuitVerify, Command, ErrorResponse, ProofSystemSettings, Response,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command as ProcessCommand,
    time::timeout,
};

const FIXTURE: &str = "tests/fixtures/noir/bb-5.2.0";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Eq, PartialEq)]
enum VerifyResult {
    Verdict(bool),
    BackendFailure,
    TimedOut,
}

#[tokio::test]
#[ignore = "requires bb 5.2.0 and a pre-provisioned CRS"]
async fn bb_5_2_0_verifies_the_pinned_noir_artifacts() {
    let bb = env::var("PULSAR_BB_PATH").expect("PULSAR_BB_PATH must point to bb 5.2.0");
    let home = env::var("PULSAR_BB_HOME")
        .expect("PULSAR_BB_HOME must contain a pre-provisioned .bb-crs directory");
    assert!(Path::new(&home).join(".bb-crs").is_dir());
    assert_eq!(bb_version(&bb).await, "5.2.0");

    let verification_key = tokio::fs::read(format!("{FIXTURE}/vk")).await.unwrap();
    let public_inputs = fields(
        &tokio::fs::read(format!("{FIXTURE}/public_inputs"))
            .await
            .unwrap(),
    )
    .unwrap();
    let proof = fields(&tokio::fs::read(format!("{FIXTURE}/proof")).await.unwrap()).unwrap();

    assert_eq!(
        verify(
            &bb,
            &home,
            verification_key.clone(),
            public_inputs.clone(),
            proof.clone(),
            Duration::from_secs(10),
        )
        .await,
        VerifyResult::Verdict(true)
    );

    let mut invalid_proof = proof.clone();
    invalid_proof[0][0] ^= 1;
    assert_eq!(
        verify(
            &bb,
            &home,
            verification_key.clone(),
            public_inputs.clone(),
            invalid_proof,
            Duration::from_secs(10),
        )
        .await,
        VerifyResult::Verdict(false)
    );

    assert_eq!(
        verify(
            &bb,
            &home,
            verification_key,
            public_inputs,
            proof,
            Duration::ZERO,
        )
        .await,
        VerifyResult::TimedOut
    );
}

#[test]
fn backend_error_response_is_not_an_invalid_verdict() {
    assert_eq!(
        classify_response(Response::ErrorResponse(ErrorResponse {
            message: "backend crashed".to_owned(),
        })),
        VerifyResult::BackendFailure
    );
}

async fn bb_version(bb: &str) -> String {
    let output = ProcessCommand::new(bb)
        .arg("--version")
        .output()
        .await
        .expect("failed to execute bb");
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .expect("bb version output must be UTF-8")
        .trim()
        .to_owned()
}

fn fields(bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if !bytes.len().is_multiple_of(32) {
        return Err(format!(
            "artifact length {} is not a multiple of 32",
            bytes.len()
        ));
    }
    Ok(bytes.chunks_exact(32).map(<[u8]>::to_vec).collect())
}

async fn verify(
    bb: &str,
    home: &str,
    verification_key: Vec<u8>,
    public_inputs: Vec<Vec<u8>>,
    proof: Vec<Vec<u8>>,
    deadline: Duration,
) -> VerifyResult {
    let settings = ProofSystemSettings {
        ipa_accumulation: false,
        oracle_hash_type: "poseidon2".to_owned(),
        disable_zk: false,
        optimized_solidity_verifier: false,
    };
    let request = rmp_serde::to_vec_named(&vec![Command::CircuitVerify(CircuitVerify::new(
        verification_key,
        public_inputs,
        proof,
        settings,
    ))])
    .expect("typed request must encode");
    let Ok(request_len) = u32::try_from(request.len()) else {
        return VerifyResult::BackendFailure;
    };

    let mut child = ProcessCommand::new(bb)
        .args(["msgpack", "run"])
        .env("HOME", home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("failed to start bb");
    let mut stdin = child.stdin.take().expect("bb stdin must be piped");
    let mut stdout = child.stdout.take().expect("bb stdout must be piped");

    let exchange = async {
        stdin.write_all(&request_len.to_le_bytes()).await?;
        stdin.write_all(&request).await?;
        stdin.flush().await?;
        let response_len = stdout.read_u32_le().await? as usize;
        if response_len > MAX_RESPONSE_BYTES {
            return Err(std::io::Error::other("bb response exceeds limit"));
        }
        let mut response = vec![0; response_len];
        stdout.read_exact(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    };

    let result = match timeout(deadline, exchange).await {
        Ok(Ok(response)) => rmp_serde::from_slice::<Response>(&response)
            .map_or(VerifyResult::BackendFailure, classify_response),
        Ok(Err(_)) => VerifyResult::BackendFailure,
        Err(_) => VerifyResult::TimedOut,
    };

    if child.kill().await.is_err() {
        return VerifyResult::BackendFailure;
    }
    if child.wait().await.is_err() {
        return VerifyResult::BackendFailure;
    }
    result
}

fn classify_response(response: Response) -> VerifyResult {
    match response {
        Response::CircuitVerifyResponse(response) => VerifyResult::Verdict(response.verified),
        _ => VerifyResult::BackendFailure,
    }
}
