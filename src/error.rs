use std::{io, net::SocketAddr, path::PathBuf, time::Duration};

use thiserror::Error;

use crate::proof::VerificationId;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors are typed so the CLI can provide actionable lifecycle failures.
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to read config {path}: {source}")]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to parse config {path}: {source}")]
    ConfigParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("another verifier is already listening on {0}")]
    AlreadyRunning(PathBuf),

    #[error("no verifier is listening on {0}")]
    NotRunning(PathBuf),

    #[error("unsafe control socket path {path}: {reason}")]
    UnsafeSocketPath { path: PathBuf, reason: String },

    #[error("control socket operation failed for {path}: {source}")]
    ControlIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("control request timed out for {0}")]
    ControlTimeout(PathBuf),

    #[error("unexpected control response: {0}")]
    ControlProtocol(String),

    #[error("verifier did not stop within {0:?}")]
    ShutdownTimeout(Duration),

    #[error("failed to install process signal handler: {0}")]
    Signal(#[source] io::Error),

    #[error("runtime task failed: {0}")]
    Task(#[from] tokio::task::JoinError),

    #[error("failed to initialize telemetry: {0}")]
    Telemetry(String),

    #[error("P2P identity error: {0}")]
    P2pIdentity(String),

    #[error("P2P authorization error: {0}")]
    P2pAuthorization(String),

    #[error("Pulsar chain client error: {0}")]
    Chain(String),

    #[error("invalid proof submission: {0}")]
    InvalidSubmission(String),

    #[error("transaction is {actual_bytes} bytes; maximum is {max_bytes}")]
    TransactionTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },

    #[error("transaction {transaction_hash} was rejected by CheckTx ({codespace}:{code}): {log}")]
    CheckTxRejected {
        transaction_hash: String,
        codespace: String,
        code: u32,
        log: String,
    },

    #[error("CometBFT returned an unexpected transaction hash")]
    TransactionHashMismatch,

    #[error("CometBFT chain ID mismatch: expected {expected}, got {actual}")]
    ChainIdMismatch { expected: String, actual: String },

    #[error("invalid committed verification contract: {0}")]
    InvalidChainContract(String),

    #[error("local validator {0} is not in the active validator set")]
    LocalValidatorRemoved(String),

    #[error("P2P protocol error: {0}")]
    P2pProtocol(String),

    #[error("P2P driver error: {0}")]
    P2pDriver(String),

    #[error("P2P driver is draining")]
    P2pDraining,

    #[error("P2P driver is closed")]
    P2pDriverClosed,

    #[error("{0} task exited unexpectedly")]
    TaskExitedUnexpectedly(&'static str),

    #[error("invalid verification ID: {0}")]
    InvalidVerificationId(String),

    #[error("unsupported proof type: {0}")]
    UnsupportedProofType(i32),

    #[error("proof store error: {0}")]
    ProofStore(String),

    #[error("proof {verification_id} is {actual_bytes} bytes; maximum is {max_bytes}")]
    ProofTooLarge {
        verification_id: VerificationId,
        actual_bytes: usize,
        max_bytes: usize,
    },

    #[error("proof does not match expected verification ID {0}")]
    VerificationIdMismatch(VerificationId),

    #[error("verification {0} has not been observed on-chain")]
    ProofNotObserved(VerificationId),

    #[error("invalid verification transition for {verification_id}: {reason}")]
    InvalidVerificationTransition {
        verification_id: VerificationId,
        reason: String,
    },

    #[error("invalid verification failure: {0}")]
    InvalidVerificationFailure(String),

    #[error("a verifier is already registered for {0:?}")]
    DuplicateVerifier(crate::proof::ProofType),

    #[error("failed to initialize Noir verifier: {0}")]
    NoirInitialization(String),

    #[error("invalid verification service request: {0}")]
    InvalidVerificationRequest(String),

    #[error("failed to bind RPC server on {address}: {source}")]
    RpcBind {
        address: SocketAddr,
        #[source]
        source: io::Error,
    },

    #[error("RPC server failed: {0}")]
    RpcServer(#[source] tonic::transport::Error),
}
