use std::{io, path::PathBuf, time::Duration};

use thiserror::Error;

use crate::proof::{ProofHash, ProofType};

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

    #[error("P2P protocol error: {0}")]
    P2pProtocol(String),

    #[error("P2P driver error: {0}")]
    P2pDriver(String),

    #[error("invalid proof hash: {0}")]
    InvalidProofHash(String),

    #[error("invalid proof type: {0}")]
    InvalidProofType(String),

    #[error("proof store error: {0}")]
    ProofStore(String),

    #[error("proof {proof_hash:?} is {actual_bytes} bytes; maximum is {max_bytes}")]
    ProofTooLarge {
        proof_hash: ProofHash,
        actual_bytes: usize,
        max_bytes: usize,
    },

    #[error("proof bytes do not match expected hash {0:?}")]
    ProofHashMismatch(ProofHash),

    #[error("proof {proof_hash:?} type conflict: existing {existing}, incoming {incoming}")]
    ProofTypeConflict {
        proof_hash: ProofHash,
        existing: ProofType,
        incoming: ProofType,
    },

    #[error("proof {0:?} has not been observed on-chain")]
    ProofNotObserved(ProofHash),

    #[error("invalid verification transition for proof {proof_hash:?}: {reason}")]
    InvalidVerificationTransition {
        proof_hash: ProofHash,
        reason: String,
    },
}
