use std::{io, path::PathBuf, time::Duration};

use thiserror::Error;

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
}
