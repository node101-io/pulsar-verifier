use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use libp2p::Multiaddr;
use reqwest::Url;
use serde::Deserialize;

use crate::{Error, Result};

/// Validated application configuration consumed by runtime components.
#[derive(Debug, Clone)]
pub struct Config {
    pub runtime: RuntimeConfig,
    pub proof_store: ProofStoreConfig,
    pub p2p: P2pConfig,
    pub verification: VerificationConfig,
    pub rpc: RpcConfig,
}

/// Loopback-only chain-facing gRPC server settings.
#[derive(Debug, Clone, Copy)]
pub struct RpcConfig {
    pub enabled: bool,
    pub listen_address: SocketAddr,
}

impl RpcConfig {
    #[cfg(test)]
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            listen_address: "127.0.0.1:50051".parse().expect("valid test address"),
        }
    }
}

/// Resource limits for asynchronous proof verification jobs.
#[derive(Debug, Clone, Copy)]
pub struct VerificationConfig {
    pub max_concurrent_jobs: usize,
    pub job_timeout: Duration,
    pub max_retries: u32,
    pub retry_backoff: Duration,
}

/// Process lifecycle settings shared by the `run` and `stop` commands.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub control_socket: PathBuf,
    pub shutdown_timeout: Duration,
}

/// Memory and lifecycle limits for the process-local proof cache.
#[derive(Debug, Clone, Copy)]
pub struct ProofStoreConfig {
    pub max_capacity_bytes: u64,
    pub max_proof_bytes: usize,
    pub terminal_retention: Duration,
    pub event_buffer: usize,
}

/// Validated networking settings used only when P2P is explicitly enabled.
#[derive(Debug, Clone)]
pub struct P2pConfig {
    pub enabled: bool,
    pub chain_id: String,
    pub listen_addresses: Vec<Multiaddr>,
    pub bootnodes: Vec<Multiaddr>,
    pub validator_key_path: PathBuf,
    pub comet_rpc_url: Url,
    pub comet_rpc_timeout: Duration,
    pub max_availability_message_bytes: usize,
    pub max_proof_bytes: usize,
    pub proof_request_timeout: Duration,
    pub command_buffer: usize,
    pub event_buffer: usize,
}

impl P2pConfig {
    #[cfg(test)]
    pub(crate) fn disabled() -> Self {
        validate_p2p(FileP2pConfig::default(), 8 * 1024 * 1024)
            .expect("default P2P config must be valid")
    }
}

impl ProofStoreConfig {
    #[cfg(test)]
    pub(crate) fn test_default() -> Self {
        validate_proof_store(FileProofStoreConfig::default())
            .expect("default proof store config must be valid")
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    runtime: FileRuntimeConfig,
    #[serde(default)]
    proof_store: FileProofStoreConfig,
    #[serde(default)]
    p2p: FileP2pConfig,
    #[serde(default)]
    verification: FileVerificationConfig,
    #[serde(default)]
    rpc: FileRpcConfig,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileRpcConfig {
    enabled: bool,
    listen_address: String,
}

impl Default for FileRpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_address: "127.0.0.1:50051".to_owned(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(default, deny_unknown_fields)]
struct FileVerificationConfig {
    max_concurrent_jobs: usize,
    job_timeout_secs: u64,
    max_retries: u32,
    retry_backoff_millis: u64,
}

impl Default for FileVerificationConfig {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: 2,
            job_timeout_secs: 30,
            max_retries: 2,
            retry_backoff_millis: 250,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(default, deny_unknown_fields)]
struct FileProofStoreConfig {
    max_capacity_bytes: u64,
    max_proof_bytes: usize,
    terminal_retention_secs: u64,
    event_buffer: usize,
}

impl Default for FileProofStoreConfig {
    fn default() -> Self {
        Self {
            max_capacity_bytes: 512 * 1024 * 1024,
            max_proof_bytes: 8 * 1024 * 1024,
            terminal_retention_secs: 15 * 60,
            event_buffer: 256,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRuntimeConfig {
    control_socket: PathBuf,
    shutdown_timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileP2pConfig {
    enabled: bool,
    chain_id: String,
    listen_addresses: Vec<String>,
    bootnodes: Vec<String>,
    validator_key_path: PathBuf,
    comet_rpc_url: String,
    comet_rpc_timeout_secs: u64,
    max_availability_message_bytes: usize,
    proof_request_timeout_secs: u64,
    command_buffer: usize,
    event_buffer: usize,
}

impl Default for FileP2pConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            chain_id: String::new(),
            listen_addresses: Vec::new(),
            bootnodes: Vec::new(),
            validator_key_path: PathBuf::new(),
            comet_rpc_url: "http://127.0.0.1:26657".to_owned(),
            comet_rpc_timeout_secs: 5,
            max_availability_message_bytes: 64 * 1024,
            proof_request_timeout_secs: 10,
            command_buffer: 64,
            event_buffer: 256,
        }
    }
}

impl Config {
    /// Reads and validates TOML so CLI flags never silently fall back to defaults.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, TOML cannot be decoded, or
    /// runtime settings violate the required socket path and timeout invariants.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| Error::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
        let file: FileConfig = toml::from_str(&contents).map_err(|source| Error::ConfigParse {
            path: path.to_path_buf(),
            source,
        })?;

        if !file.runtime.control_socket.is_absolute() {
            return Err(Error::InvalidConfig(
                "runtime.control_socket must be an absolute path".to_owned(),
            ));
        }
        if file.runtime.shutdown_timeout_secs == 0 {
            return Err(Error::InvalidConfig(
                "runtime.shutdown_timeout_secs must be greater than zero".to_owned(),
            ));
        }

        let runtime = RuntimeConfig {
            control_socket: file.runtime.control_socket,
            shutdown_timeout: Duration::from_secs(file.runtime.shutdown_timeout_secs),
        };
        let proof_store = validate_proof_store(file.proof_store)?;
        let p2p = validate_p2p(file.p2p, proof_store.max_proof_bytes)?;
        let verification = validate_verification(file.verification)?;
        let rpc = validate_rpc(&file.rpc)?;
        if p2p.enabled && runtime.shutdown_timeout <= p2p.proof_request_timeout {
            return Err(Error::InvalidConfig(
                "runtime.shutdown_timeout_secs must be greater than p2p.proof_request_timeout_secs when P2P is enabled"
                    .to_owned(),
            ));
        }

        Ok(Self {
            runtime,
            proof_store,
            p2p,
            verification,
            rpc,
        })
    }
}

fn validate_rpc(file: &FileRpcConfig) -> Result<RpcConfig> {
    let listen_address = file.listen_address.parse::<SocketAddr>().map_err(|error| {
        Error::InvalidConfig(format!(
            "rpc.listen_address must be a literal IP and port: {error}"
        ))
    })?;
    if file.enabled && listen_address.port() == 0 {
        return Err(Error::InvalidConfig(
            "rpc.listen_address port must be greater than zero when RPC is enabled".to_owned(),
        ));
    }
    if file.enabled && !listen_address.ip().is_loopback() {
        return Err(Error::InvalidConfig(
            "rpc.listen_address must use a loopback IP when RPC is enabled".to_owned(),
        ));
    }
    Ok(RpcConfig {
        enabled: file.enabled,
        listen_address,
    })
}

fn validate_verification(file: FileVerificationConfig) -> Result<VerificationConfig> {
    if !(1..=256).contains(&file.max_concurrent_jobs) {
        return Err(Error::InvalidConfig(
            "verification.max_concurrent_jobs must be between 1 and 256".to_owned(),
        ));
    }
    if file.job_timeout_secs == 0 {
        return Err(Error::InvalidConfig(
            "verification.job_timeout_secs must be greater than zero".to_owned(),
        ));
    }
    if file.max_retries > 10 {
        return Err(Error::InvalidConfig(
            "verification.max_retries must not exceed 10".to_owned(),
        ));
    }
    if file.max_retries > 0 && file.retry_backoff_millis == 0 {
        return Err(Error::InvalidConfig(
            "verification.retry_backoff_millis must be greater than zero when retries are enabled"
                .to_owned(),
        ));
    }

    Ok(VerificationConfig {
        max_concurrent_jobs: file.max_concurrent_jobs,
        job_timeout: Duration::from_secs(file.job_timeout_secs),
        max_retries: file.max_retries,
        retry_backoff: Duration::from_millis(file.retry_backoff_millis),
    })
}

fn validate_proof_store(file: FileProofStoreConfig) -> Result<ProofStoreConfig> {
    if file.max_capacity_bytes == 0
        || file.max_proof_bytes == 0
        || file.terminal_retention_secs == 0
        || file.event_buffer == 0
    {
        return Err(Error::InvalidConfig(
            "proof store capacity, proof limit, retention, and event buffer must be greater than zero"
                .to_owned(),
        ));
    }
    if u64::try_from(file.max_proof_bytes).unwrap_or(u64::MAX) > file.max_capacity_bytes {
        return Err(Error::InvalidConfig(
            "proof_store.max_proof_bytes must not exceed max_capacity_bytes".to_owned(),
        ));
    }

    Ok(ProofStoreConfig {
        max_capacity_bytes: file.max_capacity_bytes,
        max_proof_bytes: file.max_proof_bytes,
        terminal_retention: Duration::from_secs(file.terminal_retention_secs),
        event_buffer: file.event_buffer,
    })
}

fn validate_p2p(file: FileP2pConfig, max_proof_bytes: usize) -> Result<P2pConfig> {
    if file.enabled {
        if file.chain_id.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "p2p.chain_id must not be empty when P2P is enabled".to_owned(),
            ));
        }
        if file.listen_addresses.is_empty() {
            return Err(Error::InvalidConfig(
                "p2p.listen_addresses must not be empty when P2P is enabled".to_owned(),
            ));
        }
        if !file.validator_key_path.is_absolute() {
            return Err(Error::InvalidConfig(
                "p2p.validator_key_path must be absolute when P2P is enabled".to_owned(),
            ));
        }
    }

    let listen_addresses = parse_multiaddrs("p2p.listen_addresses", file.listen_addresses)?;
    let bootnodes = parse_multiaddrs("p2p.bootnodes", file.bootnodes)?;
    let comet_rpc_url = Url::parse(&file.comet_rpc_url)
        .map_err(|error| Error::InvalidConfig(format!("p2p.comet_rpc_url is invalid: {error}")))?;

    if !matches!(comet_rpc_url.scheme(), "http" | "https") {
        return Err(Error::InvalidConfig(
            "p2p.comet_rpc_url must use http or https".to_owned(),
        ));
    }
    if file.comet_rpc_timeout_secs == 0
        || file.proof_request_timeout_secs == 0
        || file.max_availability_message_bytes == 0
        || file.command_buffer == 0
        || file.event_buffer == 0
    {
        return Err(Error::InvalidConfig(
            "P2P timeouts, message limits, and channel capacities must be greater than zero"
                .to_owned(),
        ));
    }

    Ok(P2pConfig {
        enabled: file.enabled,
        chain_id: file.chain_id,
        listen_addresses,
        bootnodes,
        validator_key_path: file.validator_key_path,
        comet_rpc_url,
        comet_rpc_timeout: Duration::from_secs(file.comet_rpc_timeout_secs),
        max_availability_message_bytes: file.max_availability_message_bytes,
        max_proof_bytes,
        proof_request_timeout: Duration::from_secs(file.proof_request_timeout_secs),
        command_buffer: file.command_buffer,
        event_buffer: file.event_buffer,
    })
}

fn parse_multiaddrs(field: &str, values: Vec<String>) -> Result<Vec<Multiaddr>> {
    values
        .into_iter()
        .map(|value| {
            value.parse().map_err(|error| {
                Error::InvalidConfig(format!(
                    "{field} contains invalid multiaddr {value}: {error}"
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    fn config_file(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    #[test]
    fn loads_valid_config() {
        let file = config_file(
            r#"
                [runtime]
                control_socket = "/tmp/pulsar-verifier-test.sock"
                shutdown_timeout_secs = 7
            "#,
        );

        let config = Config::from_file(file.path()).unwrap();

        assert_eq!(
            config.runtime.control_socket,
            PathBuf::from("/tmp/pulsar-verifier-test.sock")
        );
        assert_eq!(config.runtime.shutdown_timeout, Duration::from_secs(7));
        assert_eq!(config.proof_store.max_capacity_bytes, 512 * 1024 * 1024);
        assert_eq!(config.proof_store.max_proof_bytes, 8 * 1024 * 1024);
        assert_eq!(
            config.proof_store.terminal_retention,
            Duration::from_secs(900)
        );
        assert!(!config.p2p.enabled);
        assert_eq!(config.verification.max_concurrent_jobs, 2);
        assert_eq!(config.verification.job_timeout, Duration::from_secs(30));
        assert_eq!(config.verification.max_retries, 2);
        assert_eq!(
            config.verification.retry_backoff,
            Duration::from_millis(250)
        );
        assert!(!config.rpc.enabled);
        assert_eq!(
            config.rpc.listen_address,
            "127.0.0.1:50051".parse().unwrap()
        );
    }

    #[test]
    fn rejects_relative_socket_path() {
        let file = config_file(
            r#"
                [runtime]
                control_socket = "run/control.sock"
                shutdown_timeout_secs = 10
            "#,
        );

        assert!(matches!(
            Config::from_file(file.path()),
            Err(Error::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_zero_timeout() {
        let file = config_file(
            r#"
                [runtime]
                control_socket = "/tmp/control.sock"
                shutdown_timeout_secs = 0
            "#,
        );

        assert!(matches!(
            Config::from_file(file.path()),
            Err(Error::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_unknown_fields() {
        let file = config_file(
            r#"
                [runtime]
                control_socket = "/tmp/control.sock"
                shutdown_timeout_secs = 10
                typo = true
            "#,
        );

        assert!(matches!(
            Config::from_file(file.path()),
            Err(Error::ConfigParse { .. })
        ));
    }

    #[test]
    fn reports_missing_file() {
        assert!(matches!(
            Config::from_file("/definitely/missing/pulsar-verifier.toml"),
            Err(Error::ConfigRead { .. })
        ));
    }

    #[test]
    fn loads_enabled_p2p_config() {
        let file = config_file(
            r#"
                [runtime]
                control_socket = "/tmp/control.sock"
                shutdown_timeout_secs = 15

                [p2p]
                enabled = true
                chain_id = "pulsar-test"
                listen_addresses = ["/ip4/0.0.0.0/tcp/39000"]
                validator_key_path = "/tmp/priv_validator_key.json"
            "#,
        );

        let config = Config::from_file(file.path()).unwrap();

        assert!(config.p2p.enabled);
        assert_eq!(config.p2p.chain_id, "pulsar-test");
        assert_eq!(config.p2p.max_proof_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn rejects_p2p_request_timeout_without_shutdown_margin() {
        let file = config_file(
            r#"
                [runtime]
                control_socket = "/tmp/control.sock"
                shutdown_timeout_secs = 10

                [p2p]
                enabled = true
                chain_id = "pulsar-test"
                listen_addresses = ["/ip4/0.0.0.0/tcp/39000"]
                validator_key_path = "/tmp/priv_validator_key.json"
                proof_request_timeout_secs = 10
            "#,
        );

        assert!(matches!(
            Config::from_file(file.path()),
            Err(Error::InvalidConfig(message))
                if message.contains("shutdown_timeout_secs must be greater")
        ));
    }

    #[test]
    fn rejects_enabled_p2p_without_chain_id() {
        let file = config_file(
            r#"
                [runtime]
                control_socket = "/tmp/control.sock"
                shutdown_timeout_secs = 10

                [p2p]
                enabled = true
                listen_addresses = ["/ip4/0.0.0.0/tcp/39000"]
                validator_key_path = "/tmp/priv_validator_key.json"
            "#,
        );

        assert!(matches!(
            Config::from_file(file.path()),
            Err(Error::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_invalid_proof_store_limits() {
        let file = config_file(
            r#"
                [runtime]
                control_socket = "/tmp/control.sock"
                shutdown_timeout_secs = 10

                [proof_store]
                max_capacity_bytes = 1024
                max_proof_bytes = 2048
            "#,
        );

        assert!(matches!(
            Config::from_file(file.path()),
            Err(Error::InvalidConfig(_))
        ));
    }

    #[test]
    fn rejects_invalid_verification_limits() {
        for verification in [
            "max_concurrent_jobs = 0",
            "max_concurrent_jobs = 257",
            "job_timeout_secs = 0",
            "max_retries = 11",
            "max_retries = 1\nretry_backoff_millis = 0",
        ] {
            let file = config_file(&format!(
                r#"
                    [runtime]
                    control_socket = "/tmp/control.sock"
                    shutdown_timeout_secs = 10

                    [verification]
                    {verification}
                "#,
            ));

            assert!(matches!(
                Config::from_file(file.path()),
                Err(Error::InvalidConfig(_))
            ));
        }
    }

    #[test]
    fn loads_enabled_loopback_rpc() {
        let file = config_file(
            r#"
                [runtime]
                control_socket = "/tmp/control.sock"
                shutdown_timeout_secs = 10

                [rpc]
                enabled = true
                listen_address = "[::1]:50051"
            "#,
        );

        let config = Config::from_file(file.path()).unwrap();
        assert!(config.rpc.enabled);
        assert!(config.rpc.listen_address.ip().is_loopback());
    }

    #[test]
    fn rejects_unsafe_or_malformed_rpc_addresses() {
        for address in [
            "0.0.0.0:50051",
            "192.168.1.10:50051",
            "127.0.0.1:0",
            "localhost:50051",
            "127.0.0.1",
        ] {
            let file = config_file(&format!(
                r#"
                    [runtime]
                    control_socket = "/tmp/control.sock"
                    shutdown_timeout_secs = 10

                    [rpc]
                    enabled = true
                    listen_address = "{address}"
                "#,
            ));

            assert!(matches!(
                Config::from_file(file.path()),
                Err(Error::InvalidConfig(_))
            ));
        }
    }
}
