use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;

use crate::{Error, Result};

/// Validated application configuration consumed by runtime components.
#[derive(Debug, Clone)]
pub struct Config {
    pub runtime: RuntimeConfig,
}

/// Process lifecycle settings shared by the `run` and `stop` commands.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub control_socket: PathBuf,
    pub shutdown_timeout: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    runtime: FileRuntimeConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRuntimeConfig {
    control_socket: PathBuf,
    shutdown_timeout_secs: u64,
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

        Ok(Self {
            runtime: RuntimeConfig {
                control_socket: file.runtime.control_socket,
                shutdown_timeout: Duration::from_secs(file.runtime.shutdown_timeout_secs),
            },
        })
    }
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
}
