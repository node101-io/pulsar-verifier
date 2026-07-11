use tracing_subscriber::EnvFilter;

use crate::{Error, Result};

/// Installs structured logs, with `RUST_LOG` taking precedence over `info`.
pub fn init() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init()
        .map_err(|error| Error::Telemetry(error.to_string()))
}
