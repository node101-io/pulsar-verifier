pub mod cli;
pub mod config;
pub mod p2p;

mod app;
mod control;
mod error;
mod telemetry;

use app::App;
use cli::{Cli, Command};
use config::Config;
pub use error::{Error, Result};

/// Executes the selected command after loading its shared configuration.
///
/// # Errors
///
/// Returns an error when configuration, telemetry, process lifecycle, or the
/// local control socket cannot complete the selected command.
pub async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Run(args) => {
            let config = Config::from_file(&args.config)?;
            telemetry::init()?;
            App::run(config).await
        }
        Command::Stop(args) => {
            let config = Config::from_file(&args.config)?;
            control::request_shutdown(&config.runtime).await
        }
    }
}
