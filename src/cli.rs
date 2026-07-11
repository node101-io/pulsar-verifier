use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

const DEFAULT_CONFIG_PATH: &str = "config/default.toml";

/// Command-line entrypoint for the verifier process.
#[derive(Debug, Parser)]
#[command(name = "pulsar-verifier", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// Small process-lifecycle command surface kept stable for future components.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Runs the verifier in the foreground.
    Run(ConfigArgs),
    /// Requests graceful shutdown from a running verifier.
    Stop(ConfigArgs),
}

/// Arguments shared by commands that need to locate the same runtime socket.
#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// Path to the verifier TOML configuration.
    #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
    pub config: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_run_with_default_config() {
        let cli = Cli::try_parse_from(["pulsar-verifier", "run"]).unwrap();

        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        assert_eq!(args.config, PathBuf::from(DEFAULT_CONFIG_PATH));
    }

    #[test]
    fn parses_stop_with_explicit_config() {
        let cli = Cli::try_parse_from([
            "pulsar-verifier",
            "stop",
            "--config",
            "/etc/pulsar-verifier/config.toml",
        ])
        .unwrap();

        let Command::Stop(args) = cli.command else {
            panic!("expected stop command");
        };
        assert_eq!(
            args.config,
            PathBuf::from("/etc/pulsar-verifier/config.toml")
        );
    }

    #[test]
    fn rejects_unknown_command() {
        assert!(Cli::try_parse_from(["pulsar-verifier", "start"]).is_err());
    }
}
