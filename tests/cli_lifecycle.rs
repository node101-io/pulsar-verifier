use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tempfile::TempDir;

const PROCESS_TIMEOUT: Duration = Duration::from_secs(5);

struct ChildGuard(Child);

impl ChildGuard {
    fn wait_for_exit(&mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "verifier process did not exit");
            thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_pulsar-verifier")
}

fn write_config(temp_dir: &TempDir) -> (PathBuf, PathBuf) {
    let socket = temp_dir.path().join("runtime/control.sock");
    let config = temp_dir.path().join("config.toml");
    fs::write(
        &config,
        format!(
            "[runtime]\ncontrol_socket = \"{}\"\nshutdown_timeout_secs = 2\n",
            socket.display()
        ),
    )
    .unwrap();
    (config, socket)
}

fn start(config: &Path) -> ChildGuard {
    ChildGuard(
        Command::new(binary())
            .args(["run", "--config"])
            .arg(config)
            .env("RUST_LOG", "off")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

fn wait_for_socket(socket: &Path) {
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    while !socket.exists() {
        assert!(Instant::now() < deadline, "control socket was not created");
        thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn stop_command_gracefully_stops_running_process() {
    let temp_dir = TempDir::new().unwrap();
    let (config, socket) = write_config(&temp_dir);
    let mut child = start(&config);
    wait_for_socket(&socket);

    let stop = Command::new(binary())
        .args(["stop", "--config"])
        .arg(&config)
        .output()
        .unwrap();

    assert!(
        stop.status.success(),
        "{}",
        String::from_utf8_lossy(&stop.stderr)
    );
    assert!(child.wait_for_exit().success());
    assert!(!socket.exists());
}

#[test]
fn sigterm_uses_the_same_cleanup_path() {
    let temp_dir = TempDir::new().unwrap();
    let (config, socket) = write_config(&temp_dir);
    let mut child = start(&config);
    wait_for_socket(&socket);

    kill(Pid::from_raw(child.0.id().cast_signed()), Signal::SIGTERM).unwrap();

    assert!(child.wait_for_exit().success());
    assert!(!socket.exists());
}

#[test]
fn help_lists_only_run_and_stop_commands() {
    let output = Command::new(binary()).arg("--help").output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(output.status.success());
    assert!(stdout.contains("run"));
    assert!(stdout.contains("stop"));
}
