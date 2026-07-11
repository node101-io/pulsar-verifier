use std::{
    fs, io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use nix::unistd::Uid;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    time::{Instant, sleep, timeout},
};

use crate::{Error, Result, config::RuntimeConfig};

const SHUTDOWN_REQUEST: &[u8] = b"shutdown\n";
const ACCEPTED_RESPONSE: &[u8] = b"accepted\n";
const UNKNOWN_RESPONSE: &[u8] = b"error unknown-command\n";
const TOO_LARGE_RESPONSE: &[u8] = b"error frame-too-large\n";
const MAX_FRAME_LEN: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Owns the local control endpoint while `App` owns the process lifecycle.
pub(crate) struct ControlServer {
    listener: UnixListener,
    socket_path: PathBuf,
}

impl ControlServer {
    /// Binds a private socket after distinguishing active and stale instances.
    pub(crate) async fn bind(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        prepare_parent_directory(path)?;
        prepare_socket_path(path).await?;

        let listener = UnixListener::bind(path).map_err(|source| Error::ControlIo {
            path: path.to_path_buf(),
            source,
        })?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            Error::ControlIo {
                path: path.to_path_buf(),
                source,
            }
        })?;

        Ok(Self {
            listener,
            socket_path: path.to_path_buf(),
        })
    }

    /// Serves bounded commands until a valid shutdown request is accepted.
    pub(crate) async fn wait_for_shutdown(&self) -> Result<()> {
        loop {
            let (mut stream, _) =
                self.listener
                    .accept()
                    .await
                    .map_err(|source| Error::ControlIo {
                        path: self.socket_path.clone(),
                        source,
                    })?;

            if handle_connection(&mut stream, &self.socket_path).await? {
                return Ok(());
            }
        }
    }
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        match fs::remove_file(&self.socket_path) {
            Ok(()) => tracing::debug!(path = %self.socket_path.display(), "control socket removed"),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(
                path = %self.socket_path.display(),
                %error,
                "failed to remove control socket"
            ),
        }
    }
}

/// Connects to the running instance and waits until its socket is removed.
pub(crate) async fn request_shutdown(config: &RuntimeConfig) -> Result<()> {
    let path = &config.control_socket;
    let mut stream = match timeout(IO_TIMEOUT, UnixStream::connect(path)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) if is_not_running_error(&error) => {
            return Err(Error::NotRunning(path.clone()));
        }
        Ok(Err(source)) => {
            return Err(Error::ControlIo {
                path: path.clone(),
                source,
            });
        }
        Err(_) => return Err(Error::ControlTimeout(path.clone())),
    };

    write_frame(&mut stream, SHUTDOWN_REQUEST, path).await?;
    let response = read_frame(&mut stream, path).await?;
    if response != ACCEPTED_RESPONSE {
        return Err(Error::ControlProtocol(
            String::from_utf8_lossy(&response).trim().to_owned(),
        ));
    }

    let deadline = Instant::now() + config.shutdown_timeout;
    while path.exists() {
        if Instant::now() >= deadline {
            return Err(Error::ShutdownTimeout(config.shutdown_timeout));
        }
        sleep(STOP_POLL_INTERVAL).await;
    }

    Ok(())
}

async fn handle_connection(stream: &mut UnixStream, path: &Path) -> Result<bool> {
    match read_frame(stream, path).await {
        Ok(request) if request == SHUTDOWN_REQUEST => {
            write_frame(stream, ACCEPTED_RESPONSE, path).await?;
            Ok(true)
        }
        Ok(_) => {
            write_frame(stream, UNKNOWN_RESPONSE, path).await?;
            Ok(false)
        }
        Err(Error::ControlProtocol(message)) if message == "frame exceeds 64 bytes" => {
            write_frame(stream, TOO_LARGE_RESPONSE, path).await?;
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

async fn read_frame(stream: &mut UnixStream, path: &Path) -> Result<Vec<u8>> {
    let read = async {
        let mut frame = Vec::with_capacity(SHUTDOWN_REQUEST.len());

        while frame.len() < MAX_FRAME_LEN {
            let byte = stream.read_u8().await?;
            frame.push(byte);
            if byte == b'\n' {
                return Ok(frame);
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds 64 bytes",
        ))
    };

    match timeout(IO_TIMEOUT, read).await {
        Ok(Ok(frame)) => Ok(frame),
        Ok(Err(error)) if error.kind() == io::ErrorKind::InvalidData => {
            Err(Error::ControlProtocol(error.to_string()))
        }
        Ok(Err(source)) => Err(Error::ControlIo {
            path: path.to_path_buf(),
            source,
        }),
        Err(_) => Err(Error::ControlTimeout(path.to_path_buf())),
    }
}

async fn write_frame(stream: &mut UnixStream, frame: &[u8], path: &Path) -> Result<()> {
    match timeout(IO_TIMEOUT, async {
        stream.write_all(frame).await?;
        stream.flush().await
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(Error::ControlIo {
            path: path.to_path_buf(),
            source,
        }),
        Err(_) => Err(Error::ControlTimeout(path.to_path_buf())),
    }
}

fn prepare_parent_directory(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| Error::UnsafeSocketPath {
        path: path.to_path_buf(),
        reason: "socket path has no parent directory".to_owned(),
    })?;

    fs::create_dir_all(parent).map_err(|source| Error::ControlIo {
        path: parent.to_path_buf(),
        source,
    })?;

    let metadata = fs::symlink_metadata(parent).map_err(|source| Error::ControlIo {
        path: parent.to_path_buf(),
        source,
    })?;
    if !metadata.is_dir() {
        return Err(Error::UnsafeSocketPath {
            path: parent.to_path_buf(),
            reason: "socket parent is not a directory".to_owned(),
        });
    }
    ensure_owned_by_current_user(parent, &metadata)?;

    fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).map_err(|source| {
        Error::ControlIo {
            path: parent.to_path_buf(),
            source,
        }
    })
}

async fn prepare_socket_path(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(Error::ControlIo {
                path: path.to_path_buf(),
                source,
            });
        }
    };

    if !metadata.file_type().is_socket() {
        return Err(Error::UnsafeSocketPath {
            path: path.to_path_buf(),
            reason: "existing entry is not a Unix socket".to_owned(),
        });
    }
    ensure_owned_by_current_user(path, &metadata)?;

    // A successful or inconclusive connect means an existing process may own it.
    match timeout(IO_TIMEOUT, UnixStream::connect(path)).await {
        Ok(Ok(_)) | Err(_) => return Err(Error::AlreadyRunning(path.to_path_buf())),
        Ok(Err(error)) if is_not_running_error(&error) => {}
        Ok(Err(source)) => {
            return Err(Error::ControlIo {
                path: path.to_path_buf(),
                source,
            });
        }
    }

    fs::remove_file(path).map_err(|source| Error::ControlIo {
        path: path.to_path_buf(),
        source,
    })
}

fn ensure_owned_by_current_user(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    ensure_owned_by_uid(path, metadata, Uid::effective().as_raw())
}

fn ensure_owned_by_uid(path: &Path, metadata: &fs::Metadata, effective_uid: u32) -> Result<()> {
    if metadata.uid() != effective_uid {
        return Err(Error::UnsafeSocketPath {
            path: path.to_path_buf(),
            reason: format!(
                "entry is owned by uid {}, current effective uid is {effective_uid}",
                metadata.uid()
            ),
        });
    }
    Ok(())
}

fn is_not_running_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
    )
}

#[cfg(test)]
mod tests {
    use std::{
        os::unix::{fs::MetadataExt, net::UnixListener as StdUnixListener},
        time::Duration,
    };

    use tempfile::TempDir;

    use super::*;

    fn runtime_config(temp_dir: &TempDir) -> RuntimeConfig {
        RuntimeConfig {
            control_socket: temp_dir.path().join("runtime/control.sock"),
            shutdown_timeout: Duration::from_secs(2),
        }
    }

    #[tokio::test]
    async fn shutdown_request_stops_server_and_removes_socket() {
        let temp_dir = TempDir::new().unwrap();
        let config = runtime_config(&temp_dir);
        let server = ControlServer::bind(&config.control_socket).await.unwrap();

        let server_task = tokio::spawn(async move { server.wait_for_shutdown().await });
        request_shutdown(&config).await.unwrap();

        server_task.await.unwrap().unwrap();
        assert!(!config.control_socket.exists());
    }

    #[tokio::test]
    async fn rejects_duplicate_active_server() {
        let temp_dir = TempDir::new().unwrap();
        let config = runtime_config(&temp_dir);
        let _server = ControlServer::bind(&config.control_socket).await.unwrap();

        assert!(matches!(
            ControlServer::bind(&config.control_socket).await,
            Err(Error::AlreadyRunning(_))
        ));
    }

    #[tokio::test]
    async fn replaces_owned_stale_socket() {
        let temp_dir = TempDir::new().unwrap();
        let config = runtime_config(&temp_dir);
        fs::create_dir_all(config.control_socket.parent().unwrap()).unwrap();
        let stale = StdUnixListener::bind(&config.control_socket).unwrap();
        drop(stale);

        let _server = ControlServer::bind(&config.control_socket).await.unwrap();
        assert!(config.control_socket.exists());
    }

    #[tokio::test]
    async fn refuses_to_replace_normal_file() {
        let temp_dir = TempDir::new().unwrap();
        let config = runtime_config(&temp_dir);
        fs::create_dir_all(config.control_socket.parent().unwrap()).unwrap();
        fs::write(&config.control_socket, b"not a socket").unwrap();

        assert!(matches!(
            ControlServer::bind(&config.control_socket).await,
            Err(Error::UnsafeSocketPath { .. })
        ));
        assert_eq!(fs::read(&config.control_socket).unwrap(), b"not a socket");
    }

    #[tokio::test]
    async fn unknown_command_does_not_stop_server() {
        let temp_dir = TempDir::new().unwrap();
        let config = runtime_config(&temp_dir);
        let server = ControlServer::bind(&config.control_socket).await.unwrap();
        let server_task = tokio::spawn(async move { server.wait_for_shutdown().await });

        let mut stream = UnixStream::connect(&config.control_socket).await.unwrap();
        write_frame(&mut stream, b"status\n", &config.control_socket)
            .await
            .unwrap();
        assert_eq!(
            read_frame(&mut stream, &config.control_socket)
                .await
                .unwrap(),
            UNKNOWN_RESPONSE
        );
        assert!(!server_task.is_finished());

        request_shutdown(&config).await.unwrap();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn oversized_command_does_not_stop_server() {
        let temp_dir = TempDir::new().unwrap();
        let config = runtime_config(&temp_dir);
        let server = ControlServer::bind(&config.control_socket).await.unwrap();
        let server_task = tokio::spawn(async move { server.wait_for_shutdown().await });

        let mut stream = UnixStream::connect(&config.control_socket).await.unwrap();
        write_frame(&mut stream, &[b'x'; MAX_FRAME_LEN], &config.control_socket)
            .await
            .unwrap();
        assert_eq!(
            read_frame(&mut stream, &config.control_socket)
                .await
                .unwrap(),
            TOO_LARGE_RESPONSE
        );
        assert!(!server_task.is_finished());

        request_shutdown(&config).await.unwrap();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn reports_shutdown_timeout_when_socket_remains() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = runtime_config(&temp_dir);
        config.shutdown_timeout = Duration::from_millis(50);
        prepare_parent_directory(&config.control_socket).unwrap();
        let listener = UnixListener::bind(&config.control_socket).unwrap();
        let path = config.control_socket.clone();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_frame(&mut stream, &path).await.unwrap(),
                SHUTDOWN_REQUEST
            );
            write_frame(&mut stream, ACCEPTED_RESPONSE, &path)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        assert!(matches!(
            request_shutdown(&config).await,
            Err(Error::ShutdownTimeout(_))
        ));
        server.await.unwrap();
        fs::remove_file(&config.control_socket).unwrap();
    }

    #[test]
    fn rejects_entry_owned_by_another_uid() {
        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("entry");
        fs::write(&file, b"data").unwrap();
        let metadata = fs::metadata(&file).unwrap();
        let different_uid = metadata.uid().wrapping_add(1);

        assert!(matches!(
            ensure_owned_by_uid(&file, &metadata, different_uid),
            Err(Error::UnsafeSocketPath { .. })
        ));
    }

    #[tokio::test]
    async fn reports_not_running() {
        let temp_dir = TempDir::new().unwrap();
        let config = runtime_config(&temp_dir);

        assert!(matches!(
            request_shutdown(&config).await,
            Err(Error::NotRunning(_))
        ));
    }
}
