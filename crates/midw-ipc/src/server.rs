//! IPC node server — Unix-domain socket listener.
//!
//! [`IpcNodeServer`] binds to a socket path and spawns a tokio task that
//! accepts incoming connections.  Each connection is handled in its own task:
//!
//! 1. Read one JSON line → deserialise into [`Command`].
//! 2. Process the command against shared [`NodeState`].
//! 3. Serialise the [`Response`] and write one JSON line back.
//! 4. Close the connection.

use midw_common::{Command, ConfigParams, NodeStatus, Response, TestResult};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

// ── Shared node state ─────────────────────────────────────────────────────────

pub(crate) struct NodeState {
    pub(crate) id: String,
    pub(crate) running: bool,
    pub(crate) start_time: Option<Instant>,
    pub(crate) message_count: u64,
    pub(crate) config: HashMap<String, String>,
}

impl NodeState {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            running: false,
            start_time: None,
            message_count: 0,
            config: HashMap::new(),
        }
    }

    pub(crate) fn handle(&mut self, cmd: Command) -> Response {
        self.message_count += 1;
        match cmd {
            Command::Start => {
                if self.running {
                    warn!(node = %self.id, "start requested but already running");
                    Response::Error(format!("node '{}' is already running", self.id))
                } else {
                    self.running = true;
                    self.start_time = Some(Instant::now());
                    info!(node = %self.id, "started");
                    Response::Ok
                }
            }

            Command::Stop => {
                if !self.running {
                    warn!(node = %self.id, "stop requested but not running");
                    Response::Error(format!("node '{}' is not running", self.id))
                } else {
                    self.running = false;
                    info!(node = %self.id, "stopped");
                    Response::Ok
                }
            }

            Command::Query => {
                let uptime_secs = self
                    .start_time
                    .filter(|_| self.running)
                    .map(|t| t.elapsed().as_secs())
                    .unwrap_or(0);
                debug!(node = %self.id, running = self.running, uptime_secs, "query");
                Response::Status(NodeStatus {
                    id: self.id.clone(),
                    running: self.running,
                    uptime_secs,
                    message_count: self.message_count,
                })
            }

            Command::Config(ConfigParams { key, value }) => {
                info!(node = %self.id, %key, %value, "config");
                self.config.insert(key, value);
                Response::Ok
            }

            Command::Test => {
                let passed = self.running;
                info!(node = %self.id, passed, "test");
                Response::TestResult(TestResult {
                    passed,
                    message: if passed {
                        format!("node '{}' self-test passed", self.id)
                    } else {
                        format!("node '{}' self-test failed: node not running", self.id)
                    },
                    details: vec![
                        format!("running: {}", self.running),
                        format!("message_count: {}", self.message_count),
                        format!("config_entries: {}", self.config.len()),
                    ],
                })
            }
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Unix-domain socket server for an IPC-resident node.
///
/// After calling [`serve`](IpcNodeServer::serve) the server accepts connections
/// on a background tokio task.  The caller (usually the `midw-node` binary or
/// the demo) retains the server value to keep it alive.
pub struct IpcNodeServer {
    id: String,
    socket_path: String,
}

impl IpcNodeServer {
    /// Create a new server configuration.
    ///
    /// The socket is **not** bound until [`serve`](Self::serve) is called.
    pub fn new(id: impl Into<String>, socket_path: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            socket_path: socket_path.into(),
        }
    }

    /// Bind the Unix-domain socket and start the accept loop as a background
    /// tokio task.
    ///
    /// Returns `Ok(())` as soon as the socket is successfully bound so that
    /// clients can connect immediately after this call returns.
    pub async fn serve(self) -> std::io::Result<()> {
        // Remove stale socket file if present.
        let _ = std::fs::remove_file(&self.socket_path);

        let listener = UnixListener::bind(&self.socket_path)?;
        let state = Arc::new(Mutex::new(NodeState::new(&self.id)));
        let node_id = self.id.clone();
        let socket_path = self.socket_path.clone();

        info!(node = %node_id, socket = %socket_path, "IPC server listening");

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let state = Arc::clone(&state);
                        let nid = node_id.clone();
                        tokio::spawn(async move {
                            Self::handle_connection(stream, state, &nid).await;
                        });
                    }
                    Err(e) => {
                        error!(node = %node_id, "accept error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_connection(
        stream: tokio::net::UnixStream,
        state: Arc<Mutex<NodeState>>,
        node_id: &str,
    ) {
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        if let Err(e) = reader.read_line(&mut line).await {
            error!(node = %node_id, "read error: {}", e);
            return;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            return;
        }

        let cmd: Command = match serde_json::from_str(trimmed) {
            Ok(c) => c,
            Err(e) => {
                error!(node = %node_id, "parse error: {}", e);
                let err_resp = Response::Error(format!("parse error: {}", e));
                let _ = writer
                    .write_all(
                        format!("{}\n", serde_json::to_string(&err_resp).unwrap_or_default())
                            .as_bytes(),
                    )
                    .await;
                return;
            }
        };

        let response = state.lock().await.handle(cmd);

        match serde_json::to_string(&response) {
            Ok(mut json) => {
                json.push('\n');
                if let Err(e) = writer.write_all(json.as_bytes()).await {
                    error!(node = %node_id, "write error: {}", e);
                }
            }
            Err(e) => error!(node = %node_id, "serialise error: {}", e),
        }
    }
}
