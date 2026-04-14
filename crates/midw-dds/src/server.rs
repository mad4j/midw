//! DDS node server — hdds DDS-RPC transport for the midw architecture.
//!
//! [`DdsNodeServer`] registers an hdds [`ServiceServer`] bound to the DDS
//! service `midw/node/<id>`.  Incoming commands arrive as JSON-encoded bytes
//! via the RPC request topic; responses are returned the same way.

use midw_common::{Command, ConfigParams, NodeStatus, Response, TestResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
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

// ── DDS node server ───────────────────────────────────────────────────────────

/// DDS node server backed by hdds [`ServiceServer`].
///
/// After calling [`serve`](DdsNodeServer::serve) the node registers a DDS-RPC
/// service named `midw/node/<id>` and processes commands on a background tokio
/// task.  Clients connect via [`DdsNodeHandle`](crate::DdsNodeHandle).
pub struct DdsNodeServer {
    id: String,
    participant: Arc<hdds::Participant>,
}

impl DdsNodeServer {
    /// Create a new server configuration.
    ///
    /// The service is **not** registered until [`serve`](Self::serve) is
    /// called.  `transport` controls the RTPS transport:
    /// - [`hdds::TransportMode::IntraProcess`] — same process (tests)
    /// - [`hdds::TransportMode::UdpMulticast`] — cross-process / cross-machine
    pub fn new(id: impl Into<String>, transport: hdds::TransportMode) -> hdds::Result<Self> {
        let id = id.into();
        let participant = hdds::Participant::builder(format!("midw-node-{}", id).as_str())
            .domain_id(0)
            .with_transport(transport)
            .build()?;
        Ok(Self { id, participant })
    }

    /// Register the DDS service and start the request-processing loop on a
    /// dedicated OS thread.
    ///
    /// Returns `Ok(())` as soon as the service is registered so that clients
    /// can connect immediately after this call returns.
    ///
    /// # Thread model
    ///
    /// `hdds::rpc::ServiceServer` (and `DataWriter` inside it) is `Send` but
    /// `!Sync`.  The `spin()` future holds `&self` across `.await` points,
    /// which makes it `!Send`.  A dedicated current-thread tokio runtime with
    /// a `LocalSet` lets `block_on` drive the `!Send` future without requiring
    /// `Send` on the future itself.
    pub fn serve(self) -> hdds::Result<()> {
        let service_name = format!("midw/node/{}", self.id);
        let state = Arc::new(Mutex::new(NodeState::new(&self.id)));
        let node_id = self.id.clone();
        let participant = self.participant;

        info!(node = %node_id, service = %service_name, "DDS service starting");

        let handler =
            move |_req_id: hdds::rpc::SampleIdentity,
                  payload: &[u8]|
                  -> Result<Vec<u8>, (hdds::rpc::RemoteExceptionCode, String)> {
                let cmd: Command = serde_json::from_slice(payload).map_err(|e| {
                    error!(node = %node_id, "parse error: {}", e);
                    (
                        hdds::rpc::RemoteExceptionCode::InvalidArgument,
                        format!("parse error: {}", e),
                    )
                })?;

                let response = state.lock().unwrap().handle(cmd);

                serde_json::to_vec(&response).map_err(|e| {
                    (
                        hdds::rpc::RemoteExceptionCode::InternalError,
                        format!("serialize error: {}", e),
                    )
                })
            };

        // ServiceServer::new() is synchronous – create it here before spawning
        // the thread so we can surface errors immediately.
        let server =
            hdds::rpc::ServiceServer::new(&participant, &service_name, handler)
                .map_err(|e| hdds::Error::InvalidState(e.to_string()))?;

        // Dedicated OS thread owns the !Send ServiceServer.
        // Use rt.block_on() directly — same pattern as the hdds C FFI:
        // `spin()` returns a !Send future but block_on() doesn't require Send.
        std::thread::Builder::new()
            .name(format!("midw-node-{}", self.id))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("DDS node runtime");
                rt.block_on(async move {
                    let _participant = participant;
                    server.spin().await;
                });
            })
            .map_err(hdds::Error::IoError)?;

        Ok(())
    }
}
