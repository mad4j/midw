//! DDS node server — hdds pub/sub transport for the midw architecture.
//!
//! [`DdsNodeServer`] creates DDS DataReader/DataWriter for a request/reply
//! topic pair and processes commands in a background OS thread.  Clients
//! connect via [`DdsNodeHandle`](crate::DdsNodeHandle).

use crate::types::{NodeReply, NodeRequest};
use midw_common::{Command, ConfigParams, NodeStatus, Response, TestResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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
                    self.start_time = None;
                    info!(node = %self.id, "stopped");
                    Response::Ok
                }
            }

            Command::Query => {
                debug!(node = %self.id, "query");
                Response::Status(NodeStatus {
                    id: self.id.clone(),
                    running: self.running,
                    uptime_secs: self
                        .start_time
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(0),
                    message_count: self.message_count,
                    config: self.config.clone(),
                })
            }

            Command::Config(ConfigParams { key, value }) => {
                debug!(node = %self.id, %key, %value, "config");
                self.config.insert(key, value);
                Response::Ok
            }

            Command::Test => {
                debug!(node = %self.id, "test");
                Response::TestResult(TestResult {
                    passed: self.running,
                    message: if self.running {
                        "node is running — test passed".to_string()
                    } else {
                        "node is not running — test failed".to_string()
                    },
                })
            }
        }
    }
}

// ── DDS node server ───────────────────────────────────────────────────────────

/// DDS node server backed by hdds pub/sub.
///
/// After calling [`serve`](DdsNodeServer::serve) the node listens on
/// `midw/node/<id>/req` and sends replies on `midw/node/<id>/rep`.
/// Clients connect via [`DdsNodeHandle`](crate::DdsNodeHandle).
pub struct DdsNodeServer {
    id: String,
    participant: Arc<hdds::Participant>,
}

impl DdsNodeServer {
    /// Create a new server configuration.
    ///
    /// The service is **not** started until [`serve`](Self::serve) is called.
    /// `transport` controls the DDS transport:
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

    /// Start the request-processing loop on a dedicated OS thread.
    ///
    /// Returns immediately so callers can create handles right after.
    pub fn serve(self) -> hdds::Result<()> {
        let node_id = self.id.clone();
        let req_topic = format!("midw/node/{}/req", node_id);
        let rep_topic = format!("midw/node/{}/rep", node_id);
        let participant = self.participant;

        info!(node = %node_id, "DDS node starting");

        let qos = hdds::QoS::reliable().keep_all().volatile();

        let req_reader: hdds::DataReader<NodeRequest> = participant
            .topic(&req_topic)
            .map_err(|e| hdds::Error::InvalidState(e.to_string()))?
            .reader()
            .qos(qos.clone())
            .build()
            .map_err(|e| hdds::Error::InvalidState(e.to_string()))?;

        let rep_writer: hdds::DataWriter<NodeReply> = participant
            .topic(&rep_topic)
            .map_err(|e| hdds::Error::InvalidState(e.to_string()))?
            .writer()
            .qos(qos)
            .build()
            .map_err(|e| hdds::Error::InvalidState(e.to_string()))?;

        let state = Arc::new(Mutex::new(NodeState::new(&node_id)));

        std::thread::Builder::new()
            .name(format!("midw-node-{}", node_id))
            .spawn(move || {
                let _participant = participant;
                loop {
                    match req_reader.take() {
                        Ok(Some(request)) => {
                            let result: Result<Response, _> =
                                serde_json::from_slice(&request.payload).map(|cmd: Command| {
                                    state.lock().unwrap().handle(cmd)
                                });
                            let reply_payload = match result {
                                Ok(resp) => match serde_json::to_vec(&resp) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        error!(node = %node_id, "serialize error: {}", e);
                                        continue;
                                    }
                                },
                                Err(e) => {
                                    error!(node = %node_id, "deserialize error: {}", e);
                                    continue;
                                }
                            };
                            if let Err(e) = rep_writer.write(&NodeReply {
                                correlation_id: request.correlation_id,
                                payload: reply_payload,
                            }) {
                                error!(node = %node_id, "reply write error: {}", e);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            error!(node = %node_id, "reader error: {}", e);
                        }
                    }
                    std::thread::sleep(Duration::from_micros(100));
                }
            })
            .map_err(hdds::Error::IoError)?;

        Ok(())
    }
}
