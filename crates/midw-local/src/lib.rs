//! # midw-local
//!
//! In-process node implementation for **nodes A and B**.
//!
//! Each [`LocalNodeHandle`] spawns a dedicated tokio task that owns the node
//! state.  Commands are delivered via a bounded `mpsc` channel; responses are
//! returned via per-request `oneshot` channels.  No serialization overhead —
//! everything stays on the heap of the calling process.

use async_trait::async_trait;
use midw_common::{Command, ConfigParams, NodeError, NodeInterface, NodeStatus, Response, TestResult};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

// ── Internal state (lives inside the node task) ───────────────────────────────

struct NodeState {
    id: String,
    running: bool,
    start_time: Option<Instant>,
    message_count: u64,
    config: HashMap<String, String>,
}

impl NodeState {
    fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            running: false,
            start_time: None,
            message_count: 0,
            config: HashMap::new(),
        }
    }

    fn handle(&mut self, cmd: Command) -> Response {
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

// ── Public handle ─────────────────────────────────────────────────────────────

type Envelope = (Command, oneshot::Sender<Response>);

/// A cloneable handle to a local node running as a background tokio task.
///
/// Dropping all handles does **not** immediately terminate the task; the task
/// exits naturally once its input channel is closed (i.e. all senders dropped).
#[derive(Clone)]
pub struct LocalNodeHandle {
    id: String,
    tx: mpsc::Sender<Envelope>,
}

impl LocalNodeHandle {
    /// Spawn a new local node with the given `id` and return a handle.
    ///
    /// The node task is spawned on the current tokio runtime.  `capacity`
    /// controls the depth of the command queue (backpressure).
    pub fn new(id: impl Into<String>, capacity: usize) -> Self {
        let id = id.into();
        let (tx, mut rx) = mpsc::channel::<Envelope>(capacity);

        let node_id = id.clone();
        tokio::spawn(async move {
            let mut state = NodeState::new(&node_id);
            info!(node = %node_id, "local node task started");
            while let Some((cmd, reply)) = rx.recv().await {
                let resp = state.handle(cmd);
                // Receiver may have been dropped (fire-and-forget callers).
                let _ = reply.send(resp);
            }
            info!(node = %node_id, "local node task stopped");
        });

        Self { id, tx }
    }

    async fn dispatch(&self, cmd: Command) -> Result<Response, NodeError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send((cmd, tx))
            .await
            .map_err(|_| NodeError::Communication(format!("node '{}' channel closed", self.id)))?;
        rx.await
            .map_err(|_| NodeError::Communication(format!("node '{}' reply dropped", self.id)))
    }
}

#[async_trait]
impl NodeInterface for LocalNodeHandle {
    fn id(&self) -> &str {
        &self.id
    }

    async fn start(&self) -> Result<Response, NodeError> {
        self.dispatch(Command::Start).await
    }

    async fn stop(&self) -> Result<Response, NodeError> {
        self.dispatch(Command::Stop).await
    }

    async fn query(&self) -> Result<Response, NodeError> {
        self.dispatch(Command::Query).await
    }

    async fn config(&self, params: ConfigParams) -> Result<Response, NodeError> {
        self.dispatch(Command::Config(params)).await
    }

    async fn test(&self) -> Result<Response, NodeError> {
        self.dispatch(Command::Test).await
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use midw_common::Response;

    #[tokio::test]
    async fn start_stop_cycle() {
        let node = LocalNodeHandle::new("test-A", 8);

        // Node starts in stopped state.
        let resp = node.query().await.unwrap();
        match resp {
            Response::Status(s) => assert!(!s.running),
            other => panic!("unexpected: {:?}", other),
        }

        // Start → Ok.
        assert!(matches!(node.start().await.unwrap(), Response::Ok));

        // Query shows running.
        let resp = node.query().await.unwrap();
        match resp {
            Response::Status(s) => assert!(s.running),
            other => panic!("unexpected: {:?}", other),
        }

        // Stop → Ok.
        assert!(matches!(node.stop().await.unwrap(), Response::Ok));

        // Stop again → Error (already stopped).
        assert!(matches!(node.stop().await.unwrap(), Response::Error(_)));
    }

    #[tokio::test]
    async fn config_applied() {
        let node = LocalNodeHandle::new("test-B", 8);
        let params = ConfigParams {
            key: "timeout".into(),
            value: "30".into(),
        };
        assert!(matches!(
            node.config(params).await.unwrap(),
            Response::Ok
        ));
    }

    #[tokio::test]
    async fn test_fails_when_stopped() {
        let node = LocalNodeHandle::new("test-C", 8);
        let resp = node.test().await.unwrap();
        match resp {
            Response::TestResult(t) => assert!(!t.passed),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_passes_when_running() {
        let node = LocalNodeHandle::new("test-D", 8);
        node.start().await.unwrap();
        let resp = node.test().await.unwrap();
        match resp {
            Response::TestResult(t) => assert!(t.passed),
            other => panic!("unexpected: {:?}", other),
        }
    }
}
