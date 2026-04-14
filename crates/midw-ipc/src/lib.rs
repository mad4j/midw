//! # midw-ipc
//!
//! Unix-domain-socket IPC transport for **nodes C and D**.
//!
//! ## Components
//!
//! * [`IpcNodeServer`] — bind a socket, accept connections and process
//!   commands; intended to run as a tokio task (or a separate process via the
//!   `midw-node` binary in `src/bin/`).
//! * [`IpcNodeHandle`] — thin client that connects to an [`IpcNodeServer`],
//!   sends a JSON-encoded [`Command`] and reads back a JSON-encoded
//!   [`Response`].
//!
//! ### Wire protocol
//!
//! ```text
//! client → server:  <JSON command>\n
//! server → client:  <JSON response>\n
//! ```
//!
//! Each connection carries exactly one request/response pair, then closes.
//! This keeps the implementation simple and matches DDS request-reply QoS
//! semantics.

pub mod server;

pub use server::IpcNodeServer;

use async_trait::async_trait;
use midw_common::{Command, ConfigParams, NodeError, NodeInterface, Response};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::debug;

// ── IPC client handle ─────────────────────────────────────────────────────────

/// A handle that routes [`NodeInterface`] calls to an [`IpcNodeServer`] over a
/// Unix-domain socket.
///
/// Each method call opens a fresh connection, sends one JSON line, reads one
/// JSON line, then closes the connection.
pub struct IpcNodeHandle {
    id: String,
    socket_path: String,
}

impl IpcNodeHandle {
    /// Create a new handle.
    ///
    /// `socket_path` must point to the Unix-domain socket bound by the
    /// corresponding [`IpcNodeServer`].
    pub fn new(id: impl Into<String>, socket_path: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            socket_path: socket_path.into(),
        }
    }

    async fn dispatch(&self, cmd: Command) -> Result<Response, NodeError> {
        debug!(node = %self.id, ?cmd, "IPC dispatch");

        let stream = UnixStream::connect(&self.socket_path).await.map_err(|e| {
            NodeError::Communication(format!(
                "cannot connect to node '{}' at {}: {}",
                self.id, self.socket_path, e
            ))
        })?;

        let (reader, mut writer) = stream.into_split();

        // Send: one JSON line
        let mut payload = serde_json::to_string(&cmd)?;
        payload.push('\n');
        writer.write_all(payload.as_bytes()).await?;
        // Signal EOF to the write half so the server can detect end-of-input.
        writer.shutdown().await?;

        // Receive: one JSON line
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await?;

        if line.is_empty() {
            return Err(NodeError::Communication(format!(
                "node '{}' closed connection without a response",
                self.id
            )));
        }

        let response: Response = serde_json::from_str(line.trim())?;
        Ok(response)
    }
}

#[async_trait]
impl NodeInterface for IpcNodeHandle {
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
    use midw_common::{ConfigParams, Response};
    use std::sync::Arc;

    /// Helper: start an IpcNodeServer and return a ready handle.
    async fn make_pair(id: &str) -> IpcNodeHandle {
        let socket_path = format!("/tmp/midw-test-{}-{}.sock", id, std::process::id());
        let server = IpcNodeServer::new(id, &socket_path);
        server.serve().await.expect("server bind failed");
        IpcNodeHandle::new(id, socket_path)
    }

    #[tokio::test]
    async fn ipc_start_stop() {
        let handle = make_pair("ipc-test-A").await;
        assert!(matches!(handle.start().await.unwrap(), Response::Ok));
        assert!(matches!(handle.stop().await.unwrap(), Response::Ok));
    }

    #[tokio::test]
    async fn ipc_query() {
        let handle = make_pair("ipc-test-B").await;
        handle.start().await.unwrap();
        let resp = handle.query().await.unwrap();
        match resp {
            Response::Status(s) => assert!(s.running),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[tokio::test]
    async fn ipc_config() {
        let handle = make_pair("ipc-test-C").await;
        let resp = handle
            .config(ConfigParams {
                key: "rate".into(),
                value: "100".into(),
            })
            .await
            .unwrap();
        assert!(matches!(resp, Response::Ok));
    }

    #[tokio::test]
    async fn ipc_test_pass_fail() {
        let handle = make_pair("ipc-test-D").await;

        // Stopped → fail
        let resp = handle.test().await.unwrap();
        match resp {
            Response::TestResult(t) => assert!(!t.passed),
            other => panic!("unexpected: {:?}", other),
        }

        // Started → pass
        handle.start().await.unwrap();
        let resp = handle.test().await.unwrap();
        match resp {
            Response::TestResult(t) => assert!(t.passed),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[tokio::test]
    async fn ipc_concurrent_requests() {
        let socket_path =
            format!("/tmp/midw-test-concurrent-{}.sock", std::process::id());
        let server = IpcNodeServer::new("ipc-conc", &socket_path);
        server.serve().await.unwrap();

        let path = Arc::new(socket_path);
        let mut handles = Vec::new();
        for i in 0..5 {
            let p = Arc::clone(&path);
            handles.push(tokio::spawn(async move {
                let h = IpcNodeHandle::new(format!("ipc-conc-{}", i), p.as_str());
                h.query().await.unwrap()
            }));
        }
        for h in handles {
            assert!(matches!(h.await.unwrap(), Response::Status(_)));
        }
    }
}
