//! # midw-common
//!
//! Shared protocol types and the `NodeInterface` async trait used by every
//! crate in the midw communication architecture.
//!
//! ## Architecture overview
//!
//! ```text
//!            ┌──────────────────────────────────────────────┐
//!            │                Control Node X                │
//!            │  start / stop / query / config / test        │
//!            └────┬──────────┬──────────┬──────────┬────────┘
//!                 │          │          │          │
//!          (local chan) (local chan) (Unix sock) (Unix sock)
//!                 │          │          │          │
//!              Node A     Node B     Node C     Node D
//!            (local)    (local)     (IPC)      (IPC)
//! ```
//!
//! Nodes A and B live in the same process as X; commands are delivered via
//! in-process tokio channels (zero-copy, no serialization overhead).
//!
//! Nodes C and D run as separate processes; commands are serialized as
//! newline-delimited JSON and exchanged over Unix-domain sockets, following
//! a DDS-inspired request/reply pattern.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ── Protocol types ────────────────────────────────────────────────────────────

/// Key/value pair sent with a [`Command::Config`] request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigParams {
    pub key: String,
    pub value: String,
}

/// Runtime status returned by [`Command::Query`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: String,
    pub running: bool,
    pub uptime_secs: u64,
    pub message_count: u64,
}

/// Result returned by [`Command::Test`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestResult {
    pub passed: bool,
    pub message: String,
    pub details: Vec<String>,
}

/// Commands that can be dispatched to any node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Start,
    Stop,
    Query,
    Config(ConfigParams),
    Test,
}

/// Responses produced by a node after processing a [`Command`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Command completed successfully with no extra payload.
    Ok,
    /// Status snapshot (reply to [`Command::Query`]).
    Status(NodeStatus),
    /// Self-test outcome (reply to [`Command::Test`]).
    TestResult(TestResult),
    /// Node-level error message.
    Error(String),
}

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("node '{0}' not registered")]
    NotFound(String),

    #[error("communication error: {0}")]
    Communication(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// ── Core trait ────────────────────────────────────────────────────────────────

/// The five operations exposed by every controlled node.
///
/// The trait is `Send + Sync` so handles can be stored behind `Arc<dyn
/// NodeInterface>` and shared across tokio tasks.
#[async_trait]
pub trait NodeInterface: Send + Sync {
    /// Unique identifier for this node (e.g. `"A"`, `"C"`).
    fn id(&self) -> &str;

    /// Transition the node to the *running* state.
    async fn start(&self) -> Result<Response, NodeError>;

    /// Transition the node to the *stopped* state.
    async fn stop(&self) -> Result<Response, NodeError>;

    /// Return a [`NodeStatus`] snapshot.
    async fn query(&self) -> Result<Response, NodeError>;

    /// Apply a key/value configuration parameter.
    async fn config(&self, params: ConfigParams) -> Result<Response, NodeError>;

    /// Run the node's self-test and return a [`TestResult`].
    async fn test(&self) -> Result<Response, NodeError>;
}
