//! # midw-dds
//!
//! DDS transport layer for the midw communication architecture, built on
//! [hdds](https://github.com/hdds-team/hdds) — a high-performance DDS/RTPS
//! implementation in pure Rust.
//!
//! This crate is a drop-in replacement for `midw-ipc`: Unix-domain sockets are
//! replaced by real DDS publish-subscribe transport (RTPS over UDP), enabling
//! nodes to live on separate machines.
//!
//! ## Wire protocol
//!
//! Commands and responses are JSON-serialised and exchanged over the
//! OMG DDS-RPC request/reply pattern.
//!
//! | Direction | DDS topic | Payload |
//! |-----------|-----------|---------|
//! | ctrl → node | `rq/midw/node/<id>` | JSON-encoded [`Command`] |
//! | node → ctrl | `rr/midw/node/<id>` | JSON-encoded [`Response`] |
//!
//! ## Transport modes
//!
//! | Mode | Use case |
//! |------|----------|
//! | [`hdds::TransportMode::IntraProcess`] | Same process; tests; zero network overhead |
//! | [`hdds::TransportMode::UdpMulticast`] | Cross-process / cross-machine |
//!
//! ## Architecture overview
//!
//! ```text
//!            ┌──────────────────────────────────────────────┐
//!            │                Control Node X                │
//!            │  start / stop / query / config / test        │
//!            └────┬──────────┬──────────┬──────────┬────────┘
//!                 │          │          │          │
//!          (local chan) (local chan) (DDS-RPC)  (DDS-RPC)
//!                 │          │          │          │
//!              Node A     Node B     Node C     Node D
//!            (local)    (local)     (DDS)      (DDS)
//! ```
//!
//! Nodes C and D communicate via hdds DDS-RPC over RTPS.  In UDP-multicast
//! mode they can run on separate machines; in intra-process mode they stay
//! in the same OS process with zero-copy delivery.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use hdds::TransportMode;
//! use midw_dds::{DdsNodeHandle, DdsNodeServer};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Server side (starts node C):
//! DdsNodeServer::new("C", TransportMode::IntraProcess)?.serve()?;
//!
//! // Client side (control node X dispatches to C):
//! let node_c = DdsNodeHandle::new("C", TransportMode::IntraProcess)?;
//! # Ok(())
//! # }
//! ```

pub mod server;

pub use server::DdsNodeServer;
/// Re-export so callers don't need to add `hdds` as a direct dependency.
pub use hdds::TransportMode;

use async_trait::async_trait;
use midw_common::{Command, ConfigParams, NodeError, NodeInterface, Response};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

// ── Internal dispatch types ───────────────────────────────────────────────────

/// A single request forwarded from `DdsNodeHandle` to its background thread.
struct DispatchReq {
    cmd: Command,
    reply: oneshot::Sender<Result<Response, NodeError>>,
}

// ── DDS node handle (client side) ─────────────────────────────────────────────

/// A handle that routes [`NodeInterface`] calls to a [`DdsNodeServer`] via
/// hdds DDS-RPC.
///
/// Each method JSON-serialises the [`Command`], sends it to an internal
/// background thread that exclusively owns the `hdds::rpc::ServiceClient`, and
/// awaits the JSON-deserialised [`Response`].
///
/// # Thread model
///
/// `hdds::rpc::ServiceClient` is `Send` but `!Sync`: the `call_raw` future
/// borrows `&self` across `.await` making it `!Send`.  Following the same
/// pattern as the hdds C FFI:
///
/// 1. A dedicated current-thread tokio runtime (`rt`) is created.
/// 2. `ServiceClient::new()` is called inside `rt.enter()` so its internal
///    reply-listener task is spawned on `rt`.
/// 3. A background OS thread calls `rt.block_on(client.call_raw(…))` for each
///    request, which drives both `call_raw` and the reply-listener on the same
///    thread.
///
/// The `mpsc::Sender` front end is `Send + Sync`, so `DdsNodeHandle` satisfies
/// the `NodeInterface: Send + Sync` bound.
pub struct DdsNodeHandle {
    id: String,
    tx: mpsc::Sender<DispatchReq>,
}

impl DdsNodeHandle {
    /// Create a new handle and start its background worker thread.
    ///
    /// `transport` must match the transport used by the corresponding
    /// [`DdsNodeServer`].
    pub fn new(id: impl Into<String>, transport: hdds::TransportMode) -> hdds::Result<Self> {
        let id = id.into();
        let participant = hdds::Participant::builder(format!("midw-ctrl-{}", id).as_str())
            .domain_id(0)
            .with_transport(transport)
            .build()?;
        let service_name = format!("midw/node/{}", id);

        // Build a dedicated current-thread runtime.  `ServiceClient::new()` is
        // called inside `rt.enter()` so its internal `tokio::spawn` for the
        // reply-listener goes to *this* runtime (not the caller's runtime).
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(hdds::Error::IoError)?;

        let client = {
            let _guard = rt.enter();
            hdds::rpc::ServiceClient::new(&participant, &service_name)
                .map_err(|e| hdds::Error::InvalidState(e.to_string()))?
            // _guard dropped here — safe to call block_on afterwards
        };

        let (tx, rx) = mpsc::channel::<DispatchReq>(32);

        // Dedicated OS thread: drives `rt.block_on(client.call_raw(…))` for
        // each request.  Both `call_raw` and the reply-listener run on `rt`.
        std::thread::Builder::new()
            .name(format!("midw-ctrl-{}", id))
            .spawn(move || {
                let _participant = participant;
                let mut rx = rx;
                loop {
                    // Drive `recv()` (and any background tasks) on `rt`.
                    let req = match rt.block_on(rx.recv()) {
                        Some(r) => r,
                        None => break,
                    };

                    let result = (|| {
                        let payload = serde_json::to_vec(&req.cmd)
                            .map_err(NodeError::Serialization)?;
                        rt.block_on(client.call_raw(&payload, Duration::from_secs(10)))
                            .map_err(|e| NodeError::Communication(e.to_string()))
                            .and_then(|bytes| {
                                serde_json::from_slice::<Response>(&bytes)
                                    .map_err(NodeError::Serialization)
                            })
                    })();

                    let _ = req.reply.send(result);
                }
            })
            .map_err(hdds::Error::IoError)?;

        Ok(Self { id, tx })
    }

    async fn dispatch(&self, cmd: Command) -> Result<Response, NodeError> {
        debug!(node = %self.id, ?cmd, "DDS-RPC dispatch");
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(DispatchReq {
                cmd,
                reply: reply_tx,
            })
            .await
            .map_err(|_| NodeError::Communication("dispatch channel closed".into()))?;
        reply_rx
            .await
            .map_err(|_| NodeError::Communication("reply channel closed".into()))?
    }
}

#[async_trait]
impl NodeInterface for DdsNodeHandle {
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
    use hdds::TransportMode;
    use midw_common::{ConfigParams, Response};

    /// Helper: start a DDS server and return a connected handle.
    ///
    /// Uses `IntraProcess` transport — no UDP sockets, zero discovery latency.
    async fn make_pair(id: &str) -> DdsNodeHandle {
        let transport = TransportMode::IntraProcess;
        DdsNodeServer::new(id, transport)
            .expect("server creation")
            .serve()
            .expect("server serve");
        let handle = DdsNodeHandle::new(id, transport).expect("handle creation");
        // Allow intra-process discovery to wire up.
        tokio::time::sleep(Duration::from_millis(20)).await;
        handle
    }

    #[tokio::test]
    async fn dds_start_stop() {
        let handle = make_pair("dds-test-A").await;
        assert!(matches!(handle.start().await.unwrap(), Response::Ok));
        assert!(matches!(handle.stop().await.unwrap(), Response::Ok));
    }

    #[tokio::test]
    async fn dds_query() {
        let handle = make_pair("dds-test-B").await;
        handle.start().await.unwrap();
        let resp = handle.query().await.unwrap();
        match resp {
            Response::Status(s) => assert!(s.running),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[tokio::test]
    async fn dds_config() {
        let handle = make_pair("dds-test-C").await;
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
    async fn dds_test_pass_fail() {
        let handle = make_pair("dds-test-D").await;

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
    async fn dds_stop_already_stopped() {
        let handle = make_pair("dds-test-E").await;
        assert!(matches!(handle.stop().await.unwrap(), Response::Error(_)));
    }
}
