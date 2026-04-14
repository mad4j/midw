pub mod server;
pub mod types;

pub use server::DdsNodeServer;
pub use hdds::TransportMode;

use async_trait::async_trait;
use midw_common::{Command, ConfigParams, NodeError, NodeInterface, Response};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use types::{NodeReply, NodeRequest};

/// A handle that routes [`NodeInterface`] calls to a [`DdsNodeServer`] via
/// hdds DDS pub/sub (bypassing the broken hdds RPC layer).
///
/// The handle holds a `DataWriter<NodeRequest>` and `DataReader<NodeReply>`.
/// Each `dispatch()` call writes a request with a unique correlation id, then
/// polls the reply reader (with 100 µs async sleeps) until the matching reply
/// arrives or the 10-second deadline passes.
pub struct DdsNodeHandle {
    id: String,
    req_writer: Arc<hdds::DataWriter<NodeRequest>>,
    rep_reader: Arc<hdds::DataReader<NodeReply>>,
    _participant: Arc<hdds::Participant>,
    correlation_seq: Arc<AtomicU64>,
}

impl DdsNodeHandle {
    /// Create a new handle.  `transport` must match the server's transport.
    pub fn new(id: impl Into<String>, transport: hdds::TransportMode) -> hdds::Result<Self> {
        let id = id.into();
        let participant = Arc::new(
            hdds::Participant::builder(format!("midw-ctrl-{}", id).as_str())
                .domain_id(0)
                .with_transport(transport)
                .build()?,
        );

        let qos = hdds::QoS::reliable().keep_all().volatile();

        let req_topic = format!("midw/node/{}/req", id);
        let rep_topic = format!("midw/node/{}/rep", id);

        let req_writer: hdds::DataWriter<NodeRequest> = participant
            .topic(&req_topic)
            .map_err(|e| hdds::Error::InvalidState(e.to_string()))?
            .writer()
            .qos(qos.clone())
            .build()
            .map_err(|e| hdds::Error::InvalidState(e.to_string()))?;

        let rep_reader: hdds::DataReader<NodeReply> = participant
            .topic(&rep_topic)
            .map_err(|e| hdds::Error::InvalidState(e.to_string()))?
            .reader()
            .qos(qos)
            .build()
            .map_err(|e| hdds::Error::InvalidState(e.to_string()))?;

        Ok(Self {
            id,
            req_writer: Arc::new(req_writer),
            rep_reader: Arc::new(rep_reader),
            _participant: participant,
            correlation_seq: Arc::new(AtomicU64::new(1)),
        })
    }

    async fn dispatch(&self, cmd: Command) -> Result<Response, NodeError> {
        debug!(node = %self.id, ?cmd, "DDS dispatch");

        let payload = serde_json::to_vec(&cmd).map_err(NodeError::Serialization)?;
        let corr_id = self.correlation_seq.fetch_add(1, Ordering::Relaxed);

        self.req_writer
            .write(&NodeRequest {
                correlation_id: corr_id,
                payload,
            })
            .map_err(|e| NodeError::Communication(e.to_string()))?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            // Drain all available replies looking for our correlation id.
            loop {
                match self.rep_reader.take() {
                    Ok(Some(reply)) if reply.correlation_id == corr_id => {
                        return serde_json::from_slice(&reply.payload)
                            .map_err(NodeError::Serialization);
                    }
                    Ok(Some(_)) => {} // stale reply from a previous request — skip
                    Ok(None) => break,
                    Err(e) => {
                        return Err(NodeError::Communication(e.to_string()));
                    }
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(NodeError::Communication(format!(
                    "DDS timeout waiting for reply from node '{}'",
                    self.id
                )));
            }

            tokio::time::sleep(Duration::from_micros(100)).await;
        }
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

    async fn make_pair(id: &str) -> DdsNodeHandle {
        let transport = TransportMode::IntraProcess;
        DdsNodeServer::new(id, transport)
            .expect("server creation")
            .serve()
            .expect("server serve");
        let handle = DdsNodeHandle::new(id, transport).expect("handle creation");
        // Allow IntraProcess binding to complete.
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
        match handle.query().await.unwrap() {
            Response::Status(s) => assert!(s.running),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[tokio::test]
    async fn dds_config() {
        let handle = make_pair("dds-test-C").await;
        let resp = handle
            .config(ConfigParams { key: "rate".into(), value: "100".into() })
            .await
            .unwrap();
        assert!(matches!(resp, Response::Ok));
    }

    #[tokio::test]
    async fn dds_test_pass_fail() {
        let handle = make_pair("dds-test-D").await;
        match handle.test().await.unwrap() {
            Response::TestResult(t) => assert!(!t.passed),
            other => panic!("unexpected: {:?}", other),
        }
        handle.start().await.unwrap();
        match handle.test().await.unwrap() {
            Response::TestResult(t) => assert!(t.passed),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[tokio::test]
    async fn dds_stop_already_stopped() {
        let handle = make_pair("dds-test-E").await;
        match handle.stop().await.unwrap() {
            Response::Error(_) => {}
            other => panic!("expected Error, got: {:?}", other),
        }
    }
}
