//! # midw-control
//!
//! **Control node X** — the hub that manages all registered nodes (A, B, C, D).
//!
//! [`ControlNode`] holds a map from node ID to `Arc<dyn NodeInterface>`.  It
//! can dispatch any of the five commands to a single named node, or broadcast
//! a command concurrently to every registered node.

use midw_common::{ConfigParams, NodeError, NodeInterface, Response};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

// ── ControlNode ───────────────────────────────────────────────────────────────

/// The control hub.  Register nodes with [`register`](Self::register), then
/// drive them through the [`NodeInterface`] operations.
pub struct ControlNode {
    id: String,
    nodes: HashMap<String, Arc<dyn NodeInterface>>,
}

impl ControlNode {
    /// Create a new control node with the given identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            nodes: HashMap::new(),
        }
    }

    /// The identifier of this control node (always `"X"` in the demo).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Register a node under its own `id()`.
    ///
    /// If a node with the same ID was already registered it is replaced.
    pub fn register(&mut self, node: Arc<dyn NodeInterface>) {
        info!(ctrl = %self.id, node = %node.id(), "registered node");
        self.nodes.insert(node.id().to_string(), node);
    }

    /// Return the IDs of all registered nodes (sorted for deterministic output).
    pub fn node_ids(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self.nodes.keys().map(String::as_str).collect();
        ids.sort_unstable();
        ids
    }

    // ── Single-node dispatch ─────────────────────────────────────────────────

    pub async fn start_node(&self, id: &str) -> Result<Response, NodeError> {
        debug!(ctrl = %self.id, node = %id, op = "start");
        self.get(id)?.start().await
    }

    pub async fn stop_node(&self, id: &str) -> Result<Response, NodeError> {
        debug!(ctrl = %self.id, node = %id, op = "stop");
        self.get(id)?.stop().await
    }

    pub async fn query_node(&self, id: &str) -> Result<Response, NodeError> {
        debug!(ctrl = %self.id, node = %id, op = "query");
        self.get(id)?.query().await
    }

    pub async fn config_node(
        &self,
        id: &str,
        params: ConfigParams,
    ) -> Result<Response, NodeError> {
        debug!(ctrl = %self.id, node = %id, op = "config");
        self.get(id)?.config(params).await
    }

    pub async fn test_node(&self, id: &str) -> Result<Response, NodeError> {
        debug!(ctrl = %self.id, node = %id, op = "test");
        self.get(id)?.test().await
    }

    // ── Broadcast (concurrent) ───────────────────────────────────────────────

    /// Start every registered node concurrently.
    pub async fn start_all(&self) -> HashMap<String, Result<Response, NodeError>> {
        self.broadcast(|n| async move { n.start().await }).await
    }

    /// Stop every registered node concurrently.
    pub async fn stop_all(&self) -> HashMap<String, Result<Response, NodeError>> {
        self.broadcast(|n| async move { n.stop().await }).await
    }

    /// Query every registered node concurrently.
    pub async fn query_all(&self) -> HashMap<String, Result<Response, NodeError>> {
        self.broadcast(|n| async move { n.query().await }).await
    }

    /// Run the self-test on every registered node concurrently.
    pub async fn test_all(&self) -> HashMap<String, Result<Response, NodeError>> {
        self.broadcast(|n| async move { n.test().await }).await
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn get(&self, id: &str) -> Result<Arc<dyn NodeInterface>, NodeError> {
        self.nodes
            .get(id)
            .cloned()
            .ok_or_else(|| NodeError::NotFound(id.to_string()))
    }

    /// Generic concurrent broadcast: call `f` on every node in parallel and
    /// collect the results into a `HashMap<node_id, Result>`.
    async fn broadcast<F, Fut>(&self, f: F) -> HashMap<String, Result<Response, NodeError>>
    where
        F: Fn(Arc<dyn NodeInterface>) -> Fut,
        Fut: std::future::Future<Output = Result<Response, NodeError>> + Send + 'static,
    {
        let mut join_handles = Vec::with_capacity(self.nodes.len());

        for (id, node) in &self.nodes {
            let id = id.clone();
            let fut = f(Arc::clone(node));
            join_handles.push((id, tokio::spawn(fut)));
        }

        let mut results = HashMap::with_capacity(join_handles.len());
        for (id, handle) in join_handles {
            let result = handle.await.unwrap_or_else(|e| {
                Err(NodeError::Communication(format!("task join error: {}", e)))
            });
            results.insert(id, result);
        }
        results
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use midw_local::LocalNodeHandle;

    fn make_control_with_two_local_nodes() -> ControlNode {
        let mut ctrl = ControlNode::new("X");
        ctrl.register(Arc::new(LocalNodeHandle::new("A", 8)));
        ctrl.register(Arc::new(LocalNodeHandle::new("B", 8)));
        ctrl
    }

    #[tokio::test]
    async fn start_all_returns_ok_for_all_nodes() {
        let ctrl = make_control_with_two_local_nodes();
        let results = ctrl.start_all().await;
        assert_eq!(results.len(), 2);
        for (id, res) in &results {
            assert!(
                matches!(res, Ok(Response::Ok)),
                "node {} returned unexpected {:?}",
                id,
                res
            );
        }
    }

    #[tokio::test]
    async fn query_all_after_start() {
        let ctrl = make_control_with_two_local_nodes();
        ctrl.start_all().await;
        let results = ctrl.query_all().await;
        for (_, res) in results {
            match res.unwrap() {
                Response::Status(s) => assert!(s.running),
                other => panic!("unexpected: {:?}", other),
            }
        }
    }

    #[tokio::test]
    async fn not_found_error_for_unknown_node() {
        let ctrl = make_control_with_two_local_nodes();
        let err = ctrl.start_node("Z").await.unwrap_err();
        assert!(matches!(err, NodeError::NotFound(id) if id == "Z"));
    }

    #[tokio::test]
    async fn test_all_fails_before_start() {
        let ctrl = make_control_with_two_local_nodes();
        let results = ctrl.test_all().await;
        for (_, res) in results {
            match res.unwrap() {
                Response::TestResult(t) => assert!(!t.passed),
                other => panic!("unexpected: {:?}", other),
            }
        }
    }

    #[tokio::test]
    async fn config_single_node() {
        let ctrl = make_control_with_two_local_nodes();
        let resp = ctrl
            .config_node(
                "A",
                ConfigParams {
                    key: "max_retries".into(),
                    value: "5".into(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(resp, Response::Ok));
    }

    #[tokio::test]
    async fn node_ids_are_sorted() {
        let ctrl = make_control_with_two_local_nodes();
        let ids = ctrl.node_ids();
        assert_eq!(ids, vec!["A", "B"]);
    }
}
