//! # midw-demo
//!
//! End-to-end demonstration of the midw communication architecture.
//!
//! ## Topology
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
//! Nodes **A** and **B** run inside this process, communicating with X via
//! tokio channels (no serialisation overhead).
//!
//! Nodes **C** and **D** run on Unix-domain sockets — commands are serialised
//! as newline-delimited JSON, following DDS request/reply semantics.  In this
//! demo the IPC servers are started as tokio tasks in the same process, but
//! they could equally be separate OS processes (see `midw-node` binary).

use midw_common::{ConfigParams, Response};
use midw_control::ControlNode;
use midw_ipc::{IpcNodeHandle, IpcNodeServer};
use midw_local::LocalNodeHandle;
use std::sync::Arc;

const SOCKET_C: &str = "/tmp/midw-demo-C.sock";
const SOCKET_D: &str = "/tmp/midw-demo-D.sock";

// ── Formatting helpers ────────────────────────────────────────────────────────

fn print_section(title: &str) {
    println!();
    println!("┌─────────────────────────────────────────────────────┐");
    println!("│  {:<51}│", title);
    println!("└─────────────────────────────────────────────────────┘");
}

fn print_result(node_id: &str, result: &Result<Response, midw_common::NodeError>) {
    match result {
        Ok(Response::Ok) => println!("  [{node_id}]  ✓  Ok"),
        Ok(Response::Status(s)) => println!(
            "  [{node_id}]  ✓  Status: running={}, uptime={}s, msgs={}",
            s.running, s.uptime_secs, s.message_count
        ),
        Ok(Response::TestResult(t)) => println!(
            "  [{node_id}]  {}  Test: {} — {}",
            if t.passed { "✓" } else { "✗" },
            if t.passed { "PASS" } else { "FAIL" },
            t.message
        ),
        Ok(Response::Error(e)) => println!("  [{node_id}]  ✗  Node error: {e}"),
        Err(e) => println!("  [{node_id}]  ✗  Transport error: {e}"),
    }
}

fn print_all(results: &std::collections::HashMap<String, Result<Response, midw_common::NodeError>>) {
    let mut ids: Vec<&str> = results.keys().map(String::as_str).collect();
    ids.sort_unstable();
    for id in ids {
        print_result(id, &results[id]);
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialise structured logging (suppress most internal logs so the demo
    // output stays readable; set RUST_LOG=debug for full traces).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("midw=warn".parse().unwrap()),
        )
        .init();

    println!("╔═════════════════════════════════════════════════════╗");
    println!("║     midw — DDS-inspired Communication Architecture  ║");
    println!("╠═════════════════════════════════════════════════════╣");
    println!("║  Control Node X                                     ║");
    println!("║    ├── Node A  (local / tokio channel)              ║");
    println!("║    ├── Node B  (local / tokio channel)              ║");
    println!("║    ├── Node C  (IPC   / Unix-domain socket)         ║");
    println!("║    └── Node D  (IPC   / Unix-domain socket)         ║");
    println!("╚═════════════════════════════════════════════════════╝");

    // ── Build node handles ────────────────────────────────────────────────────

    // Local nodes A and B: in-process tokio task + channel.
    let node_a = Arc::new(LocalNodeHandle::new("A", 32));
    let node_b = Arc::new(LocalNodeHandle::new("B", 32));

    // IPC nodes C and D: bind Unix sockets, serve on background tasks.
    IpcNodeServer::new("C", SOCKET_C).serve().await?;
    IpcNodeServer::new("D", SOCKET_D).serve().await?;
    let node_c = Arc::new(IpcNodeHandle::new("C", SOCKET_C));
    let node_d = Arc::new(IpcNodeHandle::new("D", SOCKET_D));

    // ── Wire everything into control node X ──────────────────────────────────

    let mut ctrl = ControlNode::new("X");
    ctrl.register(node_a);
    ctrl.register(node_b);
    ctrl.register(node_c);
    ctrl.register(node_d);

    println!();
    println!("Registered nodes: {:?}", ctrl.node_ids());

    // ══════════════════════════════════════════════════════════════════════════
    // 1. start()  — broadcast to all four nodes
    // ══════════════════════════════════════════════════════════════════════════
    print_section("1. start() — broadcast to all nodes");
    let results = ctrl.start_all().await;
    print_all(&results);

    // ══════════════════════════════════════════════════════════════════════════
    // 2. query()  — broadcast to all four nodes
    // ══════════════════════════════════════════════════════════════════════════
    print_section("2. query() — broadcast to all nodes");
    let results = ctrl.query_all().await;
    print_all(&results);

    // ══════════════════════════════════════════════════════════════════════════
    // 3. config() — targeted at single nodes
    // ══════════════════════════════════════════════════════════════════════════
    print_section("3. config() — targeted commands");
    for (node, key, value) in [
        ("A", "log_level", "debug"),
        ("B", "max_queue", "256"),
        ("C", "timeout_ms", "500"),
        ("D", "retry_count", "3"),
    ] {
        let resp = ctrl
            .config_node(
                node,
                ConfigParams {
                    key: key.to_string(),
                    value: value.to_string(),
                },
            )
            .await;
        print_result(node, &resp);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4. test()   — broadcast self-test to all nodes
    // ══════════════════════════════════════════════════════════════════════════
    print_section("4. test() — broadcast self-test");
    let results = ctrl.test_all().await;
    print_all(&results);

    // ══════════════════════════════════════════════════════════════════════════
    // 5. stop()   — broadcast to all four nodes
    // ══════════════════════════════════════════════════════════════════════════
    print_section("5. stop() — broadcast to all nodes");
    let results = ctrl.stop_all().await;
    print_all(&results);

    // ══════════════════════════════════════════════════════════════════════════
    // 6. Demonstrate error propagation: start an already-running node
    // ══════════════════════════════════════════════════════════════════════════
    print_section("6. Error handling — double-start node A");
    ctrl.start_node("A").await.ok();          // first start
    let resp = ctrl.start_node("A").await;    // second start → Error
    print_result("A", &resp);

    // ══════════════════════════════════════════════════════════════════════════
    // 7. Unknown node → NodeError::NotFound
    // ══════════════════════════════════════════════════════════════════════════
    print_section("7. Error handling — unknown node Z");
    let resp = ctrl.query_node("Z").await;
    print_result("Z", &resp);

    println!();
    println!("Demo complete.");

    Ok(())
}
