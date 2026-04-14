//! `midw-node` — standalone IPC node server binary.
//!
//! Usage:
//! ```text
//! midw-node <NODE_ID> <SOCKET_PATH>
//! ```
//!
//! Example (run as separate processes for nodes C and D):
//! ```text
//! midw-node C /tmp/midw-C.sock &
//! midw-node D /tmp/midw-D.sock &
//! ```
//!
//! The process listens on `SOCKET_PATH` (Unix-domain socket), processes
//! commands from any connecting client and exits when it receives SIGINT/SIGTERM.

use midw_ipc::IpcNodeServer;
use std::process;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("midw_ipc=info".parse().unwrap())
                .add_directive("midw_node=info".parse().unwrap()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: midw-node <NODE_ID> <SOCKET_PATH>");
        process::exit(1);
    }

    let node_id = &args[1];
    let socket_path = &args[2];

    let server = IpcNodeServer::new(node_id.as_str(), socket_path.as_str());
    if let Err(e) = server.serve().await {
        eprintln!("Failed to start node '{}': {}", node_id, e);
        process::exit(1);
    }

    // Keep the process alive until Ctrl-C / SIGTERM.
    match tokio::signal::ctrl_c().await {
        Ok(()) => eprintln!("Node '{}' received shutdown signal", node_id),
        Err(e) => eprintln!("Signal error: {}", e),
    }
}
