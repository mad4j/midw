//! `midw-dds-node` — standalone DDS node server binary.
//!
//! Usage:
//! ```text
//! midw-dds-node <NODE_ID> [udp|intra]
//! ```
//!
//! `intra` (default) uses `TransportMode::IntraProcess`; suitable for
//! same-process testing.  `udp` uses `TransportMode::UdpMulticast`; suitable
//! for cross-process / cross-machine deployment.
//!
//! Example — run nodes C and D as separate processes over UDP:
//! ```text
//! midw-dds-node C udp &
//! midw-dds-node D udp &
//! ```
//!
//! The node registers the DDS service `midw/node/<NODE_ID>` on DDS domain 0
//! and processes commands until it receives SIGINT/SIGTERM.

use hdds::TransportMode;
use midw_dds::DdsNodeServer;
use std::process;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("midw_dds=info".parse().unwrap())
                .add_directive("midw_dds_node=info".parse().unwrap()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: midw-dds-node <NODE_ID> [udp|intra]");
        process::exit(1);
    }

    let node_id = &args[1];
    let transport = match args.get(2).map(String::as_str) {
        Some("udp") => TransportMode::UdpMulticast,
        _ => TransportMode::IntraProcess,
    };

    let server = match DdsNodeServer::new(node_id.as_str(), transport) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to create node '{}': {}", node_id, e);
            process::exit(1);
        }
    };

    if let Err(e) = server.serve() {
        eprintln!("Failed to start DDS service for node '{}': {}", node_id, e);
        process::exit(1);
    }

    eprintln!(
        "Node '{}' started — DDS service midw/node/{} is ready",
        node_id, node_id
    );

    match tokio::signal::ctrl_c().await {
        Ok(()) => eprintln!("Node '{}' received shutdown signal", node_id),
        Err(e) => eprintln!("Signal error: {}", e),
    }
}
