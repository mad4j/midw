# midw — DDS-inspired Communication Middleware in Rust

A reference implementation of a **DDS-inspired** (Data Distribution Service)
communication architecture in Rust, built with `tokio`.

## Architecture

```
           ┌──────────────────────────────────────────────┐
           │                Control Node X                │
           │  start / stop / query / config / test        │
           └────┬──────────┬──────────┬──────────┬────────┘
                │          │          │          │
         (local chan) (local chan) (Unix sock) (Unix sock)
                │          │          │          │
             Node A     Node B     Node C     Node D
           (local)    (local)     (IPC)      (IPC)
```

| Node | Transport | Mechanism |
|------|-----------|-----------|
| A, B | Local (in-process) | tokio `mpsc` + `oneshot` channels |
| C, D | IPC (separate process) | Unix-domain socket, JSON wire protocol |

All nodes expose the same five-operation interface:

| Operation | Description |
|-----------|-------------|
| `start()` | Transition the node to the *running* state |
| `stop()` | Transition the node to the *stopped* state |
| `query()` | Return a `NodeStatus` snapshot |
| `config(key, value)` | Apply a key/value configuration parameter |
| `test()` | Run the node self-test, return a `TestResult` |

Control node **X** can dispatch any operation to a single named node or
**broadcast** it concurrently to all registered nodes.

## Crate layout

```
crates/
  midw-common/   # NodeInterface trait, Command/Response protocol types
  midw-local/    # LocalNodeHandle — in-process node via tokio channels
  midw-ipc/      # IpcNodeServer + IpcNodeHandle — Unix-socket IPC transport
                 # bin/midw-node — standalone IPC node server binary
  midw-control/  # ControlNode X — single-dispatch and broadcast API
  midw-demo/     # End-to-end demo binary
```

## Quick start

```bash
# Build everything
cargo build

# Run the end-to-end demo (starts A/B locally, C/D over IPC sockets)
cargo run --bin midw-demo

# Run all tests
cargo test

# Run nodes C and D as real separate processes
cargo build --release
./target/release/midw-node C /tmp/midw-C.sock &
./target/release/midw-node D /tmp/midw-D.sock &
```

## Design notes

* The `NodeInterface` trait (`async_trait`) is the single abstraction that all
  node handles implement.  `ControlNode` holds `Arc<dyn NodeInterface>` values,
  making the transport completely transparent to business logic.
* Local nodes use `tokio::sync::mpsc` + `oneshot` for zero-copy,
  zero-serialisation communication.
* IPC nodes use a newline-delimited JSON-over-Unix-socket protocol:
  one connection → one request → one response.  This maps naturally to the
  DDS *request-reply* QoS pattern.
* Broadcast operations (`start_all`, `query_all`, …) spawn one tokio task per
  node and await them concurrently via `tokio::spawn` / `JoinHandle`, giving
  fan-out parallelism.
