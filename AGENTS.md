# AGENTS.md — AI Agent Instructions for nats.rs

## Project Overview

This is the official Rust client for [NATS.io](https://nats.io), a high-performance messaging system.
The active crate is **`async-nats`** — a fully async, Tokio-based client.

> **The `nats` crate (synchronous client) is deprecated.** It receives security fixes only. All new
> work happens in `async-nats`. Do not modify the `nats/` directory unless explicitly asked.

## Repository Structure

```
nats.rs/
├── async-nats/          # Primary crate — async NATS client (this is where you work)
│   ├── src/
│   │   ├── lib.rs       # Entry point, core types (Command, ServerOp, ClientOp, connect())
│   │   ├── client.rs    # Client handle — publish, subscribe, request, flush
│   │   ├── connection.rs# Low-level I/O — protocol parsing, read/write buffers
│   │   ├── connector.rs # Connection establishment, reconnection, auth handshake
│   │   ├── options.rs   # ConnectOptions builder
│   │   ├── error.rs     # Generic Error<Kind> pattern
│   │   ├── header.rs    # HeaderMap — NATS message headers
│   │   ├── subject.rs   # Subject type, ToSubject trait
│   │   ├── status.rs    # StatusCode (100-999 NATS protocol codes)
│   │   ├── message.rs   # Message types
│   │   ├── tls.rs       # TLS configuration
│   │   ├── auth.rs      # Auth trait definitions
│   │   ├── auth_utils.rs# Credential file parsing
│   │   ├── crypto.rs    # Crypto feature support
│   │   ├── id_generator.rs
│   │   ├── jetstream/   # JetStream API (feature-gated)
│   │   │   ├── context.rs    # JetStream context — streams, publishing
│   │   │   ├── stream.rs     # Stream management, consumer creation
│   │   │   ├── consumer/     # Pull, Push, Ordered consumers
│   │   │   ├── kv/           # Key-Value store (feature: "kv")
│   │   │   ├── object_store/ # Object store (feature: "object-store")
│   │   │   ├── errors.rs     # JetStream error codes
│   │   │   ├── message.rs    # JetStream message types
│   │   │   ├── publish.rs    # PublishAck
│   │   │   └── response.rs   # Response wrapper
│   │   └── service/     # Service API (feature: "service")
│   │       ├── mod.rs        # Service, ServiceBuilder
│   │       ├── endpoint.rs   # Endpoint handling
│   │       └── error.rs      # Service errors
│   ├── tests/           # Integration tests (require running nats-server)
│   │   ├── configs/     # NATS server config files for tests
│   │   ├── client_tests.rs
│   │   ├── jetstream_tests.rs
│   │   ├── kv_tests.rs
│   │   ├── object_store.rs
│   │   ├── service_tests.rs
│   │   └── ...
│   ├── examples/        # Runnable examples
│   └── benches/         # Criterion benchmarks
├── nats-server/         # Test harness — launches real nats-server instances
├── nats/                # DEPRECATED sync client — do not modify
└── nats-core/           # Experimental embedded/no_std client (separate)
```

## Build & Test Commands

```bash
# Format (required: nightly toolchain)
cargo +nightly fmt

# Lint — CI denies all clippy warnings
cargo clippy --benches --tests --examples --all-features -- --deny clippy::all

# Build
cargo build --all-targets

# Test (standard — requires nats-server binary in PATH)
cargo test --features slow_tests,websockets -- --nocapture

# Test specific TLS backend
cargo test tls --no-default-features \
  --features jetstream,kv,object-store,service,nkeys,nuid,crypto,websockets,ring

# Test feature combinations (thorough)
bash .cargo-hack-check.sh

# Build docs
cargo doc --no-deps --all-features

# Check MSRV (1.88.0)
cargo +1.88.0 check

# Check licenses
cargo deny check licenses
```

**nats-server**: Tests require the `nats-server` binary. Install via Go:
```bash
go install github.com/nats-io/nats-server/v2@main
```

## CI Requirements

All of these must pass before a PR is merged:

| Check | Command |
|-------|---------|
| Format | `cargo +nightly fmt -- --check` |
| Clippy | `cargo clippy --benches --tests --examples --all-features -- --deny clippy::all` |
| Tests | `cargo test --features slow_tests,websockets` |
| Docs | `cargo doc --no-deps --all-features` |
| MSRV | Build with Rust 1.88.0 |
| Licenses | `cargo deny check licenses` |
| Spelling | `cargo spellcheck --code 1` |
| Examples | `cargo check --examples` |
| Min versions | `cargo check --locked --all-features --all-targets` (with `-Zminimal-versions`) |
| TLS backends | Tests run separately with `ring`, `aws-lc-rs`, and `fips` |
| Platforms | Ubuntu, macOS, Windows |

**Environment**: CI sets `RUSTFLAGS="-D warnings"` — all warnings are errors.

## Formatting Rules

Defined in `.rustfmt.toml`:
- `max_width = 100`
- `reorder_imports = true`
- `format_code_in_doc_comments = true`
- Edition: 2018 (rustfmt setting — the crate itself is edition 2021)

Always run `cargo +nightly fmt` before committing.

## Architecture

### Internal Communication

```
Client (cloneable handle)
  │  sends Command variants via mpsc channel
  ▼
ConnectionHandler (single task)
  │  manages subscriptions, multiplexer, ping/pong
  │  drives reconnection via Connector
  ▼
Connection (protocol I/O)
  │  read_buf (BytesMut) / write_buf (VecDeque<Bytes>)
  │  parses ServerOp, serializes ClientOp
  ▼
NATS Server (TCP / TLS / WebSocket)
```

- `Client` is a lightweight, cloneable handle. All protocol work happens in `ConnectionHandler`.
- Commands: `Publish`, `Request`, `Subscribe`, `Unsubscribe`, `Flush`, `Drain`, `Reconnect`.
- Request-reply uses a multiplexer: one subscription handles all requests via inbox tokens.

### Key Types

| Type | Location | Purpose |
|------|----------|---------|
| `Client` | `client.rs` | User-facing handle — publish, subscribe, request |
| `Connection` | `connection.rs` | Protocol I/O — reads `ServerOp`, writes `ClientOp` |
| `Connector` | `connector.rs` | Establishes connections, handles reconnection |
| `ConnectOptions` | `options.rs` | Builder for connection configuration |
| `Subject` | `subject.rs` | Validated subject string backed by `Bytes` |
| `HeaderMap` | `header.rs` | NATS message headers (`HashMap<HeaderName, Vec<HeaderValue>>`) |
| `StatusCode` | `status.rs` | Protocol status codes (100, 200, 404, 408, 409, 503) |
| `jetstream::Context` | `jetstream/context.rs` | JetStream API entry point |
| `jetstream::stream::Stream` | `jetstream/stream.rs` | Stream management |
| `Consumer<T>` | `jetstream/consumer/` | Pull/Push/Ordered consumers |
| `kv::Store` | `jetstream/kv/` | Key-Value store API |
| `object_store::ObjectStore` | `jetstream/object_store/` | Object store API |
| `service::Service` | `service/mod.rs` | Service API |

## Feature Flags

```toml
# Default: everything enabled
default = ["server_2_10", "server_2_11", "server_2_12", "service", "ring",
           "jetstream", "nkeys", "crypto", "object-store", "kv", "websockets", "nuid"]

# Subsystems (each gates a module)
jetstream       # JetStream API — pulls in time, serde_nanos, tryhard, base64
kv              # Key-Value store (requires jetstream)
object-store    # Object store (requires jetstream + crypto)
service         # Service API — pulls in time, serde_nanos

# Crypto backends (pick one)
ring            # Default crypto backend
aws-lc-rs       # Alternative backend
fips            # FIPS mode (requires aws-lc-rs)

# Other
nkeys           # NKey authentication
nuid            # NUID-based ID generation (falls back to rand if disabled)
crypto          # Encryption support
websockets      # WebSocket transport
experimental    # Experimental features

# Server version markers (no code, just enable version-specific fields/methods)
server_2_10
server_2_11
server_2_12

# Test-only
slow_tests              # Long-running, time-sensitive tests
compatibility_tests     # Cross-client compatibility tests
```

**When adding new code gated by a feature**, follow this pattern:
```rust
#[cfg(feature = "jetstream")]
#[cfg_attr(docsrs, doc(cfg(feature = "jetstream")))]
pub mod jetstream;
```

Always add the `docsrs` annotation so docs.rs shows feature requirements.

## Error Handling Pattern

The crate uses a generic `Error<Kind>` type. Every subsystem defines its own `ErrorKind` enum:

```rust
// 1. Define the kind enum
#[derive(Clone, Debug, PartialEq)]
pub enum FooErrorKind {
    NotFound,
    TimedOut,
    Other,
}

impl Display for FooErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "not found"),
            Self::TimedOut => write!(f, "timed out"),
            Self::Other => write!(f, "unknown error"),
        }
    }
}

// 2. Define the error type alias
pub type FooError = Error<FooErrorKind>;

// 3. Construct errors
FooError::new(FooErrorKind::NotFound)
FooError::with_source(FooErrorKind::TimedOut, source_error)

// 4. Match on errors
match result {
    Err(err) if err.kind() == FooErrorKind::NotFound => { /* handle */ }
    Err(err) => return Err(err.into()),
    Ok(val) => val,
}
```

Rules:
- Never `unwrap()` or `expect()` in library code.
- Chain error sources with `.with_source(kind, source)`.
- Tests assert on `.kind()`, not string messages.
- The top-level `Error` type is `Box<dyn std::error::Error + Send + Sync + 'static>`.

## Testing Patterns

### Server Setup

Tests use the `nats-server` crate to launch real server instances:

```rust
use nats_server;

#[tokio::test]
async fn my_test() {
    // Basic server
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();

    // With options
    let client = async_nats::ConnectOptions::new()
        .event_callback(|event| async move { println!("{event:?}") })
        .connect(server.client_url())
        .await
        .unwrap();

    // Cluster (3 nodes)
    let cluster = nats_server::run_cluster("tests/configs/jetstream.conf");
}
```

- Server configs live in `tests/configs/`.
- Servers clean up JetStream storage on drop.
- Use `server.restart()` to test reconnection.

### Feature-Gated Tests

```rust
#[cfg(feature = "kv")]
mod kv_tests {
    #[tokio::test]
    async fn create_bucket() { /* ... */ }
}
```

### Error Assertions

```rust
let err = client.publish("bad subject", "".into()).await.unwrap_err();
assert_eq!(err.kind(), PublishErrorKind::BadSubject);
```

## Code Conventions

### API Design
- **Builder pattern** for complex configurations (`ConnectOptions`, `ServiceBuilder`).
- **`ToSubject` trait** — methods accepting subjects are generic: `fn publish(&self, subject: impl ToSubject, ...)`.
- **Async everywhere** — all I/O methods are async, using Tokio.
- **`Stream` trait** — subscribers and consumers implement `futures::Stream`.
- **`Clone`-friendly** — `Client` is cheap to clone (Arc internals).

### Naming
- Error types: `FooError` (type alias for `Error<FooErrorKind>`).
- Error kinds: `FooErrorKind` enum.
- Feature-gated modules match feature names (`jetstream`, `kv`, `service`).
- Config structs are named `Config` within their module.

### Imports
- Group: std → external crates → crate-internal.
- `use crate::error::Error;` for the generic error type.
- `use super::*` is used sparingly (mainly in tests).

### Dependencies
- New dependencies must have licenses allowed in `deny.toml`: MIT, Apache-2.0, ISC, BSD-2-Clause, BSD-3-Clause.
- Consider MSRV (1.88.0) when adding dependencies.
- Feature-gate optional dependencies with `dep:` syntax: `dep:time`, `dep:serde_nanos`.

### Docs
- All public items must have doc comments.
- Use `/// # Examples` with code blocks.
- Use `no_run` for examples that need a server: ` ```no_run `.
- Feature-gated items get `#[cfg_attr(docsrs, doc(cfg(feature = "...")))]`.

## Commit & PR Standards

- Linear history — rebase, no merge commits.
- Atomic, reasonably-sized commits.
- Well-formed commit messages ([reference](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)).
- Discuss changes via issues before starting work.
- Raise PRs early for visibility.

## License

Apache-2.0. All source files carry the license header:
```
// Copyright 2020-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0
```

Do not remove or modify license headers. New files should include them.
