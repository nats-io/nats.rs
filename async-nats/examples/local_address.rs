// Copyright 2020-2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Demonstrates connecting to a NATS server while binding the client socket
/// to a specific local address. This is useful on machines with multiple
/// network interfaces when you need to control which one is used, or when
/// you need to bind to a specific local port.
///
/// Usage:
///   cargo run --example local_address -- <local_addr> [nats_url]
///
/// Examples:
///   cargo run --example local_address -- 127.0.0.1:0
///   cargo run --example local_address -- 127.0.0.1:9898 nats://localhost:4222
use std::env;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let local_addr: std::net::SocketAddr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:0".to_string())
        .parse()
        .expect("invalid socket address, expected format: IP:PORT (e.g. 127.0.0.1:0)");

    let nats_url = env::args()
        .nth(2)
        .unwrap_or_else(|| "nats://localhost:4222".to_string());

    println!("Connecting to {nats_url} using local address {local_addr}");

    let client = async_nats::ConnectOptions::new()
        .local_address(local_addr)
        .connect(&nats_url)
        .await?;

    // Publish a test message to verify the connection works.
    client
        .publish("local_address.test", "Hello from bound address!".into())
        .await?;
    client.flush().await?;

    println!("Published a message successfully from {local_addr}");

    Ok(())
}
