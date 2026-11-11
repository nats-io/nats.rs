// Copyright 2026 The NATS Authors
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

//! How the client takes the servers a cluster reports in `connect_urls`, and how it
//! connects to them. Scripted servers observe what the client sends; reconnects are
//! forced and steered to the server under test, so no test waits on the client noticing
//! a closed connection.

mod discovered_servers {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_nats::{ConnectOptions, ReconnectToServer};
    use serde_json::{json, Value};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };
    use tokio_util::sync::CancellationToken;

    fn info_line(info: &Value) -> Vec<u8> {
        format!("INFO {info}\r\n").into_bytes()
    }

    fn server_info(port: u16, extra: Value) -> Value {
        let mut info = json!({
            "server_id": format!("FAKE-{port}"),
            "server_name": format!("fake-{port}"),
            "version": "2.14.0",
            "proto": 1,
            "host": "127.0.0.1",
            "port": port,
            "headers": true,
            "max_payload": 1048576,
        });
        if let (Some(info), Some(extra)) = (info.as_object_mut(), extra.as_object()) {
            info.extend(extra.clone());
        }
        info
    }

    /// A scripted server on 127.0.0.1. It sends INFO with `info` merged in, sends every
    /// CONNECT it receives to `connects`, and answers every PING with PONG. Dropping it
    /// closes the listener and every connection.
    struct FakeServer {
        port: u16,
        connects: mpsc::UnboundedReceiver<Value>,
        stop: CancellationToken,
    }

    impl Drop for FakeServer {
        fn drop(&mut self) {
            self.stop.cancel();
        }
    }

    impl FakeServer {
        async fn plain(info: Value) -> FakeServer {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (tx, connects) = mpsc::unbounded_channel();
            let stop = CancellationToken::new();
            let token = stop.clone();
            tokio::spawn(async move {
                loop {
                    let stream = tokio::select! {
                        _ = token.cancelled() => return,
                        accepted = listener.accept() => match accepted {
                            Ok((stream, _)) => stream,
                            Err(_) => return,
                        },
                    };
                    let (info, tx, token) = (info.clone(), tx.clone(), token.clone());
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = token.cancelled() => {}
                            _ = serve_connection(stream, port, info, tx) => {}
                        }
                    });
                }
            });
            FakeServer {
                port,
                connects,
                stop,
            }
        }

        fn url(&self) -> String {
            format!("nats://127.0.0.1:{}", self.port)
        }

        /// The next CONNECT this server receives, if one arrives within five seconds.
        async fn next_connect(&mut self) -> Option<Value> {
            tokio::time::timeout(Duration::from_secs(5), self.connects.recv())
                .await
                .ok()
                .flatten()
        }
    }

    async fn serve_connection(
        mut stream: TcpStream,
        port: u16,
        info: Value,
        connects: mpsc::UnboundedSender<Value>,
    ) -> std::io::Result<()> {
        stream
            .write_all(&info_line(&server_info(port, info)))
            .await?;

        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
            while let Some(end) = buf.windows(2).position(|w| w == b"\r\n") {
                let line: Vec<u8> = buf.drain(..end + 2).collect();
                if let Some(connect) = line.strip_prefix(b"CONNECT ") {
                    if let Ok(connect) = serde_json::from_slice(&connect[..connect.len() - 2]) {
                        connects.send(connect).ok();
                    }
                } else if line == b"PING\r\n" {
                    stream.write_all(b"PONG\r\n").await?;
                }
            }
        }
    }

    /// Steers reconnects: the reconnect callback picks the pool entry with the port set
    /// here, without a delay.
    #[derive(Clone, Default)]
    struct Steer(Arc<Mutex<Option<u16>>>);

    impl Steer {
        fn to(&self, port: u16) {
            *self.0.lock().unwrap() = Some(port);
        }

        fn options(&self, options: ConnectOptions) -> ConnectOptions {
            let target = self.0.clone();
            options.reconnect_to_server_callback(move |servers, _| {
                let port = *target.lock().unwrap();
                let server = servers
                    .into_iter()
                    .find(|server| Some(server.addr.port()) == port);
                async move {
                    server.map(|server| ReconnectToServer {
                        addr: server.addr,
                        delay: Some(Duration::ZERO),
                    })
                }
            })
        }
    }

    async fn pool_urls(client: &async_nats::Client) -> Vec<String> {
        client
            .server_pool()
            .await
            .unwrap()
            .iter()
            .map(|server| server.addr.as_url_str().to_string())
            .collect()
    }

    // An entry with a scheme is skipped, not reduced to host:port: the server does not
    // choose how the client connects. The other entries are used.
    #[tokio::test]
    async fn entries_with_a_scheme_are_skipped() {
        let mut other = FakeServer::plain(json!({})).await;
        let seed = FakeServer::plain(json!({ "connect_urls": [
            "nats://127.0.0.1:1",
            "tls://127.0.0.1:1",
            "ws://127.0.0.1:1",
            "WSS://127.0.0.1:1",
            format!("127.0.0.1:{}", other.port),
        ]}))
        .await;
        let steer = Steer::default();

        let client = steer
            .options(ConnectOptions::new())
            .connect(seed.url())
            .await
            .unwrap();

        assert_eq!(pool_urls(&client).await, vec![seed.url(), other.url()]);
        steer.to(other.port);
        client.force_reconnect().await.unwrap();
        assert!(other.next_connect().await.is_some());
    }

    // An entry that is not host:port is skipped; the connect goes on with the others.
    #[tokio::test]
    async fn entries_that_are_not_host_port_are_skipped() {
        let seed = FakeServer::plain(json!({ "connect_urls": [
            "::::",
            "[::1",
            ":notaport",
            "a b:4222",
            "",
            "127.0.0.1",
            "127.0.0.1:4998/path",
            "127.0.0.1:4997?x=1",
            "127.0.0.1:4999",
        ]}))
        .await;

        let client = ConnectOptions::new().connect(seed.url()).await.unwrap();

        assert_eq!(
            pool_urls(&client).await,
            vec![seed.url(), "nats://127.0.0.1:4999".to_string()]
        );
    }

    // User info in an entry is dropped: the server is used, with the client's own
    // credentials.
    #[tokio::test]
    async fn user_info_in_an_entry_is_dropped() {
        let mut other = FakeServer::plain(json!({})).await;
        let seed = FakeServer::plain(json!({ "connect_urls": [
            format!("alice:secret@127.0.0.1:{}", other.port),
        ]}))
        .await;
        let steer = Steer::default();

        let client = steer
            .options(ConnectOptions::with_user_and_password(
                "derek".into(),
                "porkchop".into(),
            ))
            .connect(seed.url())
            .await
            .unwrap();

        assert_eq!(pool_urls(&client).await, vec![seed.url(), other.url()]);
        steer.to(other.port);
        client.force_reconnect().await.unwrap();
        let connect = other.next_connect().await.unwrap();
        assert_eq!(connect["user"], "derek");
        assert_eq!(connect["pass"], "porkchop");
    }
}
