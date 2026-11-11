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
        path::PathBuf,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_nats::{ConnectOptions, ReconnectToServer};
    use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    use serde_json::{json, Value};
    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };
    use tokio_util::sync::CancellationToken;

    fn certs(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/configs/certs")
            .join(name)
    }

    fn acceptor(cert: &str, key: &str) -> tokio_rustls::TlsAcceptor {
        let chain = CertificateDer::pem_file_iter(certs(cert))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_file(certs(key)).unwrap();
        let config = async_nats::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
        tokio_rustls::TlsAcceptor::from(Arc::new(config))
    }

    #[cfg(feature = "websockets")]
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// Writes a server config to a temporary file. `{certs}` expands to the certificate
    /// directory with `/` as the separator: the server reads `\` in a quoted string as an
    /// escape, so a Windows path would not parse.
    #[cfg(feature = "websockets")]
    fn config(contents: &str) -> String {
        let dir = certs("").display().to_string().replace('\\', "/");
        let path =
            std::env::temp_dir().join(format!("discovered-servers-{}.conf", rand::random::<u64>()));
        std::fs::write(
            &path,
            contents.replace("{certs}", dir.trim_end_matches('/')),
        )
        .unwrap();
        path.display().to_string()
    }

    trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
    impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

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

    /// A scripted server on 127.0.0.1. It sends INFO with `info` merged in, upgrades to
    /// TLS after INFO when it has a certificate, sends every CONNECT it receives to
    /// `connects`, and answers every PING with PONG. Dropping it closes the listener and
    /// every connection.
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
            FakeServer::start(info, None).await
        }

        /// A server that requires TLS, with a certificate for `localhost` from the test CA.
        async fn tls(mut info: Value) -> FakeServer {
            info["tls_required"] = json!(true);
            FakeServer::start(info, Some(("server-cert.pem", "server-key.pem"))).await
        }

        async fn start(info: Value, tls: Option<(&'static str, &'static str)>) -> FakeServer {
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
                            _ = serve_connection(stream, port, info, tls, tx) => {}
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
        tls: Option<(&'static str, &'static str)>,
        connects: mpsc::UnboundedSender<Value>,
    ) -> std::io::Result<()> {
        stream
            .write_all(&info_line(&server_info(port, info)))
            .await?;
        let mut stream: Box<dyn Stream> = match tls {
            Some((cert, key)) => Box::new(acceptor(cert, key).accept(stream).await?),
            None => Box::new(stream),
        };

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

    /// Listens on 127.0.0.1, sends an INFO that offers TLS without requiring it, like a
    /// server with `allow_non_tls`, and reports the first bytes each client sends back.
    async fn canary_offering_tls() -> (u16, mpsc::UnboundedReceiver<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let info = server_info(port, json!({ "tls_available": true }));
                    let mut first = [0u8; 16];
                    if stream.write_all(&info_line(&info)).await.is_ok() {
                        if let Ok(n) = stream.read(&mut first).await {
                            tx.send(first[..n].to_vec()).ok();
                        }
                    }
                });
            }
        });
        (port, rx)
    }

    /// Waits until the client's server pool holds a server with `port`.
    #[cfg(feature = "websockets")]
    async fn wait_for_server(client: &async_nats::Client, port: u16) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let pool = client.server_pool().await.unwrap();
            if pool.iter().any(|server| server.addr.port() == port) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no server with port {port} in {pool:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Waits until the client is connected to the server named `name`.
    #[cfg(feature = "websockets")]
    async fn reconnected_to(client: &async_nats::Client, name: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while client.server_info().server_name != name {
            assert!(
                tokio::time::Instant::now() < deadline,
                "not connected to {name}: {:?}",
                client.server_info()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
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

    // A server learned on a connection with TLS is connected to with TLS, although
    // nothing asks for TLS and its own INFO does not require it. The seed is a nats://
    // URL whose INFO requires TLS.
    #[tokio::test]
    async fn server_learned_over_tls_is_connected_to_with_tls() {
        let (canary_port, mut canary) = canary_offering_tls().await;
        let seed =
            FakeServer::tls(json!({ "connect_urls": [format!("127.0.0.1:{canary_port}")] })).await;
        let steer = Steer::default();

        let client = steer
            .options(
                ConnectOptions::new()
                    .add_root_certificates(certs("rootCA.pem"))
                    .tls_optional(),
            )
            .connect(format!("nats://localhost:{}", seed.port))
            .await
            .unwrap();

        steer.to(canary_port);
        client.force_reconnect().await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), canary.recv())
            .await
            .unwrap()
            .unwrap();
        // A TLS handshake record, not CONNECT.
        assert_eq!(first.first(), Some(&0x16), "{first:?}");
    }

    /// The addresses `server_pool()` lists, to give back to `set_server_pool()`.
    async fn listed_pool(client: &async_nats::Client) -> Vec<async_nats::ServerAddr> {
        client
            .server_pool()
            .await
            .unwrap()
            .into_iter()
            .map(|server| server.addr)
            .collect()
    }

    // server_pool() lists a server learned over TLS as tls://, so a pool made from that list,
    // here reordered, keeps TLS for it under tls_optional() too: the client still refuses the
    // plaintext INFO of that server and sends it no CONNECT.
    #[tokio::test]
    async fn a_pool_made_from_the_listed_pool_keeps_tls_under_tls_optional() {
        let mut plain = FakeServer::plain(json!({})).await;
        let seed =
            FakeServer::tls(json!({ "connect_urls": [format!("127.0.0.1:{}", plain.port)] })).await;
        let client = ConnectOptions::new()
            .add_root_certificates(certs("rootCA.pem"))
            .tls_optional()
            .retain_servers_order()
            .connect(format!("nats://localhost:{}", seed.port))
            .await
            .unwrap();
        let listed = format!("tls://127.0.0.1:{}", plain.port);
        assert_eq!(
            pool_urls(&client).await,
            [format!("nats://localhost:{}", seed.port), listed.clone()]
        );

        let mut pool = listed_pool(&client).await;
        pool.reverse();
        client.set_server_pool(pool).await.unwrap();
        client.force_reconnect().await.unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let error = loop {
            let pool = client.server_pool().await.unwrap();
            let server = pool.iter().find(|server| server.addr.port() == plain.port);
            if let Some(error) = server.and_then(|server| server.last_error.clone()) {
                break error;
            }
            assert!(tokio::time::Instant::now() < deadline, "{pool:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(
            error,
            format!("TLS error: the URL {listed:?} asks for TLS, but {listed:?} does not offer it")
        );
        assert!(plain.connects.try_recv().is_err());
    }

    // A wss:// client's server_pool() lists the servers it learned over wss as wss://, so a
    // pool made from that list is taken, and its servers are connected to with TLS. Given
    // in a pool, a discovered IP address is verified against itself; this certificate is
    // issued for 127.0.0.1.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn a_pool_made_from_the_listed_pool_of_a_wss_client_is_taken() {
        let ws_ports = [free_port(), free_port(), free_port()];
        let configs = ws_ports.map(|port| {
            config(&format!(
                "websocket {{ host: 127.0.0.1, port: {port}, tls {{ \
                 cert_file: \"{{certs}}/ip-cert.pem\", key_file: \"{{certs}}/ip-key.pem\" }} }}"
            ))
        });
        let _cluster = nats_server::run_cluster([
            configs[0].as_str(),
            configs[1].as_str(),
            configs[2].as_str(),
        ]);
        let steer = Steer::default();
        let client = steer
            .options(ConnectOptions::new().add_root_certificates(certs("ip-ca.pem")))
            .connect(format!("wss://localhost:{}", ws_ports[0]))
            .await
            .unwrap();
        wait_for_server(&client, ws_ports[1]).await;
        let listed = format!("wss://127.0.0.1:{}/", ws_ports[1]);
        assert!(pool_urls(&client).await.contains(&listed));

        client
            .set_server_pool(listed_pool(&client).await)
            .await
            .unwrap();
        steer.to(ws_ports[1]);
        client.force_reconnect().await.unwrap();
        reconnected_to(&client, "node2").await;
        client.flush().await.unwrap();
    }

    // A server learned on a websocket connection is connected to over websocket.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn server_learned_on_a_websocket_is_connected_to_over_websocket() {
        let ws_ports = [free_port(), free_port(), free_port()];
        let configs = ws_ports.map(|port| {
            config(&format!(
                "websocket {{ host: 127.0.0.1, port: {port}, no_tls: true }}"
            ))
        });
        let _cluster = nats_server::run_cluster([
            configs[0].as_str(),
            configs[1].as_str(),
            configs[2].as_str(),
        ]);
        let steer = Steer::default();

        let client = steer
            .options(ConnectOptions::new())
            .connect(format!("ws://127.0.0.1:{}", ws_ports[0]))
            .await
            .unwrap();
        wait_for_server(&client, ws_ports[1]).await;

        steer.to(ws_ports[1]);
        client.force_reconnect().await.unwrap();
        reconnected_to(&client, "node2").await;
        client.flush().await.unwrap();
    }
}
