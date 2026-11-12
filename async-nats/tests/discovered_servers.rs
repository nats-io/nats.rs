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

    use async_nats::{ConnectErrorKind, ConnectOptions, ReconnectToServer};
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

    /// How a scripted server uses TLS: the certificate and key it presents, and whether it
    /// starts with the TLS handshake (`handshake_first`) instead of upgrading after INFO.
    #[derive(Clone, Copy)]
    struct Tls {
        cert: &'static str,
        key: &'static str,
        first: bool,
    }

    /// A scripted server on 127.0.0.1. It sends INFO with `info` merged in, uses TLS when
    /// it has a certificate, sends every CONNECT it receives to `connects` with the name
    /// the client sent as SNI added as `sni`, and answers every PING with PONG. Like
    /// nats-server 2.12 and later, it sends the INFO again after the first PONG, inside TLS
    /// when it has TLS. Dropping it closes the listener and every connection.
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
        async fn tls(info: Value) -> FakeServer {
            FakeServer::tls_with(info, "server-cert.pem", "server-key.pem").await
        }

        /// A server that requires TLS and presents `cert`.
        async fn tls_with(mut info: Value, cert: &'static str, key: &'static str) -> FakeServer {
            info["tls_required"] = json!(true);
            let tls = Tls {
                cert,
                key,
                first: false,
            };
            FakeServer::start(info, Some(tls)).await
        }

        /// A server that starts with the TLS handshake and sends INFO over TLS.
        async fn tls_first(mut info: Value) -> FakeServer {
            info["tls_required"] = json!(true);
            let tls = Tls {
                cert: "server-cert.pem",
                key: "server-key.pem",
                first: true,
            };
            FakeServer::start(info, Some(tls)).await
        }

        async fn start(info: Value, tls: Option<Tls>) -> FakeServer {
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
            self.connect_within(Duration::from_secs(5)).await
        }

        async fn connect_within(&mut self, wait: Duration) -> Option<Value> {
            tokio::time::timeout(wait, self.connects.recv())
                .await
                .ok()
                .flatten()
        }
    }

    async fn serve_connection(
        mut stream: TcpStream,
        port: u16,
        info: Value,
        tls: Option<Tls>,
        connects: mpsc::UnboundedSender<Value>,
    ) -> std::io::Result<()> {
        let info = info_line(&server_info(port, info));
        let mut sni = None;
        let mut stream: Box<dyn Stream> = match tls {
            Some(tls) => {
                if !tls.first {
                    stream.write_all(&info).await?;
                }
                let mut stream = acceptor(tls.cert, tls.key).accept(stream).await?;
                sni = stream.get_ref().1.server_name().map(str::to_owned);
                if tls.first {
                    stream.write_all(&info).await?;
                }
                Box::new(stream)
            }
            None => {
                stream.write_all(&info).await?;
                Box::new(stream)
            }
        };

        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let mut info_after_pong = Some(info);
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
            while let Some(end) = buf.windows(2).position(|w| w == b"\r\n") {
                let line: Vec<u8> = buf.drain(..end + 2).collect();
                if let Some(connect) = line.strip_prefix(b"CONNECT ") {
                    if let Ok(mut connect) =
                        serde_json::from_slice::<Value>(&connect[..connect.len() - 2])
                    {
                        connect["sni"] = json!(sni);
                        connects.send(connect).ok();
                    }
                } else if line == b"PING\r\n" {
                    stream.write_all(b"PONG\r\n").await?;
                    if let Some(info) = info_after_pong.take() {
                        stream.write_all(&info).await?;
                    }
                }
            }
        }
    }

    /// Steers the next reconnect: the reconnect callback picks the pool entry with the
    /// port set here, once, with the client's own reconnect delay. When that attempt fails,
    /// the client goes on as without the callback, so it can get back to a working server
    /// and answer queries again.
    #[derive(Clone, Default)]
    struct Steer(Arc<Mutex<Option<u16>>>);

    impl Steer {
        fn to(&self, port: u16) {
            *self.0.lock().unwrap() = Some(port);
        }

        fn options(&self, options: ConnectOptions) -> ConnectOptions {
            let target = self.0.clone();
            options.reconnect_to_server_callback(move |servers, _| {
                let port = target.lock().unwrap().take();
                let server = servers
                    .into_iter()
                    .find(|server| Some(server.addr.port()) == port);
                async move {
                    server.map(|server| ReconnectToServer {
                        addr: server.addr,
                        delay: None,
                    })
                }
            })
        }
    }

    /// Listens on 127.0.0.1, sends INFO with `info` merged in, and reports the first bytes
    /// each client sends back, none when it closes the connection.
    async fn canary(info: Value) -> (u16, mpsc::UnboundedReceiver<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let (tx, info) = (tx.clone(), info.clone());
                tokio::spawn(async move {
                    let info = server_info(port, info);
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

    /// Waits until the client's server pool holds a server with `port`. While the cluster
    /// is still forming, the list comes in an INFO the cluster sends when it changes.
    #[cfg(feature = "websockets")]
    async fn wait_for_server(client: &async_nats::Client, port: u16) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let pool = client.server_pool().await.unwrap();
            if pool.iter().any(|server| server.addr.port() == port) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no server with port {port} in {pool:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
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

    /// Waits until the server with `port` in the client's pool has a last error, and
    /// returns it.
    async fn last_error(client: &async_nats::Client, port: u16) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let pool = client.server_pool().await.unwrap();
            let server = pool.iter().find(|server| server.addr.port() == port);
            if let Some(error) = server.and_then(|server| server.last_error.clone()) {
                return error;
            }
            assert!(tokio::time::Instant::now() < deadline, "{pool:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Options that trust the test CA, which issued the `localhost` certificate.
    fn test_ca() -> ConnectOptions {
        ConnectOptions::new().add_root_certificates(certs("rootCA.pem"))
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
        // Its INFO offers TLS without requiring it, like a server with `allow_non_tls`.
        let (canary_port, mut canary) = canary(json!({ "tls_available": true })).await;
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

    // A discovered IP address learned over TLS is verified against the name the server
    // that reported it was verified under, over any number of hops, and that name is
    // sent as SNI. The certificate names localhost, not 127.0.0.1.
    #[tokio::test]
    async fn discovered_ip_is_verified_against_the_name_of_its_reporter() {
        let mut third = FakeServer::tls(json!({})).await;
        let mut second =
            FakeServer::tls(json!({ "connect_urls": [format!("127.0.0.1:{}", third.port)] })).await;
        let seed =
            FakeServer::tls(json!({ "connect_urls": [format!("127.0.0.1:{}", second.port)] }))
                .await;
        let steer = Steer::default();

        let client = steer
            .options(test_ca())
            .connect(format!("tls://localhost:{}", seed.port))
            .await
            .unwrap();

        steer.to(second.port);
        client.force_reconnect().await.unwrap();
        assert_eq!(second.next_connect().await.unwrap()["sni"], "localhost");
        steer.to(third.port);
        client.force_reconnect().await.unwrap();
        assert_eq!(third.next_connect().await.unwrap()["sni"], "localhost");
    }

    // The same with TLS first, where INFO and its list arrive over TLS.
    #[tokio::test]
    async fn discovered_ip_is_verified_against_the_name_of_its_reporter_with_tls_first() {
        let mut second = FakeServer::tls_first(json!({})).await;
        let seed = FakeServer::tls_first(
            json!({ "connect_urls": [format!("127.0.0.1:{}", second.port)] }),
        )
        .await;
        let steer = Steer::default();

        let client = steer
            .options(test_ca().tls_first())
            .connect(format!("tls://localhost:{}", seed.port))
            .await
            .unwrap();

        steer.to(second.port);
        client.force_reconnect().await.unwrap();
        assert_eq!(second.next_connect().await.unwrap()["sni"], "localhost");
    }

    // A discovered IP address learned without TLS, connected to with TLS because its own
    // INFO requires it, is one more address of the configured server's host name: it is
    // verified against that name, which is sent as SNI.
    #[tokio::test]
    async fn discovered_ip_learned_without_tls_is_verified_against_the_configured_name() {
        let mut second = FakeServer::tls(json!({})).await;
        let seed =
            FakeServer::plain(json!({ "connect_urls": [format!("127.0.0.1:{}", second.port)] }))
                .await;
        let steer = Steer::default();

        let client = steer
            .options(test_ca().tls_optional())
            .connect(format!("nats://localhost:{}", seed.port))
            .await
            .unwrap();

        steer.to(second.port);
        client.force_reconnect().await.unwrap();
        assert_eq!(second.next_connect().await.unwrap()["sni"], "localhost");
    }

    // A discovered host name learned without TLS is not trusted as a name: when its INFO
    // requires TLS, the attempt fails before the TLS handshake.
    #[tokio::test]
    async fn host_name_learned_without_tls_is_not_connected_to_over_tls() {
        let (canary_port, mut canary) = canary(json!({ "tls_required": true })).await;
        let seed =
            FakeServer::plain(json!({ "connect_urls": [format!("localhost:{canary_port}")] }))
                .await;
        let steer = Steer::default();

        let client = steer
            .options(ConnectOptions::new())
            .connect(seed.url())
            .await
            .unwrap();

        steer.to(canary_port);
        client.force_reconnect().await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), canary.recv())
            .await
            .unwrap()
            .unwrap();
        // The client closed the connection without a TLS handshake.
        assert_eq!(first, Vec::<u8>::new());
    }

    // A discovered IP address reported by a server reached by an IP address has no name
    // to be verified against. It is skipped: no attempt, so no failure is counted, and
    // the client connects to another server.
    #[tokio::test]
    async fn discovered_ip_without_a_name_is_skipped() {
        let mut second = FakeServer::tls_with(json!({}), "ip-cert.pem", "ip-key.pem").await;
        let mut seed = FakeServer::tls_with(
            json!({ "connect_urls": [format!("127.0.0.1:{}", second.port)] }),
            "ip-cert.pem",
            "ip-key.pem",
        )
        .await;
        let steer = Steer::default();

        let client = steer
            .options(ConnectOptions::new().add_root_certificates(certs("ip-ca.pem")))
            .connect(format!("tls://127.0.0.1:{}", seed.port))
            .await
            .unwrap();
        // No SNI is sent for an IP address.
        assert_eq!(seed.next_connect().await.unwrap()["sni"], Value::Null);

        steer.to(second.port);
        client.force_reconnect().await.unwrap();
        assert!(seed.next_connect().await.is_some());
        assert!(second
            .connect_within(Duration::from_millis(500))
            .await
            .is_none());
        let pool = client.server_pool().await.unwrap();
        let skipped = pool
            .iter()
            .find(|server| server.addr.port() == second.port)
            .unwrap();
        assert_eq!(skipped.failed_attempts, 0, "{pool:?}");
        assert_eq!(skipped.last_error, None, "{pool:?}");
    }

    // The name a discovered server inherits does not weaken the check: a certificate from
    // the test CA for another name is not accepted.
    #[tokio::test]
    async fn discovered_server_with_a_certificate_for_another_name_is_not_connected_to() {
        let mut second =
            FakeServer::tls_with(json!({}), "wrong-name-cert.pem", "wrong-name-key.pem").await;
        let seed =
            FakeServer::tls(json!({ "connect_urls": [format!("127.0.0.1:{}", second.port)] }))
                .await;
        let steer = Steer::default();

        let client = steer
            .options(test_ca())
            .connect(format!("tls://localhost:{}", seed.port))
            .await
            .unwrap();
        assert!(pool_urls(&client)
            .await
            .contains(&format!("tls://127.0.0.1:{}", second.port)));

        steer.to(second.port);
        client.force_reconnect().await.unwrap();
        assert!(second
            .connect_within(Duration::from_secs(1))
            .await
            .is_none());
    }

    // Nor is a certificate for the inherited name, and for 127.0.0.1, from a CA the
    // client does not trust.
    #[tokio::test]
    async fn discovered_server_with_a_certificate_from_another_ca_is_not_connected_to() {
        let second = FakeServer::tls_with(json!({}), "ip-cert.pem", "ip-key.pem").await;
        let seed =
            FakeServer::tls(json!({ "connect_urls": [format!("127.0.0.1:{}", second.port)] }))
                .await;
        let steer = Steer::default();

        let client = steer
            .options(test_ca())
            .connect(format!("tls://localhost:{}", seed.port))
            .await
            .unwrap();

        steer.to(second.port);
        client.force_reconnect().await.unwrap();

        let error = last_error(&client, second.port).await;
        assert!(error.contains("UnknownIssuer"), "{error}");
    }

    // tls_server_name() sets the name every server is verified against and that is sent as
    // SNI, the configured server given by its IP address and the servers it reports alike.
    // The certificate names localhost, not 127.0.0.1.
    #[tokio::test]
    async fn tls_server_name_is_verified_and_sent_for_every_server() {
        let mut second = FakeServer::tls(json!({})).await;
        let mut seed =
            FakeServer::tls(json!({ "connect_urls": [format!("127.0.0.1:{}", second.port)] }))
                .await;
        let steer = Steer::default();

        let client = steer
            .options(test_ca().tls_server_name("localhost"))
            .connect(format!("tls://127.0.0.1:{}", seed.port))
            .await
            .unwrap();
        assert_eq!(seed.next_connect().await.unwrap()["sni"], "localhost");

        steer.to(second.port);
        client.force_reconnect().await.unwrap();
        assert_eq!(second.next_connect().await.unwrap()["sni"], "localhost");
    }

    // One trailing dot is stripped from the name, for SNI and for the check.
    #[tokio::test]
    async fn one_trailing_dot_is_stripped_from_the_name() {
        let mut server = FakeServer::tls(json!({})).await;

        test_ca()
            .tls_server_name("localhost.")
            .connect(format!("tls://127.0.0.1:{}", server.port))
            .await
            .unwrap();

        assert_eq!(server.next_connect().await.unwrap()["sni"], "localhost");
    }

    // A tls_server_name() that is neither a host name nor an IP address fails the attempt
    // with an error that names the setting and the value.
    #[tokio::test]
    async fn names_the_tls_server_name_that_is_neither_a_host_name_nor_an_ip_address() {
        let server = FakeServer::tls(json!({})).await;

        let error = test_ca()
            .tls_server_name("localhost:4222")
            .connect(format!("tls://127.0.0.1:{}", server.port))
            .await
            .unwrap_err();

        assert_eq!(error.kind(), async_nats::ConnectErrorKind::Tls);
        assert_eq!(
            error.to_string(),
            r#"TLS error: tls_server_name() is set to "localhost:4222", which is neither a host name nor an IP address"#
        );
    }

    // verify_discovered_servers_by_ip() lets the client connect to a discovered IP address
    // that has no name to be verified against: it is verified against itself, with no
    // SNI. The certificate names 127.0.0.1.
    #[tokio::test]
    async fn verify_discovered_servers_by_ip_connects_to_a_discovered_ip_without_a_name() {
        let mut second = FakeServer::tls_with(json!({}), "ip-cert.pem", "ip-key.pem").await;
        let seed = FakeServer::tls_with(
            json!({ "connect_urls": [format!("127.0.0.1:{}", second.port)] }),
            "ip-cert.pem",
            "ip-key.pem",
        )
        .await;
        let steer = Steer::default();

        let client = steer
            .options(
                ConnectOptions::new()
                    .add_root_certificates(certs("ip-ca.pem"))
                    .verify_discovered_servers_by_ip(),
            )
            .connect(format!("tls://127.0.0.1:{}", seed.port))
            .await
            .unwrap();

        steer.to(second.port);
        client.force_reconnect().await.unwrap();
        assert_eq!(second.next_connect().await.unwrap()["sni"], Value::Null);
    }

    // A failed name check says which name the client expected, which names the certificate
    // is issued for, and which setting changes the name. The error rustls reported stays
    // reachable through the source of the error.
    #[tokio::test]
    async fn a_failed_name_check_names_the_expected_and_presented_names() {
        let server = FakeServer::tls(json!({})).await;

        let err = test_ca()
            .connect(format!("tls://127.0.0.1:{}", server.port))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ConnectErrorKind::Tls);
        assert_eq!(
            err.to_string(),
            format!(
                "TLS error: the certificate of \"tls://127.0.0.1:{}\" is not valid for \
                 \"127.0.0.1\": it is issued for \"localhost\", \"::1\"; tls_server_name() \
                 sets the name every server is verified against",
                server.port
            )
        );
        let mut causes = std::iter::successors(std::error::Error::source(&err), |err| err.source());
        let rustls_error = causes.find_map(|err| {
            err.downcast_ref::<std::io::Error>()?
                .get_ref()?
                .downcast_ref::<async_nats::rustls::Error>()
        });
        assert!(
            matches!(
                rustls_error,
                Some(async_nats::rustls::Error::InvalidCertificate(_))
            ),
            "{rustls_error:?}"
        );
    }

    // For a discovered server it also points at the remedy on the server's side.
    #[tokio::test]
    async fn a_failed_name_check_of_a_discovered_server_points_at_client_advertise() {
        let second =
            FakeServer::tls_with(json!({}), "wrong-name-cert.pem", "wrong-name-key.pem").await;
        let seed =
            FakeServer::tls(json!({ "connect_urls": [format!("127.0.0.1:{}", second.port)] }))
                .await;
        let steer = Steer::default();

        let client = steer
            .options(test_ca())
            .connect(format!("tls://localhost:{}", seed.port))
            .await
            .unwrap();
        steer.to(second.port);
        client.force_reconnect().await.unwrap();

        let error = last_error(&client, second.port).await;
        assert_eq!(
            error,
            format!(
                "TLS error: the certificate of \"nats://127.0.0.1:{}\" is not valid for \
                 \"localhost\": it is issued for \"wrong.example\"; tls_server_name() sets \
                 the name every server is verified against, or each server of the cluster \
                 can advertise a name its certificate is issued for (client_advertise in \
                 nats-server)",
                second.port
            )
        );
    }

    // Over a websocket with TLS too, a discovered IP address is verified against the name
    // the server that reported it was verified under. The certificate names localhost,
    // not 127.0.0.1.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn discovered_ip_is_verified_against_the_name_of_its_reporter_over_wss() {
        let ws_ports = [free_port(), free_port(), free_port()];
        let configs = ws_ports.map(|port| {
            config(&format!(
                "websocket {{ host: 127.0.0.1, port: {port}, tls {{ \
                 cert_file: \"{{certs}}/server-cert.pem\", \
                 key_file: \"{{certs}}/server-key.pem\" }} }}"
            ))
        });
        let _cluster = nats_server::run_cluster([
            configs[0].as_str(),
            configs[1].as_str(),
            configs[2].as_str(),
        ]);
        let steer = Steer::default();

        let client = steer
            .options(test_ca())
            .connect(format!("wss://localhost:{}", ws_ports[0]))
            .await
            .unwrap();
        wait_for_server(&client, ws_ports[1]).await;

        steer.to(ws_ports[1]);
        client.force_reconnect().await.unwrap();
        reconnected_to(&client, "node2").await;
        client.flush().await.unwrap();
    }
}
