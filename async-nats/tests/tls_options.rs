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

//! What the TLS options make the client do: which settings ask for TLS, and for which
//! servers. Scripted servers observe what the client sends; real servers check that it
//! connects.

mod tls_options {
    use std::{path::PathBuf, sync::Arc, time::Duration};

    use async_nats::ConnectOptions;
    use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    use serde_json::{json, Value};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };
    use tokio_util::sync::CancellationToken;

    fn certs(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/configs/certs")
            .join(name)
    }

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

    fn acceptor() -> tokio_rustls::TlsAcceptor {
        let chain = CertificateDer::pem_file_iter(certs("server-cert.pem"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_file(certs("server-key.pem")).unwrap();
        let config = async_nats::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
        tokio_rustls::TlsAcceptor::from(Arc::new(config))
    }

    /// A rustls client config that trusts the test CA.
    fn client_config() -> async_nats::rustls::ClientConfig {
        let mut roots = async_nats::rustls::RootCertStore::empty();
        roots.add_parsable_certificates(
            CertificateDer::pem_file_iter(certs("rootCA.pem"))
                .unwrap()
                .map(Result::unwrap),
        );
        async_nats::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    }

    /// A scripted server that requires TLS: it sends INFO with `tls_required`, upgrades
    /// to TLS, and answers every PING with PONG. Dropping it closes the
    /// listener and every connection.
    struct FakeServer {
        port: u16,
        stop: CancellationToken,
    }

    impl Drop for FakeServer {
        fn drop(&mut self) {
            self.stop.cancel();
        }
    }

    impl FakeServer {
        async fn tls(mut info: Value) -> FakeServer {
            info["tls_required"] = json!(true);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
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
                    let (info, token) = (info.clone(), token.clone());
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = token.cancelled() => {}
                            _ = serve_tls_connection(stream, port, info) => {}
                        }
                    });
                }
            });
            FakeServer { port, stop }
        }
    }

    async fn serve_tls_connection(
        mut stream: TcpStream,
        port: u16,
        info: Value,
    ) -> std::io::Result<()> {
        stream
            .write_all(&info_line(&server_info(port, info)))
            .await?;
        let mut stream = acceptor().accept(stream).await?;

        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(6).any(|w| w == b"PING\r\n") {
                buf.clear();
                stream.write_all(b"PONG\r\n").await?;
            }
        }
    }

    /// What a client did on a listener that speaks NATS without TLS.
    #[derive(Debug, PartialEq)]
    enum Seen {
        PlaintextConnect,
        PlaintextWebsocket,
        TlsHandshake,
        /// The client closed the connection without sending anything.
        Closed,
        Other(Vec<u8>),
    }

    /// A canary whose INFO neither requires nor offers TLS.
    async fn canary() -> (u16, mpsc::UnboundedReceiver<Seen>) {
        canary_with(json!({})).await
    }

    /// A canary whose INFO says that TLS is available but not required, like a server
    /// with `allow_non_tls`.
    async fn canary_offering_tls() -> (u16, mpsc::UnboundedReceiver<Seen>) {
        canary_with(json!({ "tls_available": true })).await
    }

    /// Listens on 127.0.0.1 and reports what each client does. A client that waits for
    /// the server gets an INFO without TLS, with `info` merged in.
    async fn canary_with(info: Value) -> (u16, mpsc::UnboundedReceiver<Seen>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let (tx, info) = (tx.clone(), info.clone());
                tokio::spawn(async move {
                    let mut first = [0u8; 16];
                    let read =
                        tokio::time::timeout(Duration::from_millis(300), stream.read(&mut first));
                    let n = match read.await {
                        Ok(Ok(n)) => n,
                        Ok(Err(_)) => return,
                        Err(_) => {
                            let info = info_line(&server_info(port, info));
                            if stream.write_all(&info).await.is_err() {
                                return;
                            }
                            let read = tokio::time::timeout(
                                Duration::from_secs(2),
                                stream.read(&mut first),
                            );
                            match read.await {
                                Ok(Ok(n)) => n,
                                _ => return,
                            }
                        }
                    };
                    let first = &first[..n];
                    let seen = if first.starts_with(b"CONNECT") {
                        Seen::PlaintextConnect
                    } else if first.starts_with(b"GET ") {
                        Seen::PlaintextWebsocket
                    } else if first.first() == Some(&0x16) {
                        Seen::TlsHandshake
                    } else if first.is_empty() {
                        Seen::Closed
                    } else {
                        Seen::Other(first.to_vec())
                    };
                    tx.send(seen).ok();
                });
            }
        });
        (port, rx)
    }

    /// Collects what a canary saw during `wait`.
    async fn seen_during(rx: &mut mpsc::UnboundedReceiver<Seen>, wait: Duration) -> Vec<Seen> {
        let mut seen = Vec::new();
        let deadline = tokio::time::Instant::now() + wait;
        while let Ok(Some(event)) = tokio::time::timeout_at(deadline, rx.recv()).await {
            seen.push(event);
        }
        seen
    }

    /// Credentials and the test CA, nothing else.
    fn tls_options() -> ConnectOptions {
        ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
            .add_root_certificates(certs("rootCA.pem"))
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

    /// Waits until the client's server pool holds `url`.
    async fn wait_for_server(client: &async_nats::Client, url: String) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let pool = pool_urls(client).await;
            if pool.contains(&url) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{url} not in {pool:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // A CA, a client certificate or a TLS client config asks for TLS, so a `nats://` URL
    // is connected to with TLS. The INFO of the server does not require TLS; that does
    // not turn it off.
    #[tokio::test]
    async fn tls_settings_use_tls_for_a_nats_url() {
        for (name, options) in [
            (
                "root certificates",
                ConnectOptions::new().add_root_certificates(certs("rootCA.pem")),
            ),
            (
                "client certificate",
                ConnectOptions::new()
                    .add_client_certificate(certs("client-cert.pem"), certs("client-key.pem")),
            ),
            (
                "client config",
                ConnectOptions::new().tls_client_config(client_config()),
            ),
        ] {
            let (port, mut canary) = canary_offering_tls().await;

            let result = options
                .user_and_password("derek".into(), "porkchop".into())
                .connection_timeout(Duration::from_millis(1500))
                .connect(format!("nats://127.0.0.1:{port}"))
                .await;

            assert!(result.is_err(), "{name}");
            let seen = seen_during(&mut canary, Duration::from_millis(200)).await;
            assert_eq!(seen, vec![Seen::TlsHandshake], "{name}");
        }
    }

    // One `tls://` URL asks for TLS for the whole list: the `nats://` URL next to it is
    // connected to with TLS as well.
    #[tokio::test]
    async fn tls_url_uses_tls_for_the_whole_list() {
        let (port, mut canary) = canary_offering_tls().await;

        let result = ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
            .retain_servers_order()
            .connection_timeout(Duration::from_millis(1500))
            .connect(vec![
                format!("nats://127.0.0.1:{port}"),
                "tls://127.0.0.1:1".to_string(),
            ])
            .await;

        assert!(result.is_err());
        let seen = seen_during(&mut canary, Duration::from_millis(200)).await;
        assert_eq!(seen, vec![Seen::TlsHandshake]);
    }

    // TLS asked for at connect covers the servers discovered from the cluster, on the
    // reconnect to them included. The discovered server's INFO does not require TLS; the
    // client still connects to it with TLS and never sends CONNECT in plaintext. The
    // reconnect is forced and steered to the discovered server, so the window does not
    // wait on the client noticing the seed's socket close or on a refused connect to the
    // seed, both slow on Windows.
    #[tokio::test]
    async fn tls_asked_for_at_connect_covers_discovered_servers() {
        let (canary_port, mut canary) = canary_offering_tls().await;
        let seed =
            FakeServer::tls(json!({ "connect_urls": [format!("127.0.0.1:{canary_port}")] })).await;
        let canary_url = format!("nats://127.0.0.1:{canary_port}");

        let client = tls_options()
            .reconnect_delay_callback(|_| Duration::from_millis(50))
            .reconnect_to_server_callback({
                let canary_url = canary_url.clone();
                move |servers, _| {
                    let target = servers
                        .into_iter()
                        .find(|server| server.addr.as_url_str() == canary_url)
                        .map(|server| async_nats::ReconnectToServer {
                            addr: server.addr,
                            delay: None,
                        });
                    async move { target }
                }
            })
            .connection_timeout(Duration::from_secs(2))
            .connect(format!("tls://localhost:{}", seed.port))
            .await
            .unwrap();
        wait_for_server(&client, canary_url).await;

        drop(seed);
        client.force_reconnect().await.unwrap();
        let seen = seen_during(&mut canary, Duration::from_secs(2)).await;
        assert!(
            !seen.is_empty() && seen.iter().all(|seen| *seen == Seen::TlsHandshake),
            "{seen:?}"
        );
    }

    // With tls_optional() the client does not ask for TLS: a CA alone no longer keeps it
    // from a server that has none.
    #[tokio::test]
    async fn tls_optional_connects_without_tls_when_the_server_has_none() {
        let server = nats_server::run_basic_server();

        let client = tls_options()
            .tls_optional()
            .connect(server.client_url())
            .await
            .unwrap();
        client.flush().await.unwrap();
        assert!(!client.server_info().tls_required);
    }

    // With tls_optional() each URL is connected to as its own scheme says: the `nats://`
    // server gets CONNECT in plaintext, the `tls://` server next to it still gets TLS.
    #[tokio::test]
    async fn tls_optional_leaves_tls_to_each_url() {
        let tls_server = nats_server::run_server("tests/configs/tls.conf");
        let (port, mut canary) = canary().await;

        let client = tls_options()
            .add_client_certificate(certs("client-cert.pem"), certs("client-key.pem"))
            .tls_optional()
            .retain_servers_order()
            .connect(vec![
                format!("nats://127.0.0.1:{port}"),
                tls_server.client_url(),
            ])
            .await
            .unwrap();

        assert_eq!(client.server_info().port, tls_server.client_port());
        assert!(client.server_info().tls_required);
        let seen = seen_during(&mut canary, Duration::from_millis(200)).await;
        assert_eq!(seen, vec![Seen::PlaintextConnect]);
    }
}
