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

    use async_nats::{ConnectErrorKind, ConnectOptions};
    #[cfg(feature = "websockets")]
    use async_nats::{ServerAddr, SetServerPoolErrorKind};
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
        let path = std::env::temp_dir().join(format!("tls-options-{}.conf", rand::random::<u64>()));
        std::fs::write(
            &path,
            contents.replace("{certs}", dir.trim_end_matches('/')),
        )
        .unwrap();
        path.display().to_string()
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
    /// to TLS, and answers every PING with PONG. Like nats-server 2.12 and later, it sends
    /// the INFO again after the first PONG. Dropping it closes the listener and every
    /// connection.
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
        let info = info_line(&server_info(port, info));
        stream.write_all(&info).await?;
        let mut stream = acceptor().accept(stream).await?;

        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let mut info_after_pong = Some(info);
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(6).any(|w| w == b"PING\r\n") {
                buf.clear();
                stream.write_all(b"PONG\r\n").await?;
                if let Some(info) = info_after_pong.take() {
                    stream.write_all(&info).await?;
                }
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

    /// Sends every reconnect to `url` once the pool holds it, after a short delay.
    fn steered_to(options: ConnectOptions, url: &str) -> ConnectOptions {
        let url = url.to_string();
        options
            .reconnect_delay_callback(|_| Duration::from_millis(50))
            .reconnect_to_server_callback(move |servers, _| {
                let target = servers
                    .into_iter()
                    .find(|server| server.addr.as_url_str() == url)
                    .map(|server| async_nats::ReconnectToServer {
                        addr: server.addr,
                        delay: None,
                    });
                async move { target }
            })
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
        // The pool lists the canary, learned over TLS, as tls://.
        let canary_url = format!("tls://127.0.0.1:{canary_port}");

        let client = steered_to(tls_options(), &canary_url)
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

    // tls_optional() set next to a setting that asks for TLS is refused at connect, before
    // any attempt: retry_on_initial_connect() does not retry it.
    #[tokio::test]
    async fn tls_optional_with_require_tls_is_a_configuration_error() {
        let err = ConnectOptions::new()
            .tls_optional()
            .require_tls(true)
            .retry_on_initial_connect()
            .connect("nats://127.0.0.1:1")
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ConnectErrorKind::Configuration);
        let message = err.to_string();
        assert!(
            message.contains("tls_optional()") && message.contains("require_tls(true)"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn tls_optional_with_tls_first_is_a_configuration_error() {
        let err = ConnectOptions::new()
            .tls_optional()
            .tls_first()
            .retry_on_initial_connect()
            .connect("nats://127.0.0.1:1")
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ConnectErrorKind::Configuration);
        let message = err.to_string();
        assert!(
            message.contains("tls_optional()") && message.contains("tls_first()"),
            "{message}"
        );
    }

    // A websocket URL in a build without the feature is refused at connect instead of
    // being dialed as plain TCP.
    #[cfg(not(feature = "websockets"))]
    #[tokio::test]
    async fn websocket_url_without_the_feature_is_a_configuration_error() {
        for url in ["ws://127.0.0.1:1", "wss://127.0.0.1:1"] {
            let err = ConnectOptions::new()
                .retry_on_initial_connect()
                .connect(url)
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ConnectErrorKind::Configuration, "{url}");
            assert!(err.to_string().contains("websockets"), "{err}");
        }
    }

    /// Connects `options` to a ws:// URL with user info, `more` URLs after it, and returns
    /// the configuration error. It names the URL without its user info, the setting that
    /// asked for TLS and the wss:// form; the canary on the ws:// port sees nothing, also
    /// with retry_on_initial_connect().
    #[cfg(feature = "websockets")]
    async fn ws_url_is_refused(options: ConnectOptions, more: Vec<String>, asked_by: &str) {
        let (port, mut canary) = canary().await;
        let mut urls = vec![format!("ws://derek:porkchop@127.0.0.1:{port}")];
        urls.extend(more);

        let err = options
            .retain_servers_order()
            .retry_on_initial_connect()
            .connect(urls)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ConnectErrorKind::Configuration, "{asked_by}");
        let message = err.to_string();
        assert!(
            message.contains(&format!("\"ws://127.0.0.1:{port}/\""))
                && !message.contains("derek")
                && !message.contains("porkchop")
                && message.contains(asked_by)
                && message.contains("wss://"),
            "{message}"
        );
        let seen = seen_during(&mut canary, Duration::from_millis(500)).await;
        assert!(seen.is_empty(), "{asked_by}: {seen:?}");
    }

    // A ws:// URL in a client that asks for TLS is refused at connect, before any attempt:
    // the websocket has no upgrade, so the URL is neither dialed in plaintext nor with TLS.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn ws_url_under_require_tls_is_a_configuration_error() {
        ws_url_is_refused(
            ConnectOptions::new().require_tls(true),
            vec![],
            "require_tls(true)",
        )
        .await;
    }

    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn ws_url_under_root_certificates_is_a_configuration_error() {
        ws_url_is_refused(
            ConnectOptions::new().add_root_certificates(certs("rootCA.pem")),
            vec![],
            "add_root_certificates()",
        )
        .await;
    }

    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn ws_url_under_tls_first_is_a_configuration_error() {
        ws_url_is_refused(ConnectOptions::new().tls_first(), vec![], "tls_first()").await;
    }

    // A wss:// URL asks for TLS for the whole list, so a ws:// URL next to it is refused
    // too. The error names that URL without its user info.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn ws_url_next_to_a_wss_url_is_a_configuration_error() {
        ws_url_is_refused(
            ConnectOptions::new(),
            vec!["wss://derek:porkchop@127.0.0.1:1".to_string()],
            "the URL \"wss://127.0.0.1:1/\" asks for TLS",
        )
        .await;
    }

    // With tls_optional() each websocket URL keeps its own scheme, so ws:// and wss://
    // URLs can share a list: the ws:// server gets a plaintext websocket upgrade, the
    // wss:// server next to it gets TLS.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn tls_optional_keeps_each_websocket_url_its_own_scheme() {
        let ws_port = free_port();
        let server = nats_server::run_server(&config(&format!(
            "websocket {{ port: {ws_port}, tls {{ cert_file: \"{{certs}}/server-cert.pem\", \
             key_file: \"{{certs}}/server-key.pem\" }} }}"
        )));
        let _ = server.client_url();
        let (port, mut canary) = canary().await;

        let client = tls_options()
            .tls_optional()
            .retain_servers_order()
            .connect(vec![
                format!("ws://127.0.0.1:{port}"),
                format!("wss://localhost:{ws_port}"),
            ])
            .await
            .unwrap();
        // The canary closes after the first bytes, so the live connection is to the
        // wss:// server.
        client.flush().await.unwrap();
        let seen = seen_during(&mut canary, Duration::from_millis(200)).await;
        assert_eq!(seen, vec![Seen::PlaintextWebsocket]);
    }

    // A pool given after connect is checked as the list at connect was: the client asks for
    // TLS, so a ws:// URL fails the call that sets the pool, which names the URL without its
    // user info, the setting and the wss:// form, and leaves the pool as it was.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn set_server_pool_refuses_a_ws_url_when_tls_is_asked_for() {
        let server = FakeServer::tls(json!({})).await;
        let client = tls_options()
            .require_tls(true)
            .connect(format!("nats://localhost:{}", server.port))
            .await
            .unwrap();
        let before = pool_urls(&client).await;
        let port = 1;

        let err = client
            .set_server_pool(vec![format!("ws://derek:porkchop@127.0.0.1:{port}")])
            .await
            .unwrap_err();

        assert_eq!(err.kind(), SetServerPoolErrorKind::Configuration);
        let message = err.to_string();
        assert!(
            message.contains(&format!("ws://127.0.0.1:{port}"))
                && !message.contains("derek")
                && !message.contains("porkchop")
                && message.contains("require_tls(true)")
                && message.contains("wss://"),
            "{message}"
        );
        assert_eq!(pool_urls(&client).await, before);
    }

    // Under tls_optional() nothing asked for TLS at connect, so the same pool is taken.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn set_server_pool_takes_a_ws_url_under_tls_optional() {
        let server = FakeServer::tls(json!({})).await;
        let client = tls_options()
            .tls_optional()
            .connect(format!("nats://localhost:{}", server.port))
            .await
            .unwrap();
        let url: ServerAddr = "ws://127.0.0.1:1".parse().unwrap();

        client.set_server_pool(vec![url.clone()]).await.unwrap();

        let pool = client.server_pool().await.unwrap();
        assert_eq!(
            pool.iter().map(|server| &server.addr).collect::<Vec<_>>(),
            vec![&url]
        );
    }

    // A tls:// URL in a pool given after connect asks for TLS, as it does at connect. The
    // current connection stays up; from the next attempt on, the nats:// URL next to it is
    // connected to with TLS too.
    #[tokio::test]
    async fn set_server_pool_with_a_tls_url_uses_tls_for_the_whole_pool() {
        let server = nats_server::run_basic_server();
        let (port, mut canary) = canary_offering_tls().await;
        let canary_url = format!("nats://127.0.0.1:{port}");
        let client = steered_to(ConnectOptions::new(), &canary_url)
            .connect(server.client_url())
            .await
            .unwrap();

        client
            .set_server_pool(vec!["tls://127.0.0.1:1".to_string(), canary_url])
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), client.flush())
            .await
            .unwrap()
            .unwrap();
        client.force_reconnect().await.unwrap();
        let seen = seen_during(&mut canary, Duration::from_secs(2)).await;
        assert!(
            !seen.is_empty() && seen.iter().all(|seen| *seen == Seen::TlsHandshake),
            "{seen:?}"
        );
    }

    // Once a pool has asked for TLS, a later pool without a tls:// URL does not turn it off.
    #[tokio::test]
    async fn set_server_pool_without_a_tls_url_keeps_tls() {
        let server = nats_server::run_basic_server();
        let (port, mut canary) = canary_offering_tls().await;
        let canary_url = format!("nats://127.0.0.1:{port}");
        let client = steered_to(ConnectOptions::new(), &canary_url)
            .connect(server.client_url())
            .await
            .unwrap();

        client
            .set_server_pool(vec!["tls://127.0.0.1:1"])
            .await
            .unwrap();
        client.set_server_pool(vec![canary_url]).await.unwrap();

        client.force_reconnect().await.unwrap();
        let seen = seen_during(&mut canary, Duration::from_secs(2)).await;
        assert!(
            !seen.is_empty() && seen.iter().all(|seen| *seen == Seen::TlsHandshake),
            "{seen:?}"
        );
    }

    // With tls_optional() a tls:// URL in the pool asks for nothing: the nats:// URL next to
    // it gets CONNECT in plaintext.
    #[tokio::test]
    async fn set_server_pool_under_tls_optional_leaves_tls_to_each_url() {
        let server = nats_server::run_basic_server();
        let (port, mut canary) = canary_offering_tls().await;
        let canary_url = format!("nats://127.0.0.1:{port}");
        let client = steered_to(ConnectOptions::new().tls_optional(), &canary_url)
            .connect(server.client_url())
            .await
            .unwrap();

        client
            .set_server_pool(vec!["tls://127.0.0.1:1".to_string(), canary_url])
            .await
            .unwrap();

        client.force_reconnect().await.unwrap();
        let seen = seen_during(&mut canary, Duration::from_secs(2)).await;
        assert!(
            !seen.is_empty() && seen.iter().all(|seen| *seen == Seen::PlaintextConnect),
            "{seen:?}"
        );
    }

    // A refused pool changes nothing. The wss:// URL would ask for TLS, so the ws:// URL next
    // to it fails the call, which names that URL of the pool, without its user info, as what
    // asked; the client still does not ask for TLS, and the nats:// URL of a later pool gets
    // CONNECT in plaintext.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn set_server_pool_refused_for_a_ws_url_leaves_tls_as_it_was() {
        let server = nats_server::run_basic_server();
        let (port, mut canary) = canary_offering_tls().await;
        let canary_url = format!("nats://127.0.0.1:{port}");
        let client = steered_to(ConnectOptions::new(), &canary_url)
            .connect(server.client_url())
            .await
            .unwrap();
        let before = pool_urls(&client).await;

        let err = client
            .set_server_pool(vec!["wss://derek:porkchop@127.0.0.1:1", "ws://127.0.0.1:2"])
            .await
            .unwrap_err();

        assert_eq!(err.kind(), SetServerPoolErrorKind::Configuration);
        let message = err.to_string();
        assert!(
            message.contains(
                r#"the URL "wss://127.0.0.1:1/" given to set_server_pool() asks for TLS, but "ws://127.0.0.1:2/" is a ws:// URL; wss:// is the TLS form"#
            ) && !message.contains("porkchop"),
            "{message}"
        );
        assert_eq!(pool_urls(&client).await, before);

        client.set_server_pool(vec![canary_url]).await.unwrap();
        client.force_reconnect().await.unwrap();
        let seen = seen_during(&mut canary, Duration::from_secs(2)).await;
        assert!(
            !seen.is_empty() && seen.iter().all(|seen| *seen == Seen::PlaintextConnect),
            "{seen:?}"
        );
    }

    // With tls_optional() a ws:// URL is a plaintext websocket, CA or not.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn tls_optional_leaves_a_ws_url_in_plaintext() {
        let (port, mut canary) = canary().await;

        let result = tls_options()
            .tls_optional()
            .connection_timeout(Duration::from_millis(1500))
            .connect(format!("ws://127.0.0.1:{port}"))
            .await;

        assert!(result.is_err());
        let seen = seen_during(&mut canary, Duration::from_millis(200)).await;
        assert_eq!(seen, vec![Seen::PlaintextWebsocket]);
    }

    // Every address the host resolves to is tried for a websocket URL too. `localhost`
    // usually resolves to `::1` first, and this server listens on IPv4 only.
    #[cfg(feature = "websockets")]
    #[tokio::test]
    async fn websocket_tries_every_resolved_address() {
        let ws_port = free_port();
        let server = nats_server::run_server(&config(&format!(
            "websocket {{ host: 127.0.0.1, port: {ws_port}, no_tls: true }}"
        )));
        let _ = server.client_url();

        let client = ConnectOptions::new()
            .connect(format!("ws://localhost:{ws_port}"))
            .await
            .unwrap();
        client.flush().await.unwrap();
    }

    // Against a server whose INFO neither requires nor offers TLS, the error names the
    // server and the setting or URL that asked for TLS and, unless the server's own URL did,
    // tls_optional() as the way out. CONNECT never goes out in plaintext.
    #[tokio::test]
    async fn server_without_tls_names_the_setting_that_asked_for_it() {
        const WAY_OUT: &str = "tls_optional() leaves TLS to each server";
        for (scheme, options, expected) in [
            (
                "nats",
                ConnectOptions::new().require_tls(true),
                format!(
                    "require_tls(true) asks for TLS, but {{server}} does not offer it; {WAY_OUT}"
                ),
            ),
            (
                "nats",
                ConnectOptions::new().add_root_certificates(certs("rootCA.pem")),
                format!(
                    "add_root_certificates() asks for TLS, but {{server}} does not offer it; \
                     {WAY_OUT}"
                ),
            ),
            (
                "nats",
                ConnectOptions::new()
                    .add_client_certificate(certs("client-cert.pem"), certs("client-key.pem")),
                format!(
                    "add_client_certificate() asks for TLS, but {{server}} does not offer it; \
                     {WAY_OUT}"
                ),
            ),
            (
                "nats",
                ConnectOptions::new().tls_client_config(client_config()),
                format!(
                    "tls_client_config() asks for TLS, but {{server}} does not offer it; {WAY_OUT}"
                ),
            ),
            (
                "tls",
                ConnectOptions::new(),
                "the URL {server} asks for TLS, but {server} does not offer it".to_string(),
            ),
            (
                "tls",
                ConnectOptions::new().tls_optional(),
                "the URL {server} asks for TLS, but {server} does not offer it".to_string(),
            ),
        ] {
            let (port, mut canary) = canary().await;
            let url = format!("{scheme}://127.0.0.1:{port}");
            let expected = expected.replace("{server}", &format!("{url:?}"));

            let err = options
                .user_and_password("derek".into(), "porkchop".into())
                .connection_timeout(Duration::from_millis(1500))
                .connect(url)
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ConnectErrorKind::Tls, "{expected}");
            assert_eq!(err.to_string(), format!("TLS error: {expected}"));
            let seen = seen_during(&mut canary, Duration::from_millis(200)).await;
            assert!(
                !seen.contains(&Seen::PlaintextConnect),
                "{expected}: {seen:?}"
            );
        }
    }

    // In a list that mixes tls:// and nats:// URLs, the error for the nats:// server
    // without TLS names that server, the URL that asked for TLS and the way out, both URLs
    // without their user info.
    #[tokio::test]
    async fn mixed_list_names_the_server_the_tls_url_and_the_way_out() {
        let (port, _canary) = canary().await;

        let err = ConnectOptions::new()
            .retain_servers_order()
            .connection_timeout(Duration::from_millis(1500))
            .connect(vec![
                "tls://derek:porkchop@127.0.0.1:1".to_string(),
                format!("nats://derek:porkchop@127.0.0.1:{port}"),
            ])
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ConnectErrorKind::Tls);
        assert_eq!(
            err.to_string(),
            format!(
                "TLS error: the URL \"tls://127.0.0.1:1\" asks for TLS, but \
                 \"nats://127.0.0.1:{port}\" does not offer it; tls_optional() leaves TLS to \
                 each server"
            )
        );
    }

    // A TLS error of a failed attempt that the client retries reports its text in the
    // event, not only its kind. The event carries the error of the last server tried in a
    // pass, so for this one-server list, under retry_on_initial_connect() and on reconnect
    // alike, it names the setting that asked for TLS and the way out.
    #[tokio::test]
    async fn failed_attempt_event_names_the_setting_and_the_way_out() {
        let (port, _canary) = canary().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _client = ConnectOptions::new()
            .add_root_certificates(certs("rootCA.pem"))
            .retry_on_initial_connect()
            .reconnect_delay_callback(|_| Duration::from_millis(50))
            .event_callback(move |event| {
                let tx = tx.clone();
                async move {
                    if let async_nats::Event::ClientError(error) = event {
                        tx.send(error.to_string()).ok();
                    }
                }
            })
            .connect(format!("nats://127.0.0.1:{port}"))
            .await
            .unwrap();

        let text = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            text,
            format!(
                "nats: TLS error: add_root_certificates() asks for TLS, but \
                 \"nats://127.0.0.1:{port}\" does not offer it; tls_optional() leaves TLS to \
                 each server"
            )
        );
    }
}
