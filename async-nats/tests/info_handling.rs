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

//! Which INFOs the client takes servers and server info from: the INFO read before a TLS
//! handshake, the INFO that follows the first PONG, and the INFOs of an established
//! connection. Scripted servers say what each INFO holds and when it comes.

mod info_handling {
    use std::{path::PathBuf, sync::Arc, time::Duration};

    use async_nats::{ConnectErrorKind, ConnectOptions};
    use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    use serde_json::{json, Value};
    use tokio::{
        io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };
    use tokio_util::sync::CancellationToken;

    fn certs(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/configs/certs")
            .join(name)
    }

    /// Presents the test CA's certificate for `localhost`.
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

    fn info_line(port: u16, extra: &Value) -> Vec<u8> {
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
        format!("INFO {info}\r\n").into_bytes()
    }

    trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
    impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

    /// How a scripted server uses TLS.
    #[derive(Clone, Copy)]
    enum Tls {
        None,
        /// It requires TLS and upgrades after its INFO.
        AfterInfo,
        /// It starts with the TLS handshake (`handshake_first`) and sends INFO inside TLS.
        First,
    }

    /// What a scripted server does when the first PING arrives, in order.
    #[derive(Clone)]
    enum Step {
        /// Writes this line.
        Line(&'static str),
        /// Writes an INFO with these fields merged in.
        Info(Value),
        /// Reads until the client sends PONG.
        AwaitPong,
        Pause(Duration),
        Close,
    }

    /// A scripted server on 127.0.0.1. It sends INFO with `info` merged in, with
    /// `tls_required` when it has TLS, answers the first PING with `script` and every later
    /// PING with PONG. Dropping it closes the listener and every connection.
    struct Server {
        port: u16,
        /// Gets one message for each connection that ran its script to the end.
        scripts_run: mpsc::UnboundedReceiver<()>,
        stop: CancellationToken,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.cancel();
        }
    }

    impl Server {
        async fn start(tls: Tls, info: Value, script: Vec<Step>) -> Server {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (script_run, scripts_run) = mpsc::unbounded_channel();
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
                    let (info, script) = (info.clone(), script.clone());
                    let (script_run, token) = (script_run.clone(), token.clone());
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = token.cancelled() => {}
                            _ = serve(stream, port, tls, info, script, script_run) => {}
                        }
                    });
                }
            });
            Server {
                port,
                scripts_run,
                stop,
            }
        }

        fn url(&self) -> String {
            format!("nats://localhost:{}", self.port)
        }

        /// Whether a connection ran the script to the end within ten seconds.
        async fn ran_script(&mut self) -> bool {
            tokio::time::timeout(Duration::from_secs(10), self.scripts_run.recv())
                .await
                .is_ok_and(|run| run.is_some())
        }
    }

    async fn serve(
        mut stream: TcpStream,
        port: u16,
        tls: Tls,
        mut info: Value,
        script: Vec<Step>,
        script_run: mpsc::UnboundedSender<()>,
    ) -> std::io::Result<()> {
        if !matches!(tls, Tls::None) {
            info["tls_required"] = json!(true);
        }
        let info = info_line(port, &info);
        let stream: Box<dyn Stream> = match tls {
            Tls::None => {
                stream.write_all(&info).await?;
                Box::new(stream)
            }
            Tls::AfterInfo => {
                stream.write_all(&info).await?;
                Box::new(acceptor().accept(stream).await?)
            }
            Tls::First => {
                let mut stream = acceptor().accept(stream).await?;
                stream.write_all(&info).await?;
                Box::new(stream)
            }
        };
        let mut stream = BufReader::new(stream);
        let mut script = Some(script);
        let mut line = Vec::new();
        loop {
            line.clear();
            if stream.read_until(b'\n', &mut line).await? == 0 {
                return Ok(());
            }
            if line != b"PING\r\n" {
                continue;
            }
            let Some(steps) = script.take() else {
                stream.get_mut().write_all(b"PONG\r\n").await?;
                continue;
            };
            for step in steps {
                match step {
                    Step::Line(text) => {
                        let text = format!("{text}\r\n");
                        stream.get_mut().write_all(text.as_bytes()).await?;
                    }
                    Step::Info(extra) => {
                        stream.get_mut().write_all(&info_line(port, &extra)).await?;
                    }
                    Step::AwaitPong => loop {
                        line.clear();
                        if stream.read_until(b'\n', &mut line).await? == 0 {
                            return Ok(());
                        }
                        if line == b"PONG\r\n" {
                            break;
                        }
                    },
                    Step::Pause(duration) => tokio::time::sleep(duration).await,
                    Step::Close => return Ok(()),
                }
            }
            script_run.send(()).ok();
        }
    }

    /// Options that trust the test CA, which issued the `localhost` certificate. They ask
    /// for TLS.
    fn test_ca() -> ConnectOptions {
        ConnectOptions::new().add_root_certificates(certs("rootCA.pem"))
    }

    /// An INFO that lists servers on 127.0.0.1 with `ports`. No test dials them.
    fn listing(ports: &[u16]) -> Value {
        let urls: Vec<String> = ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect();
        json!({ "connect_urls": urls })
    }

    /// The pool entry of a server that `listing` lists on a connection with TLS, as
    /// `server_pool()` lists it.
    fn listed(port: u16) -> String {
        format!("tls://127.0.0.1:{port}")
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

    /// After the first PONG, the server waits before it sends this step, so a client that
    /// does not wait for it has reported connected and answers queries without it.
    const LATER: Step = Step::Pause(Duration::from_millis(500));

    // On a connection that starts TLS after INFO, the client waits after the first PONG
    // for the INFO that nats-server 2.12 and later sends there, and takes the servers it
    // lists before it reports connected.
    #[tokio::test]
    async fn waits_for_the_info_after_the_first_pong() {
        let server = Server::start(
            Tls::AfterInfo,
            json!({}),
            vec![Step::Line("PONG"), LATER, Step::Info(listing(&[4001]))],
        )
        .await;

        let client = test_ca().connect(server.url()).await.unwrap();

        assert_eq!(pool_urls(&client).await, [server.url(), listed(4001)]);
    }

    // Before the first PONG, the client answers a PING: the server here sends its PONG only
    // after that answer. A connection without TLS, so that no wait follows the PONG.
    #[tokio::test]
    async fn answers_ping_before_the_first_pong() {
        let mut server = Server::start(
            Tls::None,
            json!({}),
            vec![Step::Line("PING"), Step::AwaitPong, Step::Line("PONG")],
        )
        .await;

        let _client = ConnectOptions::new().connect(server.url()).await.unwrap();

        assert!(server.ran_script().await);
    }

    // Before the first PONG, the client skips +OK: it is not the PONG, and an error after it
    // fails the attempt.
    #[tokio::test]
    async fn skips_ok_before_the_first_pong() {
        let server = Server::start(
            Tls::None,
            json!({}),
            vec![
                Step::Line("+OK"),
                Step::Line("-ERR 'Authorization Violation'"),
            ],
        )
        .await;

        let err = ConnectOptions::new()
            .connect(server.url())
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ConnectErrorKind::AuthorizationViolation);
    }

    // So does it while it waits for the INFO that follows the first PONG.
    #[tokio::test]
    async fn answers_ping_and_skips_ok_while_it_waits_for_the_info() {
        let server = Server::start(
            Tls::AfterInfo,
            json!({}),
            vec![
                Step::Line("PONG"),
                Step::Line("+OK"),
                Step::Line("PING"),
                Step::AwaitPong,
                LATER,
                Step::Info(listing(&[4001])),
            ],
        )
        .await;

        let client = test_ca().connect(server.url()).await.unwrap();

        assert_eq!(pool_urls(&client).await, [server.url(), listed(4001)]);
    }

    // An error from the server during the wait fails the attempt with the server's error.
    #[tokio::test]
    async fn fails_the_attempt_on_an_error_while_it_waits() {
        let server = Server::start(
            Tls::AfterInfo,
            json!({}),
            vec![
                Step::Line("PONG"),
                Step::Line("-ERR 'Authorization Violation'"),
            ],
        )
        .await;

        let err = test_ca().connect(server.url()).await.unwrap_err();

        assert_eq!(err.kind(), ConnectErrorKind::AuthorizationViolation);
    }

    // A line that does not parse during the wait fails the attempt.
    #[tokio::test]
    async fn fails_the_attempt_on_a_line_that_does_not_parse_while_it_waits() {
        let server = Server::start(
            Tls::AfterInfo,
            json!({}),
            vec![Step::Line("PONG"), Step::Line("NOT A PROTOCOL LINE")],
        )
        .await;

        let err = test_ca().connect(server.url()).await.unwrap_err();

        assert_eq!(err.kind(), ConnectErrorKind::Io);
    }

    // A connection lost during the wait fails the attempt.
    #[tokio::test]
    async fn fails_the_attempt_when_the_connection_closes_while_it_waits() {
        let server = Server::start(
            Tls::AfterInfo,
            json!({}),
            vec![Step::Line("PONG"), Step::Close],
        )
        .await;

        let err = test_ca().connect(server.url()).await.unwrap_err();

        assert_eq!(err.kind(), ConnectErrorKind::Io);
    }

    // The INFO that follows the first PONG ends the wait also when it lists no servers, as
    // that of a server without a cluster does.
    #[tokio::test]
    async fn an_info_without_servers_ends_the_wait() {
        let server = Server::start(
            Tls::AfterInfo,
            json!({}),
            vec![Step::Line("PONG"), Step::Info(json!({}))],
        )
        .await;

        let client = tokio::time::timeout(
            Duration::from_secs(10),
            test_ca()
                .connection_timeout(Duration::from_secs(30))
                .connect(server.url()),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(pool_urls(&client).await, [server.url()]);
    }

    /// Whether the client reports lame duck mode when the INFO that follows the first PONG
    /// says so.
    async fn reports_lame_duck_mode(tls: Tls, options: ConnectOptions) -> bool {
        let server = Server::start(
            tls,
            json!({}),
            vec![Step::Line("PONG"), Step::Info(json!({ "ldm": true }))],
        )
        .await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _client = options
            .event_callback(move |event| {
                let tx = tx.clone();
                async move {
                    if matches!(event, async_nats::Event::LameDuckMode) {
                        tx.send(()).ok();
                    }
                }
            })
            .connect(server.url())
            .await
            .unwrap();
        matches!(
            tokio::time::timeout(Duration::from_secs(2), rx.recv()).await,
            Ok(Some(()))
        )
    }

    // The INFO that ends the wait is read for lame duck mode, as every later INFO is. The
    // event can come before the connected event.
    #[tokio::test]
    async fn reports_lame_duck_mode_from_the_info_that_ends_the_wait() {
        assert!(
            reports_lame_duck_mode(Tls::None, ConnectOptions::new()).await,
            "plaintext"
        );
        assert!(
            reports_lame_duck_mode(Tls::AfterInfo, test_ca()).await,
            "TLS after INFO"
        );
    }

    // When no INFO follows the first PONG before the connection timeout, the client reports
    // connected without it.
    #[tokio::test]
    async fn reports_connected_when_no_info_follows_the_first_pong() {
        let server = Server::start(Tls::AfterInfo, json!({}), vec![Step::Line("PONG")]).await;

        let client = tokio::time::timeout(
            Duration::from_secs(5),
            test_ca()
                .connection_timeout(Duration::from_secs(1))
                .connect(server.url()),
        )
        .await
        .expect("did not report connected after the wait")
        .unwrap();

        client.flush().await.unwrap();
    }

    // The client waits only on a connection that starts TLS after INFO, from a server whose
    // plaintext INFO gives version 2.12.0 or later, and not under ignore_discovered_servers().
    // Elsewhere it reports connected at the first PONG. No INFO follows the first PONG here,
    // so a wait would last the connection timeout. The version is read before TLS: a changed
    // version costs at most one wait, or leaves the INFO to be taken when it arrives.
    #[tokio::test]
    async fn reports_connected_at_the_first_pong_where_it_does_not_wait() {
        let mut cases = Vec::new();
        for version in [
            "2.11.0",
            "2",
            "",
            "2.11.0-beta.1",
            "99999999999999999999.0.0",
        ] {
            cases.push((
                Tls::AfterInfo,
                json!({ "version": version }),
                test_ca(),
                format!("version {version:?}"),
            ));
        }
        cases.push((
            Tls::AfterInfo,
            json!({}),
            test_ca().ignore_discovered_servers(),
            "ignore_discovered_servers()".to_string(),
        ));
        cases.push((
            Tls::None,
            json!({}),
            ConnectOptions::new(),
            "plaintext".to_string(),
        ));
        cases.push((
            Tls::First,
            json!({}),
            test_ca().tls_first(),
            "tls_first()".to_string(),
        ));

        for (tls, info, options, case) in cases {
            let server = Server::start(tls, info, vec![Step::Line("PONG")]).await;
            let connected = tokio::time::timeout(
                Duration::from_secs(10),
                options
                    .connection_timeout(Duration::from_secs(30))
                    .connect(server.url()),
            )
            .await;
            assert!(matches!(connected, Ok(Ok(_))), "{case}");
        }
    }

    // On a connection that starts TLS after INFO, the servers of the INFO read before the
    // handshake are not used.
    #[tokio::test]
    async fn does_not_use_the_servers_of_the_info_read_before_tls() {
        let server = Server::start(
            Tls::AfterInfo,
            listing(&[4001]),
            vec![Step::Line("PONG"), Step::Info(json!({}))],
        )
        .await;

        let client = test_ca().connect(server.url()).await.unwrap();

        assert_eq!(pool_urls(&client).await, [server.url()]);
    }

    // Nor when the attempt fails: after a failed handshake, here a certificate not issued
    // for 127.0.0.1, or after a handshake that succeeded, when the server refuses the
    // client. The client then connects to the third server.
    #[tokio::test]
    async fn does_not_use_the_servers_of_the_info_read_before_tls_when_the_attempt_fails() {
        let untrusted = Server::start(Tls::AfterInfo, listing(&[4001]), vec![]).await;
        let refusing = Server::start(
            Tls::AfterInfo,
            listing(&[4002]),
            vec![
                Step::Line("-ERR 'maximum account active connections exceeded'"),
                Step::Close,
            ],
        )
        .await;
        let accepting = Server::start(
            Tls::AfterInfo,
            json!({}),
            vec![Step::Line("PONG"), Step::Info(json!({}))],
        )
        .await;
        let urls = [
            format!("nats://127.0.0.1:{}", untrusted.port),
            refusing.url(),
            accepting.url(),
        ];

        let client = test_ca()
            .retain_servers_order()
            .connect(urls.as_slice())
            .await
            .unwrap();

        assert_eq!(pool_urls(&client).await, urls);
        let pool = client.server_pool().await.unwrap();
        let errors: Vec<_> = pool
            .iter()
            .map(|server| server.last_error.clone())
            .collect();
        assert!(
            errors[0]
                .as_ref()
                .is_some_and(|error| error.contains("certificate")),
            "{errors:?}"
        );
        assert!(
            errors[1]
                .as_ref()
                .is_some_and(|error| error.contains("maximum account active connections exceeded")),
            "{errors:?}"
        );
    }

    // Without TLS, and with TLS first, the client uses the servers of the first INFO, also
    // when the server then refuses it: a configured server that refuses the client still
    // leads to its peers. A peer listed without TLS is listed as nats://.
    #[tokio::test]
    async fn uses_the_servers_of_the_first_info_of_a_refused_attempt_without_tls_after_info() {
        for (tls, options, peer) in [
            (
                Tls::None,
                ConnectOptions::new(),
                "nats://127.0.0.1:4001".to_string(),
            ),
            (Tls::First, test_ca().tls_first(), listed(4001)),
        ] {
            let refusing = Server::start(
                tls,
                listing(&[4001]),
                vec![
                    Step::Line("-ERR 'maximum account active connections exceeded'"),
                    Step::Close,
                ],
            )
            .await;
            let accepting = Server::start(tls, json!({}), vec![Step::Line("PONG")]).await;

            let client = options
                .retain_servers_order()
                .connect([refusing.url(), accepting.url()].as_slice())
                .await
                .unwrap();

            assert_eq!(
                pool_urls(&client).await,
                [refusing.url(), accepting.url(), peer]
            );
        }
    }
}
