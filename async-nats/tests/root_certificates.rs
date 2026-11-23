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

//! Which root certificates the client verifies a server with. With the `native-certs`
//! feature, the platform's roots are the fallback when neither root certificates nor a
//! rustls client config is given. Without it there is no fallback: TLS that a setting or URL
//! asks for is refused at connect, and TLS a server requires fails its attempts; both errors
//! name the feature.
//!
//! rustls-native-certs reads the file in `SSL_CERT_FILE` in place of the platform store, on
//! every platform. The tests point it at the test CA, or at a file that does not exist, to
//! see whether the platform's roots were loaded. The variable is process-wide, so every test
//! holds `ENV` while it runs, and these tests have a binary of their own.
//!
//! The tests that expect an error dial 127.0.0.1, not localhost: with every address of a
//! name failing, the error is the last address's, which depends on the resolver's order.

mod root_certificates {
    use std::path::{Path, PathBuf};
    use std::sync::LazyLock;

    use async_nats::{ConnectErrorKind, ConnectOptions};
    use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    use tokio::sync::{Mutex, MutexGuard};

    static ENV: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn certs(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/configs/certs")
            .join(name)
    }

    /// Holds `ENV` while SSL_CERT_FILE is set; removes it again on drop.
    struct CertFile {
        _guard: MutexGuard<'static, ()>,
    }

    impl Drop for CertFile {
        fn drop(&mut self) {
            std::env::remove_var("SSL_CERT_FILE");
        }
    }

    /// Points rustls-native-certs at `file` in place of the platform store, or with `None`
    /// leaves it the platform store, until the returned guard is dropped. Every test takes
    /// it, so no test sees another test's file.
    async fn ssl_cert_file(file: Option<&Path>) -> CertFile {
        let guard = ENV.lock().await;
        match file {
            Some(file) => std::env::set_var("SSL_CERT_FILE", file),
            None => std::env::remove_var("SSL_CERT_FILE"),
        }
        // A directory would be read next to the file; leave only the file.
        std::env::remove_var("SSL_CERT_DIR");
        CertFile { _guard: guard }
    }

    /// tests/configs/tls.conf requires a client certificate and a user.
    fn client() -> ConnectOptions {
        ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
            .add_client_certificate(certs("client-cert.pem"), certs("client-key.pem"))
    }

    /// A client config that trusts the test CA and presents the client certificate.
    fn client_config() -> async_nats::rustls::ClientConfig {
        let mut roots = async_nats::rustls::RootCertStore::empty();
        roots.add_parsable_certificates(
            CertificateDer::pem_file_iter(certs("rootCA.pem"))
                .unwrap()
                .map(Result::unwrap),
        );
        let cert = CertificateDer::pem_file_iter(certs("client-cert.pem"))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let key = PrivateKeyDer::from_pem_file(certs("client-key.pem")).unwrap();
        async_nats::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(cert, key)
            .unwrap()
    }

    async fn round_trip(client: &async_nats::Client) {
        use futures_util::StreamExt;

        let mut subscriber = client.subscribe("roots").await.unwrap();
        client.publish("roots", "data".into()).await.unwrap();
        assert_eq!(subscriber.next().await.unwrap().payload, "data");
    }

    // Root certificates the user gives are trusted, with or without `native-certs`.
    #[tokio::test]
    async fn root_certificates_are_trusted() {
        let _env = ssl_cert_file(None).await;
        let server = nats_server::run_server("tests/configs/tls.conf");

        let client = client()
            .add_root_certificates(certs("rootCA.pem"))
            .connect(server.client_url())
            .await
            .unwrap();

        round_trip(&client).await;
    }

    // A client config brings its own roots, with or without `native-certs`.
    #[tokio::test]
    async fn client_config_is_trusted() {
        let _env = ssl_cert_file(None).await;
        let server = nats_server::run_server("tests/configs/tls.conf");

        let client = ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
            .tls_client_config(client_config())
            .connect(server.client_url())
            .await
            .unwrap();

        round_trip(&client).await;
    }

    #[cfg(feature = "native-certs")]
    mod native_certs {
        use std::time::Duration;

        use super::*;

        // With no roots given, the platform's roots are trusted: the test CA is reached only
        // through SSL_CERT_FILE.
        #[tokio::test]
        async fn platform_roots_are_trusted_when_no_roots_are_given() {
            let _env = ssl_cert_file(Some(&certs("rootCA.pem"))).await;
            let server = nats_server::run_server("tests/configs/tls.conf");

            let client = client().connect(server.client_url()).await.unwrap();

            round_trip(&client).await;
        }

        // Platform roots that fail to load fail the connect. This is also what shows that the
        // tests below would see the platform's roots being loaded.
        #[tokio::test]
        async fn platform_roots_that_fail_to_load_fail_the_connect() {
            let _env = ssl_cert_file(Some(&certs("does-not-exist.pem"))).await;
            let server = nats_server::run_server("tests/configs/tls.conf");

            let err = client()
                .connect(format!("tls://127.0.0.1:{}", server.client_port()))
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ConnectErrorKind::Tls);
            assert!(
                err.to_string()
                    .starts_with("TLS error: could not load platform certs: "),
                "{err}"
            );
        }

        // A client config does not load the platform's roots, so a broken platform store
        // does not fail it (#1622).
        #[tokio::test]
        async fn client_config_does_not_load_platform_roots() {
            let _env = ssl_cert_file(Some(&certs("does-not-exist.pem"))).await;
            let server = nats_server::run_server("tests/configs/tls.conf");

            let client = ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
                .tls_client_config(client_config())
                .connect(server.client_url())
                .await
                .unwrap();

            round_trip(&client).await;
        }

        // Root certificates the user gives replace the platform's roots.
        #[tokio::test]
        async fn root_certificates_do_not_load_platform_roots() {
            let _env = ssl_cert_file(Some(&certs("does-not-exist.pem"))).await;
            let server = nats_server::run_server("tests/configs/tls.conf");

            let client = client()
                .add_root_certificates(certs("rootCA.pem"))
                .connect(server.client_url())
                .await
                .unwrap();

            round_trip(&client).await;
        }

        // With the platform's roots there to fall back on, TLS asked for without roots is
        // not refused: the client dials, and fails on the closed port.
        #[tokio::test]
        async fn tls_without_roots_is_not_refused() {
            let _env = ssl_cert_file(None).await;

            let err = ConnectOptions::new()
                .require_tls(true)
                .connection_timeout(Duration::from_secs(2))
                .connect("nats://127.0.0.1:1")
                .await
                .unwrap_err();

            assert_ne!(err.kind(), ConnectErrorKind::Configuration, "{err}");
        }
    }

    #[cfg(not(feature = "native-certs"))]
    mod without_native_certs {
        use std::time::Duration;

        use async_nats::SetServerPoolErrorKind;

        use super::*;

        const WAY_OUT: &str = "enable the `native-certs` feature of async-nats for the \
            platform's roots, or pass roots with add_root_certificates() or tls_client_config()";

        fn refused(asked_by: &str) -> String {
            format!(
                "configuration error: {asked_by} asks for TLS, but there are no root \
                 certificates to verify servers with; {WAY_OUT}"
            )
        }

        fn no_roots_for(server: &str) -> String {
            format!(
                "TLS error: {server:?}: no root certificates to verify the server with; \
                 {WAY_OUT}"
            )
        }

        // Every setting and URL that asks for TLS is refused before any attempt when no
        // roots are given. Nothing listens on port 1, so an attempt would end in an IO error.
        #[tokio::test]
        async fn tls_asked_for_without_roots_is_refused_before_any_attempt() {
            let _env = ssl_cert_file(None).await;
            let cases = [
                (
                    "require_tls(true)",
                    ConnectOptions::new().require_tls(true),
                    "nats://127.0.0.1:1",
                ),
                (
                    "tls_first()",
                    ConnectOptions::new().tls_first(),
                    "nats://127.0.0.1:1",
                ),
                (
                    "add_client_certificate()",
                    ConnectOptions::new()
                        .add_client_certificate(certs("client-cert.pem"), certs("client-key.pem")),
                    "nats://127.0.0.1:1",
                ),
                (
                    "tls_server_name()",
                    ConnectOptions::new().tls_server_name("localhost"),
                    "nats://127.0.0.1:1",
                ),
                (
                    "verify_discovered_servers_by_ip()",
                    ConnectOptions::new().verify_discovered_servers_by_ip(),
                    "nats://127.0.0.1:1",
                ),
                (
                    r#"the URL "tls://127.0.0.1:1""#,
                    ConnectOptions::new(),
                    "tls://127.0.0.1:1",
                ),
                #[cfg(feature = "websockets")]
                (
                    r#"the URL "wss://127.0.0.1:1/""#,
                    ConnectOptions::new(),
                    "wss://127.0.0.1:1",
                ),
            ];

            for (asked_by, options, url) in cases {
                let err = options
                    .connection_timeout(Duration::from_secs(2))
                    .connect(url)
                    .await
                    .unwrap_err();

                assert_eq!(err.kind(), ConnectErrorKind::Configuration, "{asked_by}");
                assert_eq!(err.to_string(), refused(asked_by));
            }
        }

        // retry_on_initial_connect does not retry it: connect returns the error, not a
        // client that never connects.
        #[tokio::test]
        async fn retry_on_initial_connect_does_not_retry_it() {
            let _env = ssl_cert_file(None).await;

            let err = tokio::time::timeout(
                Duration::from_secs(5),
                ConnectOptions::new()
                    .require_tls(true)
                    .retry_on_initial_connect()
                    .connect("nats://127.0.0.1:1"),
            )
            .await
            .expect("refused at once, not retried")
            .unwrap_err();

            assert_eq!(err.kind(), ConnectErrorKind::Configuration);
            assert_eq!(err.to_string(), refused("require_tls(true)"));
        }

        // A server that requires TLS, with nothing on the client asking for it, fails its
        // attempt with an error that names the server and the feature.
        #[tokio::test]
        async fn server_requiring_tls_without_roots_names_the_server() {
            let _env = ssl_cert_file(None).await;
            let server = nats_server::run_server("tests/configs/tls.conf");
            let url = format!("nats://127.0.0.1:{}", server.client_port());

            let err = ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
                .connect(&url)
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ConnectErrorKind::Tls);
            assert_eq!(err.to_string(), no_roots_for(&url));
        }

        // With tls_optional() a tls:// URL asks for nothing, so it is not refused at
        // connect; its own TLS fails the attempt the same way.
        #[tokio::test]
        async fn tls_url_with_tls_optional_without_roots_names_the_server() {
            let _env = ssl_cert_file(None).await;
            let server = nats_server::run_server("tests/configs/tls.conf");
            let url = format!("tls://127.0.0.1:{}", server.client_port());

            let err = ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
                .tls_optional()
                .connect(&url)
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ConnectErrorKind::Tls);
            assert_eq!(err.to_string(), no_roots_for(&url));
        }

        // A pool given later that asks for TLS is refused the same way, and the client keeps
        // its pool and its plaintext connection.
        #[tokio::test]
        async fn set_server_pool_asking_for_tls_without_roots_is_refused() {
            let _env = ssl_cert_file(None).await;
            let server = nats_server::run_basic_server();
            let client = async_nats::connect(server.client_url()).await.unwrap();
            let addrs = |pool: Vec<async_nats::Server>| {
                pool.into_iter()
                    .map(|server| server.addr)
                    .collect::<Vec<_>>()
            };
            let pool = addrs(client.server_pool().await.unwrap());

            let err = client
                .set_server_pool(vec!["tls://127.0.0.1:1"])
                .await
                .unwrap_err();

            assert_eq!(err.kind(), SetServerPoolErrorKind::Configuration);
            assert_eq!(
                err.to_string(),
                refused(r#"the URL "tls://127.0.0.1:1" given to set_server_pool()"#)
            );
            assert_eq!(addrs(client.server_pool().await.unwrap()), pool);
            round_trip(&client).await;
        }

        // Without the feature the platform's roots are never read: SSL_CERT_FILE pointing at
        // the test CA does not give the client roots for a server that requires TLS.
        #[tokio::test]
        async fn platform_roots_are_not_read() {
            let _env = ssl_cert_file(Some(&certs("rootCA.pem"))).await;
            let server = nats_server::run_server("tests/configs/tls.conf");
            let url = format!("nats://127.0.0.1:{}", server.client_port());

            let err = ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
                .connect(&url)
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ConnectErrorKind::Tls);
            assert_eq!(err.to_string(), no_roots_for(&url));
        }

        // Without TLS no roots are needed.
        #[tokio::test]
        async fn plaintext_needs_no_roots() {
            let _env = ssl_cert_file(None).await;
            let server = nats_server::run_basic_server();

            let client = async_nats::connect(server.client_url()).await.unwrap();

            round_trip(&client).await;
        }
    }
}
