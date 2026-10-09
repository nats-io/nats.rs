// Copyright 2020-2022 The NATS Authors
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

mod client {
    use std::{path::PathBuf, time::Duration};

    use futures_util::StreamExt;

    #[tokio::test]
    async fn basic_tls() {
        let server = nats_server::run_server("tests/configs/tls.conf");

        // Should fail without certs.
        assert!(async_nats::connect(&server.client_url()).await.is_err());

        // Should fail with IP (cert doesn't have proper SAN entry)
        assert!(
            async_nats::connect(format!("tls://127.0.0.1:{}", server.client_port()))
                .await
                .is_err()
        );

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        async_nats::ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
            .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
            .add_client_certificate(
                path.join("tests/configs/certs/client-cert.pem"),
                path.join("tests/configs/certs/client-key.pem"),
            )
            .require_tls(true)
            .connect(server.client_url())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ip_basic_tls() {
        let server = nats_server::run_server("tests/configs/ip-tls.conf");

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        async_nats::ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
            .add_root_certificates(path.join("tests/configs/certs/ip-ca.pem"))
            .require_tls(true)
            .connect(format!("tls://127.0.0.1:{}", server.client_port()))
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn unknown_server_ca() {
        let server = nats_server::run_server("tests/configs/tls.conf");
        assert!(async_nats::connect(&server.client_url()).await.is_err());

        async_nats::ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
            .require_tls(true)
            .connect(server.client_url())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn basic_tls_all_certs_one_file() {
        let s = nats_server::run_server("tests/configs/tls.conf");

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        // test scenario where rootCA, client certificate and client key are all in one .pem file
        tokio::time::timeout(
            tokio::time::Duration::from_secs(10),
            async_nats::ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
                .add_root_certificates(path.join("tests/configs/certs/client-all.pem"))
                .add_client_certificate(
                    path.join("tests/configs/certs/client-all.pem"),
                    path.join("tests/configs/certs/client-all.pem"),
                )
                .require_tls(true)
                .connect(&s.client_url()),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn custom_tls_client() {
        let mut root_store = async_nats::rustls::RootCertStore::empty();

        root_store.add_parsable_certificates(rustls_native_certs::load_native_certs().unwrap());

        let tls_client = async_nats::rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let client = async_nats::ConnectOptions::new()
            .require_tls(true)
            .tls_client_config(tls_client)
            .connect("demo.nats.io")
            .await
            .unwrap();

        let mut subscribe = client.subscribe("subject").await.unwrap();
        client.publish("subject", "data".into()).await.unwrap();
        assert!(subscribe.next().await.is_some());
    }

    #[tokio::test]
    async fn tls_with_native_certs() {
        let client = async_nats::ConnectOptions::new()
            .require_tls(true)
            .connect("tls://demo.nats.io")
            .await
            .unwrap();

        let mut subscription = client.subscribe("subject").await.unwrap();
        client.publish("subject", "data".into()).await.unwrap();

        client.flush().await.unwrap();
        assert!(subscription.next().await.is_some());
    }

    #[tokio::test]
    async fn tls_first() {
        let _server =
            nats_server::run_server_with_port("tests/configs/tls_first.conf", Some("9090"));

        // Need to add some timeout here, as `client_url` does not work with tls_first aproach,
        // and without it there is nothing that ensures that server is up and running.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let client =
            async_nats::ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
                .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
                .add_client_certificate(
                    path.join("tests/configs/certs/client-cert.pem"),
                    path.join("tests/configs/certs/client-key.pem"),
                )
                .require_tls(true)
                .tls_first()
                .connect("tls://localhost:9090")
                .await
                .unwrap();

        assert!(client.server_info().tls_required);

        async_nats::ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
            .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
            .add_client_certificate(
                path.join("tests/configs/certs/client-cert.pem"),
                path.join("tests/configs/certs/client-key.pem"),
            )
            .require_tls(true)
            .connect("tls://localhost:9090")
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn tls_auto() {
        let server = nats_server::run_server("tests/configs/tls_first_auto.conf");

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        async_nats::ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
            .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
            .add_client_certificate(
                path.join("tests/configs/certs/client-cert.pem"),
                path.join("tests/configs/certs/client-key.pem"),
            )
            .require_tls(true)
            .connect(server.client_url())
            .await
            .unwrap();

        let client =
            async_nats::ConnectOptions::with_user_and_password("derek".into(), "porkchop".into())
                .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
                .add_client_certificate(
                    path.join("tests/configs/certs/client-cert.pem"),
                    path.join("tests/configs/certs/client-key.pem"),
                )
                .require_tls(true)
                .tls_first()
                .connect(server.client_url())
                .await
                .unwrap();

        assert!(client.server_info().tls_required);
    }

    #[tokio::test]
    async fn discovered_ip_servers_keep_tls_name() {
        // Servers advertise 127.0.0.1, while the certificate is only valid
        // for localhost, so discovered servers have to be verified against
        // the name the client was originally given.
        let mut cluster = nats_server::run_cluster("tests/configs/tls_cluster.conf");
        let seed_port = cluster.servers[0].client_port();

        let (reconnect_tx, mut reconnect_rx) = tokio::sync::mpsc::channel(8);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let client = async_nats::ConnectOptions::new()
            .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
            .require_tls(true)
            .event_callback(move |event| {
                let reconnect_tx = reconnect_tx.clone();
                async move {
                    if let async_nats::Event::Connected = event {
                        reconnect_tx.send(()).await.ok();
                    }
                }
            })
            .connect(format!("tls://localhost:{seed_port}"))
            .await
            .unwrap();
        reconnect_rx.recv().await.unwrap();

        let pool = client.server_pool().await.unwrap();
        let discovered: Vec<_> = pool.iter().filter(|s| s.is_discovered).collect();
        assert!(!discovered.is_empty(), "no servers discovered: {pool:?}");
        assert!(
            discovered.iter().all(|s| s.addr.host() == "127.0.0.1"),
            "expected only IP servers to be discovered: {pool:?}"
        );

        // Stop the seed server, leaving only servers reachable by IP.
        drop(cluster.servers.remove(0));

        tokio::time::timeout(Duration::from_secs(10), reconnect_rx.recv())
            .await
            .expect("did not reconnect to a discovered server")
            .unwrap();
        assert_ne!(client.server_info().port, seed_port);
        assert!(client.server_info().tls_required);

        let mut sub = client.subscribe("foo").await.unwrap();
        client.publish("foo", "data".into()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .unwrap();

        // The name check is not skipped: an IP the certificate does not
        // cover is still rejected when dialed directly.
        let port = client.server_info().port;
        async_nats::ConnectOptions::new()
            .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
            .require_tls(true)
            .connect(format!("tls://127.0.0.1:{port}"))
            .await
            .unwrap_err();
    }
}
