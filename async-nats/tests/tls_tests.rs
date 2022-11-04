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
    use std::path::PathBuf;

    #[tokio::test]
    async fn basic_tls() {
        let server = nats_server::run_server("tests/configs/tls.conf");
        assert!(async_nats::connect(&server.client_url()).await.is_err());

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
}
