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

mod nats_server;

mod client {
    use std::io;
    use std::path::PathBuf;

    use super::nats_server;
    #[tokio::test]
    async fn basic_tls() -> io::Result<()> {
        let s = nats_server::run_server("tests/configs/tls.conf");

        assert!(async_nats::connect("nats://127.0.0.1").await.is_err());

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        async_nats::Options::new()
            .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
            .add_client_certificate(
                path.join("tests/configs/certs/client-cert.pem"),
                path.join("tests/configs/certs/client-key.pem"),
            )
            .require_tls(true)
            .connect(&s.client_url())
            .await?;

        // test scenario where rootCA, client certificate and client key are all in one .pem file
        async_nats::Options::new()
            .add_root_certificates(path.join("tests/configs/certs/client-all.pem"))
            .add_client_certificate(
                path.join("tests/configs/certs/client-all.pem"),
                path.join("tests/configs/certs/client-all.pem"),
            )
            .require_tls(true)
            .connect(&s.client_url())
            .await?;

        Ok(())
    }
}
