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

use std::path::PathBuf;

#[test]
fn basic_tls() {
    let server = nats_server::run_server("tests/configs/tls.conf");
    assert!(nats::connect(server.client_url()).is_err());

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    nats::Options::with_user_pass("derek", "porkchop")
        .add_root_certificate(path.join("tests/configs/certs/rootCA.pem"))
        .client_cert(
            path.join("tests/configs/certs/client-cert.pem"),
            path.join("tests/configs/certs/client-key.pem"),
        )
        .connect(server.client_url())
        .unwrap();

    // test scenario where rootCA, client certificate and client key are all in one .pem file
    nats::Options::with_user_pass("derek", "porkchop")
        .add_root_certificate(path.join("tests/configs/certs/client-all.pem"))
        .client_cert(
            path.join("tests/configs/certs/client-all.pem"),
            path.join("tests/configs/certs/client-all.pem"),
        )
        .connect(server.client_url())
        .unwrap();
}

#[test]
fn ip_basic_tls() {
    let server = nats_server::run_server("tests/configs/ip-tls.conf");

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    nats::Options::with_user_pass("derek", "porkchop")
        .add_root_certificate(path.join("tests/configs/certs/ip-ca.pem"))
        .tls_required(true)
        .connect(format!("tls://127.0.0.1:{}", server.client_port()))
        .unwrap();
}
