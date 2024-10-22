// Copyright 2024 The NATS Authors
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

mod websocket {
    use std::path::PathBuf;

    use futures::StreamExt;

    #[tokio::test]
    async fn simple() {
        let _server = nats_server::run_server("tests/configs/ws.conf");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("ws://localhost:8444")
            .await
            .unwrap();

        let mut sub = client.subscribe("foo").await.unwrap();
        client.publish("foo", "hello".into()).await.unwrap();
        assert_eq!(sub.next().await.unwrap().payload, "hello");
    }

    #[tokio::test]
    async fn tls() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let _server = nats_server::run_server("tests/configs/ws_tls.conf");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let client = async_nats::ConnectOptions::new()
            .user_and_password("derek".into(), "porkchop".into())
            .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
            .connect("wss://localhost:8445")
            .await
            .unwrap();

        let mut sub = client.subscribe("foo").await.unwrap();
        client.publish("foo", "hello".into()).await.unwrap();
        assert_eq!(sub.next().await.unwrap().payload, "hello");
    }
}
