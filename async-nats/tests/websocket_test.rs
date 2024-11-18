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

#[cfg(feature = "websockets")]
mod websockets {
    use std::path::PathBuf;

    use futures::StreamExt;

    #[tokio::test]
    async fn core() {
        let _server = nats_server::run_server("tests/configs/ws.conf");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("ws://localhost:8444")
            .await
            .unwrap();

        // Simple pub/sub
        let mut sub = client.subscribe("foo").await.unwrap();
        client.publish("foo", "hello".into()).await.unwrap();
        assert_eq!(sub.next().await.unwrap().payload, "hello");

        // Large messages
        let payload = bytes::Bytes::from(vec![22; 1024 * 1024]);

        let mut sub = client.subscribe("foo").await.unwrap().take(10);
        for _ in 0..10 {
            client.publish("foo", payload.clone()).await.unwrap();
        }
        while let Some(msg) = sub.next().await {
            assert_eq!(msg.payload, payload);
        }

        // Request/reply
        let mut requests = client.subscribe("foo").await.unwrap();
        tokio::task::spawn({
            let client = client.clone();
            async move {
                let request = requests.next().await.unwrap();
                client
                    .publish(request.reply.unwrap(), request.payload)
                    .await
                    .unwrap();
            }
        });
        let response = client.request("foo", "hello".into()).await.unwrap();
        assert_eq!(response.payload, "hello");
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
