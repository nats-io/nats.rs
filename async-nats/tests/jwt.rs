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
    use bytes::Bytes;
    use futures::stream::StreamExt;
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn jwt_auth() {
        let s = nats_server::run_server("tests/configs/jwt.conf");

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let nc = async_nats::ConnectOptions::with_credentials_file(
            path.join("tests/configs/TestUser.creds"),
        )
        .await
        .expect("loaded user creds file")
        .connect(s.client_url())
        .await
        .unwrap();

        // publish something
        nc.publish("hello".into(), "world".into())
            .await
            .expect("published");
    }

    #[tokio::test]
    async fn jwt_reconnect() {
        let server = nats_server::run_server("tests/configs/jwt.conf");

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let client = async_nats::ConnectOptions::with_credentials_file(
            path.join("tests/configs/TestUser.creds"),
        )
        .await
        .expect("loaded user creds file")
        .connect(server.client_url())
        .await
        .unwrap();

        // Subscribe to our subject
        let mut subscriber = client.subscribe("events".into()).await.unwrap();

        // publish something
        client
            .publish("events".into(), "one".into())
            .await
            .expect("published");

        // Drop the server
        drop(server);

        // Wait a bit for the server to die completely
        sleep(Duration::from_secs(1)).await;

        // Publish while disconnected
        client
            .publish("events".into(), "two".into())
            .await
            .expect("published");

        // And start another instance which should trigger reconnect
        let _server = nats_server::run_server("tests/configs/jwt.conf");

        // Check that everything arrived in order
        let message = subscriber.next().await.unwrap();
        assert_eq!(message.payload, Bytes::from("one"));

        let message = subscriber.next().await.unwrap();
        assert_eq!(message.payload, Bytes::from("two"));
    }
}
