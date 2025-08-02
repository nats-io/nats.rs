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
    use futures_util::stream::StreamExt;
    use std::path::PathBuf;

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
        nc.publish("hello", "world".into())
            .await
            .expect("published");
    }

    #[tokio::test]
    async fn jwt_auth_builder() {
        let s = nats_server::run_server("tests/configs/jwt.conf");

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let nc = async_nats::ConnectOptions::new()
            .credentials_file(path.join("tests/configs/TestUser.creds"))
            .await
            .expect("loaded user creds file")
            .connect(s.client_url())
            .await
            .unwrap();

        // publish something
        nc.publish("hello", "world".into())
            .await
            .expect("published");
    }

    #[tokio::test]
    async fn jwt_reconnect() {
        use async_nats::ServerAddr;
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let mut servers = vec![
            nats_server::run_server("tests/configs/jwt.conf"),
            nats_server::run_server("tests/configs/jwt.conf"),
            nats_server::run_server("tests/configs/jwt.conf"),
        ];

        let client = async_nats::ConnectOptions::with_credentials_file(
            path.join("tests/configs/TestUser.creds"),
        )
        .await
        .unwrap()
        .connect(
            servers
                .iter()
                .map(|server| server.client_url().parse::<ServerAddr>().unwrap())
                .collect::<Vec<ServerAddr>>()
                .as_slice(),
        )
        .await
        .unwrap();

        let mut subscriber = client.subscribe("test").await.unwrap();
        while !servers.is_empty() {
            client.publish("test", "data".into()).await.unwrap();
            client.flush().await.unwrap();
            assert!(subscriber.next().await.is_some());

            drop(servers.remove(0));
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }
}
