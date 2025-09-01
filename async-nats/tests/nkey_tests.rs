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

    const SECRET_SEED: &str = "SUACH75SWCM5D2JMJM6EKLR2WDARVGZT4QC6LX3AGHSWOMVAKERABBBRWM";

    #[tokio::test]
    async fn nkey_auth() {
        let s = nats_server::run_server("tests/configs/nkey.conf");

        let nc = async_nats::ConnectOptions::with_nkey(SECRET_SEED.into())
            .connect(s.client_url())
            .await
            .unwrap();

        // publish something
        nc.publish("hello", "world".into())
            .await
            .expect("published");
    }

    #[tokio::test]
    async fn nkey_auth_builder() {
        let s = nats_server::run_server("tests/configs/nkey.conf");

        let nc = async_nats::ConnectOptions::new()
            .nkey(SECRET_SEED.into())
            .connect(s.client_url())
            .await
            .unwrap();

        // publish something
        nc.publish("hello", "world".into())
            .await
            .expect("published");
    }

    #[tokio::test]
    async fn nkey_reconnect() {
        use async_nats::ServerAddr;

        let mut servers = vec![
            nats_server::run_server("tests/configs/nkey.conf"),
            nats_server::run_server("tests/configs/nkey.conf"),
            nats_server::run_server("tests/configs/nkey.conf"),
        ];

        let client = async_nats::ConnectOptions::with_nkey(SECRET_SEED.into())
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
