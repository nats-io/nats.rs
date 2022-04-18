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

    use super::nats_server;
    use bytes::Bytes;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn basic_pub_sub() {
        let server = nats_server::run_basic_server();
        let mut client = async_nats::connect(server.client_url()).await.unwrap();

        let mut subscriber = client.subscribe("foo".into()).await.unwrap();

        for _ in 0..10 {
            client.publish("foo".into(), "data".into()).await.unwrap();
        }

        client.flush().await.unwrap();

        let mut i = 0;
        while tokio::time::timeout(tokio::time::Duration::from_millis(500), subscriber.next())
            .await
            .unwrap()
            .is_some()
        {
            i += 1;
            if i >= 10 {
                break;
            }
        }
        assert_eq!(i, 10);
    }

    #[tokio::test]
    async fn cloned_client() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let mut subscriber = client.clone().subscribe("foo".into()).await.unwrap();

        let mut cloned_client = client.clone();
        for _ in 0..10 {
            cloned_client
                .publish("foo".into(), "data".into())
                .await
                .unwrap();
        }

        let mut i = 0;
        while tokio::time::timeout(tokio::time::Duration::from_millis(500), subscriber.next())
            .await
            .unwrap()
            .is_some()
        {
            i += 1;
            if i >= 10 {
                break;
            }
        }
        assert_eq!(i, 10);
    }

    #[tokio::test]
    async fn publish_request() {
        let server = nats_server::run_basic_server();
        let mut client = async_nats::connect(server.client_url()).await.unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();

        tokio::spawn({
            let mut client = client.clone();
            async move {
                let msg = sub.next().await.unwrap();
                client
                    .publish(msg.reply.unwrap(), "resp".into())
                    .await
                    .unwrap();
            }
        });
        let inbox = client.new_inbox();
        let mut insub = client.subscribe(inbox.clone()).await.unwrap();
        client
            .publish_with_reply("test".into(), inbox, "data".into())
            .await
            .unwrap();
        assert!(insub.next().await.is_some());
    }

    #[tokio::test]
    async fn request() {
        let server = nats_server::run_basic_server();
        let mut client = async_nats::connect(server.client_url()).await.unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();

        tokio::spawn({
            let mut client = client.clone();
            async move {
                let msg = sub.next().await.unwrap();
                client
                    .publish(msg.reply.unwrap(), "reply".into())
                    .await
                    .unwrap();
            }
        });

        let resp = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            client.request("test".into(), "request".into()),
        )
        .await
        .unwrap();
        assert_eq!(resp.unwrap().payload, Bytes::from("reply"));
    }

    #[tokio::test]
    async fn token_auth() {
        let server = nats_server::run_server("tests/configs/user_pass.conf");
        let mut nc = async_nats::ConnectOptions::with_user_pass("dupa".into(), "dupaaaa".into())
            .connect("localhost:4222")
            .await
            .unwrap();
        // let mut nc = async_nats::connect("localhost:4222").await.unwrap();
        nc.publish("dupa".into(), "data".into()).await.unwrap();
        nc.flush().await.unwrap();
    }

    #[tokio::test]
    async fn user_pass_auth() {
        let server = nats_server::run_server("tests/configs/user_pass.conf");
        let mut nc = async_nats::ConnectOptions::with_user_pass("derek".into(), "s3cr3t".into())
            .connect(server.client_url())
            .await
            .unwrap();

        nc.publish("test".into(), "test".into()).await.unwrap();
    }

    #[tokio::test]
    async fn user_pass_auth_wrong_pass() {
        let server = nats_server::run_server("tests/configs/user_pass.conf");
        let mut nc = async_nats::ConnectOptions::with_user_pass("derek".into(), "bad".into())
            .connect("localhost:4222")
            .await
            .unwrap();
        let mut sub = nc.subscribe("whatever".into()).await.unwrap();
        tokio::time::timeout(tokio::time::Duration::from_millis(5000), sub.next())
            .await
            .unwrap();
    }
}
