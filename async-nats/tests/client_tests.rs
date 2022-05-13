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
    use futures::future::join_all;
    use futures::stream::StreamExt;
    use std::time::Duration;

    #[tokio::test]
    async fn basic_pub_sub() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

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
    async fn queue_sub() {
        let server = nats_server::run_basic_server();
        const NUM_SUBSCRIBERS: usize = 3;
        const NUM_ITEMS: usize = 20;

        let mut subscribers = Vec::new();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        for _i in 0..NUM_SUBSCRIBERS {
            subscribers.push(
                client
                    .queue_subscribe("qfoo".into(), "group".into())
                    .await
                    .unwrap(),
            );
        }

        for _ in 0..NUM_ITEMS {
            client.publish("qfoo".into(), "data".into()).await.unwrap();
        }
        client.flush().await.unwrap();
        let mut results = Vec::new();
        for mut subscriber in subscribers.into_iter() {
            results.push(tokio::spawn(async move {
                let mut count = 0u32;
                while let Ok(Some(_)) = tokio::time::timeout(
                    tokio::time::Duration::from_millis(1000),
                    subscriber.next(),
                )
                .await
                {
                    count += 1;
                }
                count
            }));
        }
        let counts = join_all(results.iter_mut())
            .await
            .into_iter()
            .filter_map(|n| n.ok())
            .collect::<Vec<u32>>();
        let total: u32 = counts.iter().sum();
        assert_eq!(total, NUM_ITEMS as u32, "all items received");
        let num_receivers = counts.into_iter().filter(|n| *n > 0u32).count();
        assert!(num_receivers > 1, "should not all go to single subscriber");
    }

    #[tokio::test]
    async fn cloned_client() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let mut subscriber = client.clone().subscribe("foo".into()).await.unwrap();

        let cloned_client = client.clone();
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
    async fn publish_with_headers() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let mut subscriber = client.subscribe("test".into()).await.unwrap();

        let mut headers = async_nats::HeaderMap::new();
        headers.append("X-Test", b"Test".as_ref().try_into().unwrap());

        client
            .publish_with_headers("test".into(), headers.clone(), b"".as_ref().into())
            .await
            .unwrap();

        client.flush().await.unwrap();

        let message = subscriber.next().await.unwrap();
        assert_eq!(message.headers.unwrap(), headers);
    }

    #[tokio::test]
    async fn publish_request() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();

        tokio::spawn({
            let client = client.clone();
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
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();

        tokio::spawn({
            let client = client.clone();
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
    async fn unsubscribe() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();

        client.publish("test".into(), "data".into()).await.unwrap();
        client.flush().await.unwrap();

        assert!(sub.next().await.is_some());
        sub.unsubscribe().await.unwrap();
        // check if we can still send messages after unsubscribe.
        let mut sub2 = client.subscribe("test2".into()).await.unwrap();
        client.publish("test2".into(), "data".into()).await.unwrap();
        client.flush().await.unwrap();
        assert!(sub2.next().await.is_some());
    }

    #[tokio::test]
    async fn unsubscribe_after() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();

        for _ in 0..2 {
            client.publish("test".into(), "data".into()).await.unwrap();
        }

        sub.unsubscribe_after(3).await.unwrap();
        client.publish("test".into(), "data".into()).await.unwrap();
        client.flush().await.unwrap();

        for _ in 0..3 {
            assert!(sub.next().await.is_some());
        }
        assert!(sub.next().await.is_none());
    }
    #[tokio::test]
    async fn unsubscribe_after_immediate() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();

        client.publish("test".into(), "data".into()).await.unwrap();
        client.publish("test".into(), "data".into()).await.unwrap();

        sub.unsubscribe_after(1).await.unwrap();
        client.flush().await.unwrap();

        assert!(sub.next().await.is_some());
        assert!(sub.next().await.is_none());
    }

    #[tokio::test]
    async fn connect_invalid() {
        assert!(async_nats::connect("localhost:1111").await.is_err());
    }

    #[tokio::test]
    async fn reconnect_fallback() {
        use async_nats::ServerAddr;

        let mut servers = vec![
            nats_server::run_basic_server(),
            nats_server::run_basic_server(),
            nats_server::run_basic_server(),
        ];

        let client = async_nats::ConnectOptions::new()
            .connect(
                servers
                    .iter()
                    .map(|server| server.client_url().parse::<ServerAddr>().unwrap())
                    .collect::<Vec<ServerAddr>>()
                    .as_slice(),
            )
            .await
            .unwrap();

        let mut subscriber = client.subscribe("test".into()).await.unwrap();
        while !servers.is_empty() {
            client.publish("test".into(), "data".into()).await.unwrap();
            client.flush().await.unwrap();
            assert!(subscriber.next().await.is_some());

            drop(servers.remove(0));
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }

    #[tokio::test]
    async fn token_auth() {
        let server = nats_server::run_server("tests/configs/token.conf");
        let client = async_nats::ConnectOptions::with_token("s3cr3t".into())
            .connect(server.client_url())
            .await
            .unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();
        client.publish("test".into(), "test".into()).await.unwrap();
        client.flush().await.unwrap();
        assert!(sub.next().await.is_some());
    }

    #[tokio::test]
    async fn user_pass_auth() {
        let server = nats_server::run_server("tests/configs/user_pass.conf");
        let client =
            async_nats::ConnectOptions::with_user_and_password("derek".into(), "s3cr3t".into())
                .connect(server.client_url())
                .await
                .unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();
        client.publish("test".into(), "test".into()).await.unwrap();
        client.flush().await.unwrap();
        assert!(sub.next().await.is_some());
    }

    #[tokio::test]
    async fn user_pass_auth_wrong_pass() {
        let server = nats_server::run_server("tests/configs/user_pass.conf");
        async_nats::ConnectOptions::with_user_and_password("derek".into(), "bad_password".into())
            .connect(server.client_url())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn connection_callbacks() {
        let server = nats_server::run_basic_server();
        let port = server.client_port().to_string();

        let (tx, mut rx) = tokio::sync::mpsc::channel(128);
        let (dc_tx, mut dc_rx) = tokio::sync::mpsc::channel(128);
        let client = async_nats::ConnectOptions::new()
            .reconnect_callback(move || {
                let tx = tx.clone();
                async move {
                    println!("reconnection callback fired");
                    tx.send(()).await.unwrap();
                }
            })
            .disconnect_callback(move || {
                let dc_tx = dc_tx.clone();
                async move {
                    println!("disconnect callback fired");
                    dc_tx.send(()).await.unwrap();
                }
            })
            .connect(server.client_url())
            .await
            .unwrap();
        println!("conncted");
        client.subscribe("test".to_string()).await.unwrap();
        client.flush().await.unwrap();

        println!("dropped server {:?}", server.client_url());
        drop(server);
        tokio::time::sleep(Duration::from_secs(3)).await;

        let _server = nats_server::run_server_with_port("", Some(port.as_str()));

        tokio::time::timeout(Duration::from_secs(15), dc_rx.recv())
            .await
            .unwrap()
            .unwrap();

        tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
}
