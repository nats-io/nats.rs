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
    use futures_util::{future::join_all, StreamExt};

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
    async fn queue_sub() {
        let server = nats_server::run_basic_server();
        const NUM_SUBSCRIBERS: usize = 3;
        const NUM_ITEMS: usize = 20;

        let mut subscribers = Vec::new();
        let mut client = async_nats::connect(server.client_url()).await.unwrap();
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
                while let Ok(Some(item)) = tokio::time::timeout(
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
    async fn unsubscribe() {
        let server = nats_server::run_basic_server();
        let mut client = async_nats::connect(server.client_url()).await.unwrap();

        let mut sub = client.subscribe("test".into()).await.unwrap();

        client.publish("test".into(), "data".into()).await.unwrap();
        client.flush().await.unwrap();

        assert!(sub.next().await.is_some());
        sub.unsubscribe();
        // check if we can still send messages after unsubscribe.
        let mut sub2 = client.subscribe("test2".into()).await.unwrap();
        client.publish("test2".into(), "data".into()).await.unwrap();
        client.flush().await.unwrap();
        assert!(sub2.next().await.is_some());
    }
}
