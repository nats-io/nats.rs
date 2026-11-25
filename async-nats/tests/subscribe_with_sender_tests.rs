// Copyright 2020-2026 The NATS Authors
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

mod subscribe_with_sender {
    use std::collections::HashMap;
    use std::time::Duration;

    use async_nats::{ConnectOptions, Event, Message};
    use futures_util::StreamExt;
    use tokio::sync::mpsc;

    async fn next(receiver: &mut mpsc::Receiver<(u64, Message)>) -> (u64, String) {
        let (sid, message) = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("a message arrives")
            .expect("the channel is open");
        (sid, message.subject.to_string())
    }

    async fn quiet(receiver: &mut mpsc::Receiver<(u64, Message)>) {
        let more = tokio::time::timeout(Duration::from_millis(300), receiver.recv()).await;
        assert!(more.is_err(), "unexpected message {more:?}");
    }

    #[tokio::test]
    async fn each_message_carries_its_subscription_id() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (sender, mut receiver) = mpsc::channel(16);
        let mut handles = HashMap::new();
        for (subject, heard) in [("a", "a"), ("b.*", "b.x"), ("c.>", "c.x.y")] {
            let handle = client
                .subscribe_with_sender(subject, sender.clone())
                .await
                .unwrap();
            handles.insert(handle.sid(), (heard, handle));
        }
        client.flush().await.unwrap();

        for subject in ["a", "b.x", "c.x.y"] {
            client.publish(subject, "data".into()).await.unwrap();
        }
        for _ in 0..3 {
            let (sid, heard) = next(&mut receiver).await;
            assert_eq!(handles[&sid].0, heard);
        }
        quiet(&mut receiver).await;
    }

    #[tokio::test]
    async fn dropping_a_handle_unsubscribes_it() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (sender, mut receiver) = mpsc::channel(16);
        let gone = client
            .subscribe_with_sender("gone", sender.clone())
            .await
            .unwrap();
        let kept = client.subscribe_with_sender("kept", sender).await.unwrap();
        drop(gone);
        client.flush().await.unwrap();

        client.publish("gone", "data".into()).await.unwrap();
        client.publish("kept", "data".into()).await.unwrap();
        assert_eq!(next(&mut receiver).await, (kept.sid(), "kept".into()));
        quiet(&mut receiver).await;
    }

    #[tokio::test]
    async fn unsubscribing_a_handle_stops_its_messages() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (sender, mut receiver) = mpsc::channel(16);
        let gone = client
            .subscribe_with_sender("gone", sender.clone())
            .await
            .unwrap();
        let kept = client.subscribe_with_sender("kept", sender).await.unwrap();
        gone.unsubscribe().await.unwrap();
        client.flush().await.unwrap();

        client.publish("gone", "data".into()).await.unwrap();
        client.publish("kept", "data".into()).await.unwrap();
        assert_eq!(next(&mut receiver).await, (kept.sid(), "kept".into()));
        quiet(&mut receiver).await;
    }

    #[tokio::test]
    async fn a_full_channel_reports_the_sid_it_dropped() {
        let server = nats_server::run_basic_server();
        let (events, mut slow) = mpsc::channel(128);
        let client = ConnectOptions::new()
            .event_callback(move |event| {
                let events = events.clone();
                async move {
                    if let Event::SlowConsumer(sid) = event {
                        events.send(sid).await.unwrap();
                    }
                }
            })
            .connect(server.client_url())
            .await
            .unwrap();
        let (sender, mut receiver) = mpsc::channel(1);
        let fast = client
            .subscribe_with_sender("fast", sender.clone())
            .await
            .unwrap();
        let flood = client.subscribe_with_sender("flood", sender).await.unwrap();
        client.flush().await.unwrap();

        client.publish("fast", "data".into()).await.unwrap();
        client.publish("flood", "data".into()).await.unwrap();
        client.flush().await.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), slow.recv()).await;
        assert_eq!(event.unwrap(), Some(flood.sid()));

        assert_eq!(next(&mut receiver).await, (fast.sid(), "fast".into()));
        client.publish("flood", "data".into()).await.unwrap();
        assert_eq!(next(&mut receiver).await, (flood.sid(), "flood".into()));
    }

    #[tokio::test]
    async fn subscriptions_survive_a_reconnect() {
        let mut server = nats_server::run_server_with_port("", Some("5761"));
        let (events, mut connected) = mpsc::channel(128);
        let client = ConnectOptions::new()
            .event_callback(move |event| {
                let events = events.clone();
                async move {
                    if let Event::Connected = event {
                        events.send(()).await.unwrap();
                    }
                }
            })
            .connect(server.client_url())
            .await
            .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), connected.recv()).await;
        first.unwrap().unwrap();
        let (sender, mut receiver) = mpsc::channel(16);
        let handle = client.subscribe_with_sender("kept", sender).await.unwrap();
        client.flush().await.unwrap();

        server.restart();
        let again = tokio::time::timeout(Duration::from_secs(10), connected.recv()).await;
        again.unwrap().unwrap();
        client.flush().await.unwrap();
        client.publish("kept", "data".into()).await.unwrap();
        assert_eq!(next(&mut receiver).await, (handle.sid(), "kept".into()));
    }

    #[tokio::test]
    async fn the_channel_ends_once_the_client_drains() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (sender, mut receiver) = mpsc::channel(16);
        let _handle = client.subscribe_with_sender("data", sender).await.unwrap();
        client.flush().await.unwrap();

        client.drain().await.unwrap();
        let end = tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await;
        assert!(end.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_dropped_receiver_leaves_other_subscriptions_working() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (sender, receiver) = mpsc::channel(16);
        let _handle = client.subscribe_with_sender("data", sender).await.unwrap();
        let mut plain = client.subscribe("data").await.unwrap();
        client.flush().await.unwrap();
        drop(receiver);

        for _ in 0..2 {
            client.publish("data", "data".into()).await.unwrap();
            let heard = tokio::time::timeout(Duration::from_secs(5), plain.next()).await;
            assert_eq!(heard.unwrap().unwrap().subject.as_str(), "data");
        }
    }

    #[tokio::test]
    async fn a_queue_group_delivers_each_message_to_one_member() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (sender, mut receiver) = mpsc::channel(16);
        let mut members = Vec::new();
        for _ in 0..2 {
            let member = client
                .queue_subscribe_with_sender("work", "workers".into(), sender.clone())
                .await
                .unwrap();
            members.push(member);
        }
        client.flush().await.unwrap();

        for _ in 0..8 {
            client.publish("work", "data".into()).await.unwrap();
        }
        for _ in 0..8 {
            let (sid, heard) = next(&mut receiver).await;
            assert!(members.iter().any(|member| member.sid() == sid));
            assert_eq!(heard, "work");
        }
        quiet(&mut receiver).await;
    }
}
