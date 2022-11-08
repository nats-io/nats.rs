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

use serde::{Deserialize, Serialize};

/// contains info about the `JetStream` usage from the current account.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct AccountInfo {
    pub memory: i64,
    pub storage: i64,
    pub streams: i64,
    pub consumers: i64,
}

mod jetstream {

    use std::str::from_utf8;
    use std::time::{Duration, Instant};

    use super::*;
    use async_nats::connection::State;
    use async_nats::header::HeaderMap;
    use async_nats::jetstream::consumer::{
        self, DeliverPolicy, OrderedPushConsumer, PullConsumer, PushConsumer,
    };
    use async_nats::jetstream::response::Response;
    use async_nats::jetstream::stream::{self, DiscardPolicy, StorageType};
    use async_nats::ConnectOptions;
    use bytes::Bytes;
    use futures::stream::{StreamExt, TryStreamExt};
    use time::OffsetDateTime;
    use tokio_retry::Retry;

    #[tokio::test]
    async fn query_account_requests() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let account = context.query_account().await.unwrap();
        assert_eq!(account.requests.total, 0);

        let account = context.query_account().await.unwrap();
        assert_eq!(account.requests.total, 1);
    }

    #[tokio::test]
    async fn publish_with_headers() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let _stream = context
            .create_stream(stream::Config {
                name: "TEST".to_string(),
                subjects: vec!["foo".into(), "bar".into(), "baz".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let headers = HeaderMap::new();
        let payload = b"Hello JetStream";

        let ack = context
            .publish_with_headers("foo".into(), headers, payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();

        assert_eq!(ack.stream, "TEST");
        assert_eq!(ack.sequence, 1);
    }

    #[tokio::test]
    async fn publish_async() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "TEST".to_string(),
                subjects: vec!["foo".into(), "bar".into(), "baz".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let ack = context
            .publish("foo".to_string(), "paylaod".into())
            .await
            .unwrap();
        assert!(ack.await.is_ok());
        let ack = context
            .publish("not_stream".to_string(), "paylaod".into())
            .await
            .unwrap();
        assert!(ack.await.is_err());
    }

    #[tokio::test]
    async fn request() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "TEST".to_string(),
                subjects: vec!["foo".into(), "bar".into(), "baz".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"Hello JetStream";

        // Basic publish like NATS core.
        let ack = context
            .publish("foo".into(), payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();
        assert_eq!(ack.stream, "TEST");
        assert_eq!(ack.sequence, 1);
    }

    #[tokio::test]
    async fn request_ok() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let response: Response<AccountInfo> =
            context.request("INFO".to_string(), &()).await.unwrap();

        assert!(matches!(response, Response::Ok { .. }));
    }

    #[tokio::test]
    async fn request_err() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let response: Response<AccountInfo> = context
            .request("STREAM.INFO.nonexisting".to_string(), &())
            .await
            .unwrap();

        assert!(matches!(response, Response::Err { .. }));
    }

    // Interesting edge case with Jetstream
    #[tokio::test]
    #[ignore]
    async fn request_no_responders() {
        // let server = nats_server::run_server("tests/configs/jetstream.conf");
        // let client = async_nats::connect(server.client_url()).await.unwrap();
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();
        let context = async_nats::jetstream::new(client);

        let response: Response<AccountInfo> =
            context.request("API.FONI".to_string(), &()).await.unwrap();

        assert!(matches!(response, Response::Err { .. }));
    }

    #[tokio::test]
    async fn create_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context.create_stream("events").await.unwrap();

        context
            .create_stream(&stream::Config {
                name: "events2".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn create_stream_with_replicas() {
        use crate::jetstream::stream::StorageType;
        let cluster = nats_server::run_cluster("tests/configs/jetstream.conf");
        tokio::time::sleep(Duration::from_secs(5)).await;
        let client = async_nats::connect(cluster.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let mut stream = context
            .create_stream(&stream::Config {
                name: "events2".to_string(),
                num_replicas: 3,
                max_bytes: 1024,
                storage: StorageType::Memory,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(stream.cached_info().config.num_replicas, 3);
        assert!(stream.cached_info().cluster.is_some());
        assert_eq!(
            stream
                .info()
                .await
                .unwrap()
                .cluster
                .as_ref()
                .unwrap()
                .replicas
                .len(),
            2
        );

        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some("name".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(consumer.cached_info().cluster.replicas.len(), 2);

        context.delete_stream("events2").await.unwrap();
    }

    #[tokio::test]
    async fn get_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context.create_stream("events").await.unwrap();
        assert_eq!(
            context
                .get_stream("events")
                .await
                .unwrap()
                .info()
                .await
                .unwrap()
                .config
                .name,
            "events".to_string()
        );
    }

    #[tokio::test]
    async fn purge_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context.create_stream("events").await.unwrap();

        for _ in 0..3 {
            context
                .publish("events".to_string(), "data".into())
                .await
                .unwrap();
        }
        let mut stream = context.get_stream("events").await.unwrap();
        assert_eq!(stream.cached_info().state.messages, 3);

        stream.purge().await.unwrap();

        assert_eq!(stream.info().await.unwrap().state.messages, 0);
    }

    #[tokio::test]
    async fn purge_stream_subject() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "events".to_string(),
                subjects: vec!["events.*".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..3 {
            context
                .publish("events.one".to_string(), "data".into())
                .await
                .unwrap();
        }
        for _ in 0..4 {
            context
                .publish("events.two".to_string(), "data".into())
                .await
                .unwrap();
        }
        let mut stream = context.get_stream("events").await.unwrap();
        assert_eq!(stream.cached_info().state.messages, 7);

        stream.purge_subject("events.two").await.unwrap();

        assert_eq!(stream.info().await.unwrap().state.messages, 3);
    }

    #[tokio::test]
    async fn get_or_create_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context.create_stream("events").await.unwrap();
        assert_eq!(
            context
                .get_or_create_stream("events")
                .await
                .unwrap()
                .cached_info()
                .config
                .name,
            "events".to_string()
        );

        assert_eq!(
            context
                .get_or_create_stream(&stream::Config {
                    name: "events2".to_string(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .cached_info()
                .config
                .name,
            "events2".to_string()
        );
    }

    #[tokio::test]
    async fn delete_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context.create_stream("events").await.unwrap();
        assert!(context.delete_stream("events").await.unwrap().success);
    }

    #[tokio::test]
    async fn update_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let _stream = context.create_stream("events").await.unwrap();
        let info = context
            .update_stream(stream::Config {
                name: "events".to_string(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                ..Default::default()
            })
            .await
            .unwrap();
        context.update_stream(&info.config).await.unwrap();
        assert_eq!(info.config.max_messages, 1000);
        assert_eq!(info.config.max_messages_per_subject, 100);
    }

    #[tokio::test]
    async fn get_raw_message() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("events").await.unwrap();
        let payload = b"payload";
        let publish_ack = context
            .publish("events".into(), payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();

        let raw_message = stream.get_raw_message(publish_ack.sequence).await.unwrap();
        assert_eq!(raw_message.sequence, publish_ack.sequence);
    }

    #[tokio::test]
    async fn get_last_raw_message_by_subject() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .get_or_create_stream(stream::Config {
                subjects: vec!["events".to_string(), "entries".to_string()],
                name: "events".to_string(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        let publish_ack = context
            .publish("events".into(), payload.as_ref().into())
            .await
            .unwrap();

        context
            .publish("entries".into(), payload.as_ref().into())
            .await
            .unwrap();

        let raw_message = stream
            .get_last_raw_message_by_subject("events")
            .await
            .unwrap();

        assert_eq!(raw_message.sequence, publish_ack.await.unwrap().sequence);
    }

    #[tokio::test]
    async fn direct_get_last_for_subject() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .get_or_create_stream(stream::Config {
                subjects: vec!["events".to_string(), "entries".to_string()],
                name: "events".to_string(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        context
            .publish("events".into(), "not this".into())
            .await
            .unwrap();
        let publish_ack = context
            .publish("events".into(), payload.as_ref().into())
            .await
            .unwrap();

        context
            .publish("entries".into(), payload.as_ref().into())
            .await
            .unwrap();

        let message = stream.direct_get_last_for_subject("events").await.unwrap();

        let sequence = message
            .headers
            .as_ref()
            .unwrap()
            .get("Nats-Sequence")
            .unwrap()
            .iter()
            .next()
            .unwrap();
        assert_eq!(
            sequence.parse::<u64>().unwrap(),
            publish_ack.await.unwrap().sequence
        );
        assert_eq!(payload, message.payload.as_ref());

        stream
            .direct_get_last_for_subject("wrong")
            .await
            .expect_err("should error");
    }
    #[tokio::test]
    async fn direct_get_next_for_subject() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .get_or_create_stream(stream::Config {
                subjects: vec!["events".to_string(), "entries".to_string()],
                name: "events".to_string(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        let publish_ack = context
            .publish("events".into(), payload.as_ref().into())
            .await
            .unwrap();
        context
            .publish("events".into(), "not this".into())
            .await
            .unwrap();

        context
            .publish("entries".into(), payload.as_ref().into())
            .await
            .unwrap();

        let message = stream
            .direct_get_next_for_subject("events", None)
            .await
            .unwrap();

        let sequence = message
            .headers
            .as_ref()
            .unwrap()
            .get("Nats-Sequence")
            .unwrap()
            .iter()
            .next()
            .unwrap();
        assert_eq!(
            sequence.parse::<u64>().unwrap(),
            publish_ack.await.unwrap().sequence
        );
        assert_eq!(payload, message.payload.as_ref());

        stream
            .direct_get_next_for_subject("wrong", None)
            .await
            .expect_err("should error");
    }

    #[tokio::test]
    async fn direct_get_next_for_subject_after_sequence() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .get_or_create_stream(stream::Config {
                subjects: vec!["events".to_string(), "entries".to_string()],
                name: "events".to_string(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        context
            .publish("events".into(), "not this".into())
            .await
            .unwrap();
        context
            .publish("events".into(), "not this".into())
            .await
            .unwrap();
        let publish_ack = context
            .publish("events".into(), payload.as_ref().into())
            .await
            .unwrap();
        context
            .publish("events".into(), "not this".into())
            .await
            .unwrap();

        context
            .publish("events".into(), "not this".into())
            .await
            .unwrap();

        let message = stream
            .direct_get_next_for_subject("events", Some(3))
            .await
            .unwrap();

        let sequence = message
            .headers
            .as_ref()
            .unwrap()
            .get("Nats-Sequence")
            .unwrap()
            .iter()
            .next()
            .unwrap();
        assert_eq!(
            sequence.parse::<u64>().unwrap(),
            publish_ack.await.unwrap().sequence
        );
        assert_eq!(payload, message.payload.as_ref());

        stream
            .direct_get_next_for_subject("events", Some(14))
            .await
            .expect_err("should error");
        stream
            .direct_get_next_for_subject("entries", Some(1))
            .await
            .expect_err("should error");
    }

    #[tokio::test]
    async fn direct_get_for_sequence() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .get_or_create_stream(stream::Config {
                subjects: vec!["events".to_string(), "entries".to_string()],
                name: "events".to_string(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        context
            .publish("events".into(), "not this".into())
            .await
            .unwrap();
        let publish_ack = context
            .publish("events".into(), payload.as_ref().into())
            .await
            .unwrap();

        context
            .publish("entries".into(), payload.as_ref().into())
            .await
            .unwrap();

        let message = stream.direct_get(2).await.unwrap();

        let sequence = message
            .headers
            .as_ref()
            .unwrap()
            .get("Nats-Sequence")
            .unwrap()
            .iter()
            .next()
            .unwrap();
        assert_eq!(
            sequence.parse::<u64>().unwrap(),
            publish_ack.await.unwrap().sequence
        );
        assert_eq!(payload, message.payload.as_ref());

        stream.direct_get(22).await.expect_err("should error");
    }

    #[tokio::test]
    async fn delete_message() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("events").await.unwrap();
        let payload = b"payload";
        context
            .publish("events".into(), payload.as_ref().into())
            .await
            .unwrap();
        let publish_ack = context
            .publish("events".into(), payload.as_ref().into())
            .await
            .unwrap();
        context
            .publish("events".into(), payload.as_ref().into())
            .await
            .unwrap();

        let consumer = stream
            .get_or_create_consumer(
                "consumer",
                consumer::pull::Config {
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut messages = consumer.messages().await.unwrap();
        stream
            .delete_message(publish_ack.await.unwrap().sequence)
            .await
            .unwrap();

        assert_eq!(
            messages
                .next()
                .await
                .unwrap()
                .unwrap()
                .info()
                .unwrap()
                .stream_sequence,
            1
        );
        assert_eq!(
            messages
                .next()
                .await
                .unwrap()
                .unwrap()
                .info()
                .unwrap()
                .stream_sequence,
            3
        );
    }

    #[tokio::test]
    async fn create_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        // durable consumer
        context
            .get_or_create_stream("events")
            .await
            .unwrap()
            .create_consumer(consumer::pull::Config {
                durable_name: Some("durable".to_string()),
                deliver_policy: DeliverPolicy::ByStartSequence { start_sequence: 10 },
                ..Default::default()
            })
            .await
            .unwrap();
        // ephemeral consumer
        context
            .get_or_create_stream("events")
            .await
            .unwrap()
            .create_consumer(consumer::pull::Config {
                deliver_policy: DeliverPolicy::ByStartTime {
                    start_time: OffsetDateTime::now_utc(),
                },
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .get_or_create_stream("events")
            .await
            .unwrap()
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull_explicit".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer = context
            .get_or_create_stream("events")
            .await
            .unwrap()
            .create_consumer(consumer::pull::Config {
                name: Some("name".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!("name".to_string(), consumer.cached_info().name);

        context
            .get_or_create_stream("events")
            .await
            .unwrap()
            .create_consumer(consumer::pull::Config {
                durable_name: Some("namex".to_string()),
                name: Some("namey".to_string()),
                ..Default::default()
            })
            .await
            .unwrap_err();
    }
    #[tokio::test]
    async fn delete_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("consumer".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        stream.delete_consumer("consumer").await.unwrap();
        assert!(stream
            .get_consumer::<consumer::pull::Config>("consumer")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn get_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("stream").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "subject".to_string(),
                durable_name: Some("push".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();
        let _consumer: PushConsumer = stream.get_consumer("push").await.unwrap();

        let consumer = stream.get_consumer("pull").await.unwrap();
        consumer.fetch().max_messages(10).messages().await.unwrap();
    }

    #[tokio::test]
    async fn get_or_create_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("stream").await.unwrap();

        // this creates the consumer
        let _consumer: PullConsumer = stream
            .get_or_create_consumer::<consumer::pull::Config>(
                "consumer",
                consumer::pull::Config {
                    durable_name: Some("consumer".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // check if consumer is there
        stream
            .get_consumer::<consumer::pull::Config>("consumer")
            .await
            .unwrap();

        // bind to previously created consumer.
        stream
            .get_or_create_consumer::<consumer::pull::Config>(
                "consumer",
                consumer::pull::Config {
                    durable_name: Some("consumer".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pull_sequence() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer = stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..1000 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut iter = consumer.sequence(50).unwrap().take(10);
        while let Ok(Some(mut batch)) = iter.try_next().await {
            while let Ok(Some(message)) = batch.try_next().await {
                assert_eq!(message.payload, Bytes::from(b"dat".as_ref()));
            }
        }
    }

    #[tokio::test]
    async fn pull_stream_by_one() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);
        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("push".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer: PullConsumer = stream.get_consumer("push").await.unwrap();

        for _ in 0..100 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut messages = consumer
            .stream()
            .max_messages_per_batch(1)
            .messages()
            .await
            .unwrap()
            .take(100);

        while let Some(Ok(message)) = messages.next().await {
            println!("message");
            assert_eq!(message.status, None);
            assert_eq!(message.payload.as_ref(), b"dat");
        }
    }

    #[tokio::test]
    async fn push_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "push".to_string(),
                durable_name: Some("push".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer: PushConsumer = stream.get_consumer("push").await.unwrap();

        for _ in 0..1000 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut messages = consumer.messages().await.unwrap().take(1000);
        while let Some(Ok(message)) = messages.next().await {
            assert_eq!(message.status, None);
            assert_eq!(message.payload.as_ref(), b"dat");
        }
    }

    #[tokio::test]
    async fn push_ordered() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                storage: StorageType::Memory,
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer: OrderedPushConsumer = stream
            .create_consumer(consumer::push::OrderedConfig {
                deliver_subject: "push".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::task::spawn({
            let context = context.clone();
            async move {
                for i in 0..1000 {
                    if i % 500 == 0 {
                        tokio::time::sleep(Duration::from_secs(6)).await
                    }
                    context
                        .publish("events".to_string(), "dat".into())
                        .await
                        .unwrap();
                }
            }
        });

        let mut messages = consumer.messages().await.unwrap().take(1000);
        while let Some(message) = messages.next().await {
            let message = message.unwrap();
            assert_eq!(message.status, None);
            assert_eq!(message.payload.as_ref(), b"dat");
        }
    }
    #[tokio::test]
    async fn push_ordered_recreate() {
        let mut server =
            nats_server::run_server_with_port("tests/configs/jetstream.conf", Some("5656"));
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client.clone());

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events.>".to_string()],
                storage: StorageType::File,
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer: OrderedPushConsumer = stream
            .create_consumer(consumer::push::OrderedConfig {
                deliver_subject: "push".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::task::spawn({
            let client = client.clone();
            async move {
                for i in 0..10000 {
                    if i == 10 || i == 500 {
                        server.restart();
                        loop {
                            // FIXME(jarema): publish is not resilient against disconnects.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            if client.connection_state() == State::Connected {
                                break;
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    context
                        .publish(format!("events.{}", i), i.to_string().into())
                        .await
                        .ok();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        let mut messages = consumer.messages().await.unwrap().take(1001).enumerate();
        while let Some((i, message)) = messages.next().await {
            if i == 1001 {
                assert!(message.is_err());
                break;
            }
            let message = message.unwrap();
            assert_eq!(i + 1, message.info().unwrap().stream_sequence as usize);
            assert_eq!(message.status, None);
        }
    }

    // test added just to be sure, that if messages have arrived to the stream already, we won't
    // miss them in AckPolicy::None setup.
    #[tokio::test]
    async fn push_ordered_delayed() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        // let client = async_nats::connect("localhost:4222").await.unwrap();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                storage: StorageType::Memory,
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer: OrderedPushConsumer = stream
            .create_consumer(consumer::push::OrderedConfig {
                deliver_subject: "push".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..1000 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut messages = consumer.messages().await.unwrap().take(1000);
        while let Some(message) = messages.next().await {
            let message = message.unwrap();
            assert_eq!(message.status, None);
            assert_eq!(message.payload.as_ref(), b"dat");
        }
    }

    #[tokio::test]
    async fn push_ordered_capped() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                storage: StorageType::Memory,
                discard: DiscardPolicy::Old,
                max_messages: 500,
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer: OrderedPushConsumer = stream
            .create_consumer(consumer::push::OrderedConfig {
                deliver_subject: "push".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        for i in 0..1000 {
            context
                .publish("events".to_string(), format!("{}", i).into())
                .await
                .unwrap();
        }

        // distrupt stream sequence continuity.
        stream.delete_message(510).await.unwrap();
        stream.delete_message(600).await.unwrap();
        stream.delete_message(800).await.unwrap();

        // take 3 messages less, as we discarded 3 from remaining 500.
        let mut messages = consumer.messages().await.unwrap().take(497);
        // expect sequence to start at 500, after server discarded other 500 of messages.
        let mut i = 500;
        while let Some(message) = messages.next().await {
            // account for deleted messages.
            if i == 509 || i == 599 || i == 799 {
                i += 1;
            }
            let message = message.unwrap();
            assert_eq!(message.status, None);
            assert_eq!(message.payload, bytes::Bytes::from(format!("{}", i)));
            i += 1;
        }
    }

    #[tokio::test]
    async fn push_stream_flow_control() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "push".to_string(),
                durable_name: Some("push".to_string()),
                flow_control: true,
                idle_heartbeat: Duration::from_millis(100),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer: PushConsumer = stream.get_consumer("push").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        for _ in 0..1000 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut messages = consumer.messages().await.unwrap().take(1000);
        while let Some(Ok(message)) = messages.next().await {
            assert_eq!(message.status, None);
            assert_eq!(message.payload.as_ref(), b"dat");
        }
    }

    #[tokio::test]
    async fn push_stream_heartbeat() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "push".to_string(),
                durable_name: Some("push".to_string()),
                idle_heartbeat: Duration::from_millis(100),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer: PushConsumer = stream.get_consumer("push").await.unwrap();
        let mut messages = consumer.messages().await.unwrap().take(1000);

        tokio::time::sleep(Duration::from_secs(1)).await;

        for _ in 0..1000 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut seen = 0;
        while let Some(Ok(message)) = messages.next().await {
            assert_eq!(message.payload.as_ref(), b"dat");
            seen += 1;
        }
        assert_eq!(seen, 1000);
    }

    #[tokio::test]
    async fn pull_stream_default() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..1000 {
                context
                    .publish("events".to_string(), format!("i: {}", i).into())
                    .await
                    .unwrap();
            }
        });

        let mut iter = consumer.messages().await.unwrap().take(1000);
        while let Some(result) = iter.next().await {
            result.unwrap().ack().await.unwrap();
        }

        let info = consumer.info().await.unwrap();
        assert!(info.delivered.last_active.is_some());
    }

    #[tokio::test]
    // Test ignored until Server issue around sending Pull Request immediately after getting
    // 408 timeout is resolved.
    #[ignore]
    async fn pull_stream_with_timeout() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..100 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let ack = context
                    .publish(
                        "events".to_string(),
                        format!("timeout test message: {}", i).into(),
                    )
                    .await
                    .unwrap();
                println!("ack from publish {}: {:?}", i, ack);
            }
            println!("send all 100 messages to jetstream");
        });

        let mut iter = consumer
            .stream()
            .max_messages_per_batch(25)
            .expires(Duration::from_millis(100))
            .messages()
            .await
            .unwrap()
            .take(100);
        while let Some(result) = iter.next().await {
            println!("MESSAGE: {:?}", result);
            result.unwrap().ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn pull_stream_with_heartbeat() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..10 {
                tokio::time::sleep(Duration::from_millis(600)).await;
                context
                    .publish(
                        "events".to_string(),
                        format!("heartbeat message: {}", i).into(),
                    )
                    .await
                    .unwrap();
            }
        });

        let mut iter = consumer
            .stream()
            .max_messages_per_batch(10)
            .expires(Duration::from_millis(1000))
            .heartbeat(Duration::from_millis(500))
            .messages()
            .await
            .unwrap()
            .take(10);
        while let Some(result) = iter.next().await {
            result.unwrap().ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn pull_stream_error() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..100 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                context
                    .publish(
                        "events".to_string(),
                        format!("heartbeat message: {}", i).into(),
                    )
                    .await
                    .unwrap();
            }
        });

        let mut iter = consumer
            .stream()
            .max_messages_per_batch(25)
            .heartbeat(Duration::from_millis(100))
            .expires(Duration::default())
            .messages()
            .await
            .unwrap()
            .take(1);
        while let Some(result) = iter.next().await {
            result.expect_err("should be status error");
        }
    }
    #[tokio::test]
    async fn pull_fetch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {:?}", err) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        for _ in 0..10 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut iter = consumer.fetch().max_messages(100).messages().await.unwrap();

        let mut i = 0;
        while (iter.next().await).is_some() {
            i += 1;
        }
        assert_eq!(i, 10);
    }
    #[tokio::test]
    async fn pull_batch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {:?}", err) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        for _ in 0..100 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut iter = consumer
            .batch()
            .max_messages(100)
            .expires(Duration::from_millis(500))
            .messages()
            .await
            .unwrap();

        let mut i = 0;
        while (iter.next().await).is_some() {
            i += 1;
            if i >= 100 {
                return;
            }
        }
    }
    #[tokio::test]
    async fn pull_consumer_stream_without_heartbeat() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {:?}", err) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        context
            .publish("events".to_string(), "dat".into())
            .await
            .unwrap();

        let mut messages = consumer
            .stream()
            .max_messages_per_batch(3)
            .messages()
            .await
            .unwrap();

        messages.next().await.unwrap().unwrap().ack().await.unwrap();
        let name = &consumer.cached_info().name;
        stream.delete_consumer(name).await.unwrap();
        let now = Instant::now();
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("time elapsed {:?}", now.elapsed());
    }
    #[tokio::test]
    async fn pull_consumer_stream_with_heartbeat() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {:?}", err) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        context
            .publish("events".to_string(), "dat".into())
            .await
            .unwrap();

        let mut messages = consumer.messages().await.unwrap();

        messages.next().await.unwrap().unwrap().ack().await.unwrap();
        let name = &consumer.cached_info().name;
        stream.delete_consumer(name).await.unwrap();
        let now = Instant::now();
        assert_eq!(
            messages
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .downcast::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::TimedOut
        );
        assert!(now.elapsed().le(&Duration::from_secs(50)));
    }

    #[tokio::test]
    async fn consumer_info() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {:?}", err) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();
        assert_eq!(
            consumer.info().await.unwrap().clone(),
            consumer.cached_info().clone()
        );
    }

    #[tokio::test]
    async fn ack() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {:?}", err) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client.clone());

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut consumer = stream.get_consumer("pull").await.unwrap();

        for _ in 0..10 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut iter = consumer.fetch().max_messages(100).messages().await.unwrap();
        client.flush().await.unwrap();

        // TODO: when rtt() is available, use it here.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let info = consumer.info().await.unwrap();
        assert_eq!(info.num_ack_pending, 10);

        // standard ack
        if let Some(message) = iter.next().await {
            message.unwrap().ack().await.unwrap();
        }
        client.flush().await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        let info = consumer.info().await.unwrap();
        assert_eq!(info.num_ack_pending, 9);

        // double ack
        if let Some(message) = iter.next().await {
            message.unwrap().double_ack().await.unwrap();
        }
        client.flush().await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;
        let info = consumer.info().await.unwrap();
        assert_eq!(info.num_ack_pending, 8);

        // in progress
        if let Some(message) = iter.next().await {
            message
                .unwrap()
                .ack_with(async_nats::jetstream::AckKind::Nak)
                .await
                .unwrap();
        }
        client.flush().await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        let info = consumer.info().await.unwrap();
        assert_eq!(info.num_ack_pending, 8);
    }

    #[tokio::test]
    async fn pull_consumer_with_reconnections() {
        let mut server =
            nats_server::run_server_with_port("tests/configs/jetstream.conf", Some("2323"));

        let client = async_nats::ConnectOptions::new()
            .event_callback(|event| async move {
                println!("EVENT: {}", event);
            })
            // .connect(cluster.servers.get(0).unwrap().client_url())
            .connect(server.client_url())
            .await
            .unwrap();

        let jetstream = async_nats::jetstream::new(client.clone());
        // cluster takes some time to spin up.
        // we can have a retry mechanism added later.
        let retry_strategy = tokio_retry::strategy::FibonacciBackoff::from_millis(500).take(5);
        let stream = Retry::spawn(retry_strategy, || {
            jetstream.create_stream(async_nats::jetstream::stream::Config {
                name: "reconnect".to_string(),
                subjects: vec!["reconnect.>".to_string()],
                num_replicas: 1,
                ..Default::default()
            })
        })
        .await
        .unwrap();

        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("durable_reconnect".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        println!("stream and consumer created");

        for i in 0..1000 {
            jetstream
                .publish(format!("reconnect.{}", i), i.to_string().into())
                .await
                .unwrap();
        }

        let messages = consumer
            .stream()
            .expires(Duration::from_secs(60))
            .heartbeat(Duration::from_secs(30))
            .messages()
            .await
            .unwrap();

        println!("starting interation");
        let mut messages = messages.enumerate();
        while let Some((i, message)) = messages.next().await {
            if i % 700 == 0 {
                server.restart();
            }
            let message = message.unwrap();
            message.ack().await.unwrap();
            if from_utf8(&message.payload)
                .unwrap()
                .parse::<usize>()
                .unwrap()
                == 999
            {
                break;
            }
        }
    }

    #[tokio::test]
    async fn timeout_out_request() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        tokio::time::sleep(Duration::from_secs(5)).await;
        let client = async_nats::ConnectOptions::new()
            .event_callback(|event| async move {
                println!("EVENT: {}", event);
            })
            .connect(server.client_url())
            .await
            .unwrap();

        let mut jetstream = async_nats::jetstream::new(client.clone());
        jetstream.set_timeout(Duration::from_millis(500));
        jetstream.create_stream("events").await.unwrap();

        for i in 0..500 {
            jetstream
                .publish("events".into(), format!("{}", i).into())
                .await
                .unwrap();
        }
        drop(server);
        let ack = jetstream
            .publish("events".into(), "fail".into())
            .await
            .unwrap()
            .await;
        println!("ACK: {:?}", ack);
        println!(
            "DOWNCAST: {:?}",
            ack.unwrap_err().downcast::<std::io::Error>()
        );
        // assert_eq!(
        //     ack.unwrap_err()
        //         .downcast::<std::io::Error>()
        //         .unwrap()
        //         .kind(),
        //     ErrorKind::TimedOut
        // )
    }

    #[tokio::test]
    async fn republish() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let _source_stream = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "source".to_string(),
                max_messages: 1000,
                subjects: vec!["source.>".to_string()],
                republish: Some(async_nats::jetstream::stream::Republish {
                    source: ">".to_string(),
                    destination: "dest.>".to_string(),
                    headers_only: false,
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let destination_stream = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "dest".to_string(),
                max_messages: 2000,
                subjects: vec!["dest.>".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer = destination_stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("dest".to_string()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: consumer::AckPolicy::Explicit,
                ..Default::default()
            })
            .await
            .unwrap();
        let mut messages = consumer.messages().await.unwrap().take(100).enumerate();
        for i in 0..100 {
            jetstream
                .publish(format!("source.{}", i), format!("{}", i).into())
                .await
                .unwrap();
        }

        while let Some((i, message)) = messages.next().await {
            let message = message.unwrap();
            assert_eq!(format!("dest.source.{}", i), message.subject);
            assert_eq!(i.to_string(), from_utf8(&message.payload).unwrap());
            message.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn discard_new_per_subject() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let _source_stream = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "source".to_string(),
                max_messages: 10,
                max_messages_per_subject: 2,
                discard_new_per_subject: true,
                subjects: vec!["events.>".to_string()],
                discard: DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .unwrap();

        jetstream
            .publish("events.1".to_string(), "data".into())
            .await
            .unwrap()
            .await
            .unwrap();
        jetstream
            .publish("events.1".to_string(), "data".into())
            .await
            .unwrap()
            .await
            .unwrap();
        jetstream
            .publish("events.1".to_string(), "data".into())
            .await
            .unwrap()
            .await
            .expect_err("should get 503 maximum messages per subject exceeded error");
    }

    #[tokio::test]
    async fn mirrors() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let stream = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "TEST".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        jetstream
            .publish("events".to_string(), "skipped".into())
            .await
            .unwrap();
        jetstream
            .publish("events".to_string(), "data".into())
            .await
            .unwrap();

        let mut mirror = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "MIRROR".to_string(),
                mirror: Some(async_nats::jetstream::stream::Source {
                    name: stream.cached_info().config.name.clone(),
                    start_sequence: Some(2),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        let mirror_info = mirror.info().await.unwrap();
        assert_eq!(
            mirror_info.config.mirror.as_ref().unwrap().name.as_str(),
            "TEST"
        );

        let mut messages = mirror
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                name: Some("consumer".to_string()),
                ..Default::default()
            })
            .await
            .unwrap()
            .messages()
            .await
            .unwrap();

        assert_eq!(
            from_utf8(&messages.next().await.unwrap().unwrap().message.payload).unwrap(),
            "data".to_string(),
        )
    }
    #[tokio::test]
    async fn sources() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let stream = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "TEST".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let stream2 = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "TEST2".to_string(),
                subjects: vec!["events2".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        jetstream
            .publish("events".to_string(), "skipped".into())
            .await
            .unwrap();
        jetstream
            .publish("events".to_string(), "data".into())
            .await
            .unwrap();
        jetstream
            .publish("events2".to_string(), "data".into())
            .await
            .unwrap();
        jetstream
            .publish("events2".to_string(), "data".into())
            .await
            .unwrap();

        let mut source = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "SOURCE".to_string(),
                sources: Some(vec![
                    async_nats::jetstream::stream::Source {
                        name: stream.cached_info().config.name.clone(),
                        start_sequence: Some(2),
                        ..Default::default()
                    },
                    async_nats::jetstream::stream::Source {
                        name: stream2.cached_info().config.name.clone(),
                        start_sequence: Some(1),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            })
            .await
            .unwrap();

        let source_info = source.info().await.unwrap();
        assert_eq!(
            source_info.config.sources.as_ref().unwrap()[0]
                .name
                .as_str(),
            "TEST"
        );
        assert_eq!(
            source_info.config.sources.as_ref().unwrap()[1]
                .name
                .as_str(),
            "TEST2"
        );

        let mut messages = source
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                name: Some("consumer".to_string()),
                ..Default::default()
            })
            .await
            .unwrap()
            .messages()
            .await
            .unwrap();

        assert_eq!(
            from_utf8(&messages.next().await.unwrap().unwrap().message.payload).unwrap(),
            "data".to_string(),
        );
        assert_eq!(
            from_utf8(&messages.next().await.unwrap().unwrap().message.payload).unwrap(),
            "data".to_string(),
        );
        assert_eq!(
            from_utf8(&messages.next().await.unwrap().unwrap().message.payload).unwrap(),
            "data".to_string(),
        );
    }

    #[tokio::test]
    async fn pull_by_bytes() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..1000 {
                context
                    .publish(
                        "events".to_string(),
                        format!("Some bytes to sent with sequence number included: {}", i).into(),
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let mut iter = consumer
            .stream()
            .expires(Duration::from_secs(3))
            .max_bytes_per_batch(1024)
            .messages()
            .await
            .unwrap()
            .take(1000);
        while let Some(result) = iter.next().await {
            result.unwrap().ack().await.unwrap();
        }

        let info = consumer.info().await.unwrap();
        assert!(info.delivered.last_active.is_some());
    }
}
