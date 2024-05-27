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

    #[cfg(feature = "server_2_10")]
    use std::collections::HashMap;
    use std::str::from_utf8;
    use std::time::{Duration, Instant};

    use super::*;
    use async_nats::connection::State;
    use async_nats::header::{self, HeaderMap, NATS_MESSAGE_ID};
    use async_nats::jetstream::consumer::{
        self, push, AckPolicy, DeliverPolicy, Info, OrderedPullConsumer, OrderedPushConsumer,
        PullConsumer, PushConsumer, ReplayPolicy,
    };
    use async_nats::jetstream::context::{Publish, PublishErrorKind};
    use async_nats::jetstream::response::Response;
    use async_nats::jetstream::stream::{
        self, ConsumerCreateStrictErrorKind, ConsumerUpdateErrorKind, DiscardPolicy, StorageType,
    };
    #[cfg(feature = "server_2_10")]
    use async_nats::jetstream::stream::{Compression, ConsumerLimits, Source, SubjectTransform};
    use async_nats::jetstream::AckKind;
    use async_nats::ConnectOptions;
    use futures::stream::{StreamExt, TryStreamExt};
    use time::OffsetDateTime;
    use tracing::debug;

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
                name: "TEST".into(),
                subjects: vec!["foo".into(), "bar".into(), "baz".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let headers = HeaderMap::new();
        let payload = b"Hello JetStream";

        let ack = context
            .publish_with_headers("foo", headers, payload.as_ref().into())
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
                name: "TEST".into(),
                subjects: vec!["foo".into(), "bar".into(), "baz".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let ack = context.publish("foo", "payload".into()).await.unwrap();
        assert!(ack.await.is_ok());
        let ack = context
            .publish("not_stream", "payload".into())
            .await
            .unwrap();
        assert!(ack.await.is_err());
    }

    #[tokio::test]
    async fn send_publish() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let mut stream = context
            .create_stream(stream::Config {
                name: "TEST".into(),
                subjects: vec!["foo".into(), "bar".into(), "baz".into()],
                allow_direct: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let id = "UUID".to_string();
        // Publish first message
        context
            .send_publish(
                "foo",
                Publish::build()
                    .message_id(id.clone())
                    .payload("data".into()),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        // Publish second message, a duplicate.
        context
            .send_publish("foo", Publish::build().message_id(id.clone()))
            .await
            .unwrap()
            .await
            .unwrap();
        // Check if we still have one message.
        let info = stream.info().await.unwrap();
        assert_eq!(1, info.state.messages);
        let message = stream
            .direct_get_last_for_subject("foo".to_string())
            .await
            .unwrap();
        assert_eq!(message.payload, bytes::Bytes::from("data"));

        // Publish message with different ID and expect error.
        let err = context
            .send_publish("foo", Publish::build().expected_last_message_id("BAD_ID"))
            .await
            .unwrap()
            .await
            .unwrap_err()
            .kind();
        assert_eq!(err, PublishErrorKind::WrongLastMessageId);
        // Publish a new message with expected ID.
        context
            .send_publish("foo", Publish::build().expected_last_message_id(id.clone()))
            .await
            .unwrap()
            .await
            .unwrap();

        // We should have now two messages. Check it.
        context
            .send_publish("foo", Publish::build().expected_last_sequence(2))
            .await
            .unwrap()
            .await
            .unwrap();
        // 3 messages should be there, so this should error.
        assert_eq!(
            context
                .send_publish("foo", Publish::build().expected_last_sequence(2),)
                .await
                .unwrap()
                .await
                .unwrap_err()
                .kind(),
            PublishErrorKind::WrongLastSequence
        );
        // 3 messages there, should be ok for this subject too.
        context
            .send_publish("foo", Publish::build().expected_last_subject_sequence(3))
            .await
            .unwrap()
            .await
            .unwrap();
        // 4 messages there, should error.
        assert_eq!(
            context
                .send_publish("foo", Publish::build().expected_last_subject_sequence(3),)
                .await
                .unwrap()
                .await
                .unwrap_err()
                .kind(),
            PublishErrorKind::WrongLastSequence
        );

        // Check if it works for the other subjects in the stream.
        context
            .send_publish("bar", Publish::build().expected_last_subject_sequence(0))
            .await
            .unwrap()
            .await
            .unwrap();
        // Sequence is now 1, so this should fail.
        context
            .send_publish("bar", Publish::build().expected_last_subject_sequence(0))
            .await
            .unwrap()
            .await
            .unwrap_err();
        // test header shorthand
        assert_eq!(stream.info().await.unwrap().state.messages, 5);
        context
            .send_publish("foo", Publish::build().header(NATS_MESSAGE_ID, id.as_str()))
            .await
            .unwrap()
            .await
            .unwrap();
        // above message should be ignored.
        assert_eq!(stream.info().await.unwrap().state.messages, 5);
        context
            .send_publish("bar", Publish::build().expected_stream("TEST"))
            .await
            .unwrap()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn request() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "TEST".into(),
                subjects: vec!["foo".into(), "bar".into(), "baz".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"Hello JetStream";

        // Basic publish like NATS core.
        let ack = context
            .publish("foo", payload.as_ref().into())
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

        let response: Response<AccountInfo> = context.request("INFO", &()).await.unwrap();

        assert!(matches!(response, Response::Ok { .. }));
    }

    #[tokio::test]
    async fn request_err() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let response: Response<AccountInfo> = context
            .request("STREAM.INFO.nonexisting", &())
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

        let response: Response<AccountInfo> = context.request("API.FONI", &()).await.unwrap();

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
                name: "events2".into(),
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

        let mut stream = tryhard::retry_fn(|| {
            context.create_stream(stream::Config {
                name: "events2".to_string(),
                num_replicas: 3,
                max_bytes: 1024,
                storage: StorageType::Memory,
                ..Default::default()
            })
        })
        .retries(5)
        .exponential_backoff(Duration::from_millis(500))
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
                durable_name: Some("name".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let cluster = consumer.cached_info().cluster.as_ref().unwrap();

        assert_eq!(cluster.replicas.len(), 2);

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
            "events"
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
                .publish("events", "data".into())
                .await
                .unwrap()
                .await
                .unwrap();
        }
        let mut stream = context.get_stream("events").await.unwrap();
        assert_eq!(stream.cached_info().state.messages, 3);

        stream.purge().await.unwrap();

        assert_eq!(stream.info().await.unwrap().state.messages, 0);
    }

    #[tokio::test]
    async fn purge_filter() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "events".into(),
                subjects: vec!["events.*".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..3 {
            context
                .publish("events.one", "data".into())
                .await
                .unwrap()
                .await
                .unwrap();
        }
        for _ in 0..4 {
            context
                .publish("events.two", "data".into())
                .await
                .unwrap()
                .await
                .unwrap();
        }
        let mut stream = context.get_stream("events").await.unwrap();
        assert_eq!(stream.cached_info().state.messages, 7);

        stream.purge().filter("events.two").await.unwrap();

        assert_eq!(stream.info().await.unwrap().state.messages, 3);
    }
    #[tokio::test]
    async fn purge() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "events".into(),
                subjects: vec!["events.*".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..100 {
            context
                .publish("events.two", "data".into())
                .await
                .unwrap()
                .await
                .unwrap();
        }
        let mut stream = context.get_stream("events").await.unwrap();

        stream.purge().sequence(90).await.unwrap();
        assert_eq!(stream.info().await.unwrap().state.messages, 11);
        stream.purge().keep(5).await.unwrap();
        assert_eq!(stream.info().await.unwrap().state.messages, 5);
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
            "events"
        );

        assert_eq!(
            context
                .get_or_create_stream(&stream::Config {
                    name: "events2".into(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .cached_info()
                .config
                .name,
            "events2"
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
                name: "events".into(),
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
            .publish("events", payload.as_ref().into())
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
                subjects: vec!["events".into(), "entries".into()],
                name: "events".into(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        let publish_ack = context
            .publish("events", payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();

        context
            .publish("entries", payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();

        let raw_message = stream
            .get_last_raw_message_by_subject("events")
            .await
            .unwrap();

        assert_eq!(raw_message.sequence, publish_ack.sequence);
    }

    #[tokio::test]
    async fn direct_get_last_for_subject() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .get_or_create_stream(stream::Config {
                subjects: vec!["events".into(), "entries".into()],
                name: "events".into(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        context
            .publish("events", "not this".into())
            .await
            .unwrap()
            .await
            .unwrap();
        let publish_ack = context
            .publish("events", payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();

        context
            .publish("entries", payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();

        let message = stream.direct_get_last_for_subject("events").await.unwrap();

        let sequence = message
            .headers
            .as_ref()
            .unwrap()
            .get(header::NATS_SEQUENCE)
            .unwrap()
            .as_str();

        assert_eq!(sequence.parse::<u64>().unwrap(), publish_ack.sequence);
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
                subjects: vec!["events".into(), "entries".into()],
                name: "events".into(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        let publish_ack = context
            .publish("events", payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();
        context
            .publish("events", "not this".into())
            .await
            .unwrap()
            .await
            .unwrap();

        context
            .publish("entries", payload.as_ref().into())
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
            .get(header::NATS_SEQUENCE)
            .unwrap()
            .as_str();

        assert_eq!(sequence.parse::<u64>().unwrap(), publish_ack.sequence);
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
                subjects: vec!["events".into(), "entries".into()],
                name: "events".into(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        context
            .publish("events", "not this".into())
            .await
            .unwrap()
            .await
            .unwrap();
        context
            .publish("events", "not this".into())
            .await
            .unwrap()
            .await
            .unwrap();
        let publish_ack = context
            .publish("events", payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();
        context
            .publish("events", "not this".into())
            .await
            .unwrap()
            .await
            .unwrap();

        context
            .publish("events", "not this".into())
            .await
            .unwrap()
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
            .get(header::NATS_SEQUENCE)
            .unwrap()
            .as_str();

        assert_eq!(sequence.parse::<u64>().unwrap(), publish_ack.sequence);
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
                subjects: vec!["events".into(), "entries".into()],
                name: "events".into(),
                max_messages: 1000,
                max_messages_per_subject: 100,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload = b"payload";
        context
            .publish("events", "not this".into())
            .await
            .unwrap()
            .await
            .unwrap();
        // .await
        // .unwrap();
        let publish_ack = context
            .publish("events", payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();
        // .await
        // .unwrap();

        context
            .publish("entries", payload.as_ref().into())
            .await
            .unwrap()
            .await
            .unwrap();
        // .await
        // .unwrap();

        let message = stream.direct_get(2).await.unwrap();

        let sequence = message
            .headers
            .as_ref()
            .unwrap()
            .get(header::NATS_SEQUENCE)
            .unwrap()
            .as_str();

        assert_eq!(sequence.parse::<u64>().unwrap(), publish_ack.sequence);
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
            .publish("events", payload.as_ref().into())
            .await
            .unwrap();
        let publish_ack = context
            .publish("events", payload.as_ref().into())
            .await
            .unwrap();
        context
            .publish("events", payload.as_ref().into())
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
                durable_name: Some("durable".into()),
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
                durable_name: Some("pull_explicit".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer = context
            .get_or_create_stream("events")
            .await
            .unwrap()
            .create_consumer(consumer::pull::Config {
                name: Some("name".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!("name", consumer.cached_info().name);

        context
            .get_or_create_stream("events")
            .await
            .unwrap()
            .create_consumer(consumer::pull::Config {
                durable_name: Some("namex".into()),
                name: Some("namey".into()),
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
                durable_name: Some("consumer".into()),
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
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "subject".into(),
                durable_name: Some("push".into()),
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
    async fn fetch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(&server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .create_stream(jetstream::stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..20 {
            context.publish("events", "data".into()).await.unwrap();
        }

        let consumer = stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let messages = consumer
            .fetch()
            .max_messages(15)
            .messages()
            .await
            .unwrap()
            .count()
            .await;
        assert_eq!(messages, 15);

        let messages = consumer
            .fetch()
            .max_messages(15)
            .messages()
            .await
            .unwrap()
            .count()
            .await;
        assert_eq!(messages, 5);
    }

    #[tokio::test]
    async fn get_consumer_from_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("stream").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        stream
            .create_consumer(consumer::push::Config {
                durable_name: Some("push".into()),
                deliver_subject: "subject".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer: PullConsumer = context
            .get_consumer_from_stream("pull", "stream")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_consumer_from_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("stream").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        stream
            .create_consumer(consumer::push::Config {
                durable_name: Some("push".into()),
                deliver_subject: "subject".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .delete_consumer_from_stream("pull", "stream")
            .await
            .unwrap();
        assert!(stream
            .get_consumer::<consumer::Config>("pull")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn create_consumer_on_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("stream").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer = context
            .create_consumer_on_stream(
                consumer::push::Config {
                    durable_name: Some("push".into()),
                    deliver_subject: "subject".into(),
                    ..Default::default()
                },
                "stream",
            )
            .await
            .unwrap();
        assert_eq!(consumer.cached_info().name, "push");
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
                    durable_name: Some("consumer".into()),
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
                    durable_name: Some("consumer".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    #[cfg(feature = "slow_tests")]
    #[tokio::test]
    async fn pull_sequence() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer = stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..1000 {
            context.publish("events", "dat".into()).await.unwrap();
        }

        let mut iter = consumer.sequence(50).unwrap().take(10);
        while let Ok(Some(mut batch)) = iter.try_next().await {
            while let Ok(Some(message)) = batch.try_next().await {
                assert_eq!(message.payload, bytes::Bytes::from(b"dat".as_ref()));
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
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("push".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer: PullConsumer = stream.get_consumer("push").await.unwrap();

        for _ in 0..100 {
            context.publish("events", "dat".into()).await.unwrap();
        }

        let mut messages = consumer
            .stream()
            .max_messages_per_batch(1)
            .messages()
            .await
            .unwrap()
            .take(100);

        while let Some(Ok(message)) = messages.next().await {
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
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "push".into(),
                durable_name: Some("push".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer: PushConsumer = stream.get_consumer("push").await.unwrap();

        for _ in 0..1000 {
            context.publish("events", "dat".into()).await.unwrap();
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
                name: "events".into(),
                subjects: vec!["events".into()],
                storage: StorageType::Memory,
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer: OrderedPushConsumer = stream
            .create_consumer(consumer::push::OrderedConfig {
                deliver_subject: "push".into(),
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
                    context.publish("events", "dat".into()).await.unwrap();
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
    async fn pull_ordered() {
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
        let consumer: OrderedPullConsumer = stream
            .create_consumer(consumer::pull::OrderedConfig {
                name: Some("pull_ordered".to_string()),
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
                    context.publish("events", "dat".into()).await.unwrap();
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
                name: "events".into(),
                subjects: vec!["events.>".into()],
                storage: StorageType::File,
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer: OrderedPushConsumer = stream
            .create_consumer(consumer::push::OrderedConfig {
                deliver_subject: "push".into(),
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
                        .publish(format!("events.{i}"), i.to_string().into())
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
                name: "events".into(),
                subjects: vec!["events".into()],
                storage: StorageType::Memory,
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer: OrderedPushConsumer = stream
            .create_consumer(consumer::push::OrderedConfig {
                deliver_subject: "push".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..1000 {
            context
                .publish("events", "dat".into())
                .await
                .unwrap()
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
                name: "events".into(),
                subjects: vec!["events".into()],
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
                deliver_subject: "push".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        for i in 0..1000 {
            context
                .publish("events", format!("{i}").into())
                .await
                .unwrap()
                .await
                .unwrap();
        }

        // disrupt stream sequence continuity.
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
            assert_eq!(message.payload, bytes::Bytes::from(format!("{i}")));
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
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "push".into(),
                durable_name: Some("push".into()),
                flow_control: true,
                idle_heartbeat: Duration::from_millis(100),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer: PushConsumer = stream.get_consumer("push").await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        for _ in 0..1000 {
            context.publish("events", "dat".into()).await.unwrap();
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
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "push".into(),
                durable_name: Some("push".into()),
                idle_heartbeat: Duration::from_millis(100),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer: PushConsumer = stream.get_consumer("push").await.unwrap();
        let mut messages = consumer.messages().await.unwrap().take(1000);

        tokio::time::sleep(Duration::from_secs(1)).await;

        for _ in 0..1000 {
            context.publish("events", "dat".into()).await.unwrap();
        }

        let mut seen = 0;
        while let Some(Ok(message)) = messages.next().await {
            assert_eq!(message.payload.as_ref(), b"dat");
            seen += 1;
        }
        assert_eq!(seen, 1000);

        let consumer = stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "delivery".to_string(),
                durable_name: Some("delete_me".to_string()),
                idle_heartbeat: Duration::from_secs(5),
                ..Default::default()
            })
            .await
            .unwrap();

        stream.delete_consumer("delete_me").await.unwrap();

        let mut messages = consumer.messages().await.unwrap();
        assert_eq!(
            messages.next().await.unwrap().unwrap_err().kind(),
            async_nats::jetstream::consumer::push::MessagesErrorKind::MissingHeartbeat
        );
        stream
            .create_consumer(consumer::push::Config {
                deliver_subject: "delivery".to_string(),
                durable_name: Some("delete_me".to_string()),
                idle_heartbeat: Duration::from_secs(5),
                ..Default::default()
            })
            .await
            .unwrap();

        messages.next().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn pull_stream_default() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..1000 {
                context
                    .publish("events", format!("i: {i}").into())
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
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..100 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let ack = context
                    .publish("events", format!("timeout test message: {i}").into())
                    .await
                    .unwrap();
                println!("ack from publish {i}: {ack:?}");
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
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..10 {
                tokio::time::sleep(Duration::from_millis(600)).await;
                context
                    .publish("events", format!("heartbeat message: {i}").into())
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

    #[cfg(feature = "server_2_10")]
    #[tokio::test]
    async fn update_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .create_stream(stream::Config {
                name: "events".to_string(),
                subjects: vec!["events".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        stream
            .create_consumer(consumer::pull::Config {
                name: Some("CONSUMER".into()),
                filter_subjects: vec!["one".into(), "two".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer = stream
            .create_consumer(consumer::pull::Config {
                name: Some("CONSUMER".into()),
                filter_subjects: vec!["one".into(), "three".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(
            consumer.cached_info().config.filter_subjects,
            vec!["one".to_string(), "three".to_string()]
        );
    }

    #[tokio::test]
    async fn pull_stream_error() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..100 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                context
                    .publish("events", format!("heartbeat message: {i}").into())
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
            .event_callback(|err| async move { println!("error: {err:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        for _ in 0..10 {
            context
                .publish("events", "dat".into())
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let mut iter = consumer.fetch().max_messages(100).messages().await.unwrap();

        let mut i = 0;
        while let Some(message) = iter.next().await {
            let message = message.unwrap();
            message.ack().await.unwrap();
            i += 1;
        }
        assert_eq!(i, 10);
    }

    #[tokio::test]
    async fn pull_batch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client.clone());

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        let consumer = stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ack_policy: AckPolicy::Explicit,
                max_ack_pending: 10000,
                ..Default::default()
            })
            .await
            .unwrap();

        let num_messages = 1000;
        let handle = tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(20));
            for i in 0..=num_messages {
                context
                    .publish("events", i.to_string().into())
                    .await
                    .unwrap()
                    .await
                    .unwrap();
                interval.tick().await;
            }
        });

        let mut received = 0;
        loop {
            let mut iter = consumer
                .batch()
                .expires(Duration::from_millis(200))
                .max_messages(200)
                .messages()
                .await
                .unwrap();
            while let Some(message) = iter.next().await {
                match message {
                    Ok(message) => {
                        if received == num_messages {
                            handle.abort();
                            return;
                        }
                        received += 1;
                        message.ack().await.unwrap()
                    }
                    Err(err) => {
                        assert_eq!(
                            std::io::ErrorKind::TimedOut,
                            err.downcast::<std::io::Error>().unwrap().kind()
                        )
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn pull_consumer_stream_without_heartbeat() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {err:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        context.publish("events", "dat".into()).await.unwrap();

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

    #[cfg(feature = "slow_tests")]
    #[tokio::test]
    async fn pull_consumer_long_idle() {
        use tracing::debug;
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {err:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        // A delayed publish, making sure that until it happens, consumer properly handles idle
        // heartbeats.
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            // Publish something.
            debug!("publishing the message");
            context
                .publish("events", "data".into())
                .await
                .unwrap()
                .await
                .unwrap();
        });
        let mut messages = consumer
            .stream()
            .expires(Duration::from_secs(3))
            .heartbeat(Duration::from_secs(1))
            .messages()
            .await
            .unwrap();
        messages.next().await.unwrap().unwrap();
    }

    #[cfg(feature = "slow_tests")]
    #[tokio::test]
    async fn pull_consumer_stream_with_heartbeat() {
        use tracing::debug;
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {err:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        // Delete the consumer before starting fetching messages.
        let name = &consumer.cached_info().name;
        stream.delete_consumer(name).await.unwrap();
        // Expect Idle Heartbeats to kick in.
        debug!("waiting for the first idle heartbeat timeout");
        let mut messages = consumer.messages().await.unwrap();
        assert_eq!(
            messages.next().await.unwrap().unwrap_err().kind(),
            async_nats::jetstream::consumer::pull::MessagesErrorKind::MissingHeartbeat,
        );
        // But the consumer iterator should still be there.
        // We should get timeout again.
        debug!("waiting for the second idle heartbeat timeout");
        assert_eq!(
            messages.next().await.unwrap().unwrap_err().kind(),
            async_nats::jetstream::consumer::pull::MessagesErrorKind::MissingHeartbeat,
        );
        // Now recreate the consumer and see if we can continue.
        // So recreate the consumer.
        debug!("recreating the consumer");
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        // Publish something.
        debug!("publishing the message");
        context
            .publish("events", "data".into())
            .await
            .unwrap()
            .await
            .unwrap();
        // and expect the message to be there.
        debug!("awaiting the message with recreated consumer");
        let now = Instant::now();
        let m = messages.next().await.unwrap();
        println!("after: {:?}", now.elapsed());
        m.unwrap();
    }

    #[tokio::test]
    async fn pull_consumer_deleted() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {err:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        context.publish("events", "dat".into()).await.unwrap();

        let mut messages = consumer.messages().await.unwrap();

        messages.next().await.unwrap().unwrap().ack().await.unwrap();
        let name = &consumer.cached_info().name;
        stream.delete_consumer(name).await.unwrap();
        assert_eq!(
            messages.next().await.unwrap().unwrap_err().kind(),
            async_nats::jetstream::consumer::pull::MessagesErrorKind::ConsumerDeleted,
        );
        messages.next().await;
        // after terminal error, consumer should always return none.
        assert!(messages.next().await.is_none());
    }

    #[tokio::test]
    async fn consumer_info() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|err| async move { println!("error: {err:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
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
            .event_callback(|err| async move { println!("error: {err:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client.clone());

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut consumer = stream.get_consumer("pull").await.unwrap();

        for _ in 0..10 {
            context
                .publish("events", "dat".into())
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let mut iter = consumer.batch().max_messages(10).messages().await.unwrap();
        client.flush().await.unwrap();

        tryhard::retry_fn(|| async {
            let mut consumer = consumer.clone();
            let num_ack_pending = consumer.info().await?.num_ack_pending;
            if num_ack_pending != 10 {
                return Err(format!("expected {}, got {}", 10, num_ack_pending).into());
            }
            Ok::<(), async_nats::Error>(())
        })
        .retries(10)
        .exponential_backoff(Duration::from_millis(500))
        .await
        .unwrap();

        // standard ack
        if let Some(message) = iter.next().await {
            message.unwrap().ack().await.unwrap();
        }
        client.flush().await.unwrap();

        tryhard::retry_fn(|| async {
            let mut consumer = consumer.clone();
            let num_ack_pending = consumer.info().await?.num_ack_pending;
            if num_ack_pending != 9 {
                return Err(format!("expected {}, got {}", 9, num_ack_pending).into());
            }
            Ok::<(), async_nats::Error>(())
        })
        .retries(10)
        .exponential_backoff(Duration::from_millis(500))
        .await
        .unwrap();

        // double ack
        if let Some(message) = iter.next().await {
            message.unwrap().double_ack().await.unwrap();
        }

        tryhard::retry_fn(|| async {
            let mut consumer = consumer.clone();
            let num_ack_pending = consumer.info().await?.num_ack_pending;
            if num_ack_pending != 8 {
                return Err(format!("expected {}, got {}", 8, num_ack_pending).into());
            }
            Ok::<(), async_nats::Error>(())
        })
        .retries(10)
        .exponential_backoff(Duration::from_millis(500))
        .await
        .unwrap();

        // in progress
        if let Some(message) = iter.next().await {
            message
                .unwrap()
                .ack_with(async_nats::jetstream::AckKind::Nak(None))
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
                println!("EVENT: {event}");
            })
            .connect(server.client_url())
            .await
            .unwrap();

        let jetstream = async_nats::jetstream::new(client.clone());
        // cluster takes some time to spin up.
        // we can have a retry mechanism added later.
        let stream = tryhard::retry_fn(|| {
            jetstream.create_stream(async_nats::jetstream::stream::Config {
                name: "reconnect".into(),
                subjects: vec!["reconnect.>".into()],
                num_replicas: 1,
                ..Default::default()
            })
        })
        .retries(5)
        .exponential_backoff(Duration::from_secs(1))
        .await
        .unwrap();

        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("durable_reconnect".into()),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(5),
                ..Default::default()
            })
            .await
            .unwrap();
        println!("stream and consumer created");

        for i in 0..1000 {
            jetstream
                .publish(format!("reconnect.{i}"), i.to_string().into())
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let messages = consumer
            .stream()
            .expires(Duration::from_secs(30))
            .heartbeat(Duration::from_secs(15))
            .messages()
            .await
            .unwrap();

        let mut messages = messages.enumerate();
        while let Some((i, message)) = messages.next().await {
            let message = message.unwrap();
            let payload = from_utf8(&message.payload).unwrap();
            message.ack().await.unwrap();
            if payload.parse::<usize>().unwrap() == 999 {
                break;
            }
            if i == 200 {
                server.restart();
            }
        }
    }

    #[tokio::test]
    async fn timeout_out_request() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        tokio::time::sleep(Duration::from_secs(5)).await;
        let client = async_nats::ConnectOptions::new()
            .event_callback(|event| async move {
                println!("EVENT: {event}");
            })
            .connect(server.client_url())
            .await
            .unwrap();

        let mut jetstream = async_nats::jetstream::new(client.clone());
        jetstream.set_timeout(Duration::from_millis(500));
        jetstream.create_stream("events").await.unwrap();

        for i in 0..500 {
            jetstream
                .publish("events", format!("{i}").into())
                .await
                .unwrap();
        }
        drop(server);
        assert_eq!(
            jetstream
                .publish("events", "fail".into())
                .await
                .unwrap()
                .await
                .unwrap_err()
                .kind(),
            PublishErrorKind::TimedOut
        );
    }

    #[tokio::test]
    async fn republish() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let _source_stream = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "source".into(),
                max_messages: 1000,
                subjects: vec!["source.>".into()],
                republish: Some(async_nats::jetstream::stream::Republish {
                    source: ">".into(),
                    destination: "dest.>".into(),
                    headers_only: false,
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let destination_stream = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "dest".into(),
                max_messages: 2000,
                subjects: vec!["dest.>".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer = destination_stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("dest".into()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: consumer::AckPolicy::Explicit,
                ..Default::default()
            })
            .await
            .unwrap();
        let mut messages = consumer.messages().await.unwrap().take(100).enumerate();
        for i in 0..100 {
            jetstream
                .publish(format!("source.{i}"), format!("{i}").into())
                .await
                .unwrap();
        }

        while let Some((i, message)) = messages.next().await {
            let message = message.unwrap();
            assert_eq!(format!("dest.source.{i}"), message.subject.as_str());
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
                name: "source".into(),
                max_messages: 10,
                max_messages_per_subject: 2,
                discard_new_per_subject: true,
                subjects: vec!["events.>".into()],
                discard: DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .unwrap();

        jetstream
            .publish("events.1", "data".into())
            .await
            .unwrap()
            .await
            .unwrap();
        jetstream
            .publish("events.1", "data".into())
            .await
            .unwrap()
            .await
            .unwrap();
        jetstream
            .publish("events.1", "data".into())
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
                name: "TEST".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        jetstream.publish("events", "skipped".into()).await.unwrap();
        jetstream.publish("events", "data".into()).await.unwrap();

        let mut mirror = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "MIRROR".into(),
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
                name: Some("consumer".into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .messages()
            .await
            .unwrap();

        assert_eq!(
            from_utf8(&messages.next().await.unwrap().unwrap().message.payload).unwrap(),
            "data"
        )
    }
    #[tokio::test]
    async fn sources() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let stream = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "TEST".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();
        let stream2 = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "TEST2".into(),
                subjects: vec!["events2".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        jetstream.publish("events", "skipped".into()).await.unwrap();
        jetstream.publish("events", "data".into()).await.unwrap();
        jetstream.publish("events2", "data".into()).await.unwrap();
        jetstream.publish("events2", "data".into()).await.unwrap();

        let mut source = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "SOURCE".into(),
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
                name: Some("consumer".into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .messages()
            .await
            .unwrap();

        assert_eq!(
            from_utf8(&messages.next().await.unwrap().unwrap().message.payload).unwrap(),
            "data"
        );
        assert_eq!(
            from_utf8(&messages.next().await.unwrap().unwrap().message.payload).unwrap(),
            "data"
        );
        assert_eq!(
            from_utf8(&messages.next().await.unwrap().unwrap().message.payload).unwrap(),
            "data"
        );
    }

    #[tokio::test]
    #[cfg(feature = "server_2_10")]
    async fn stream_subject_transforms() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let subject_transform = stream::SubjectTransform {
            source: "foo".to_string(),
            destination: "bar".to_string(),
        };

        let source = stream::Source {
            name: "source".to_string(),
            filter_subject: Some("stream1.foo".to_string()),
            ..Default::default()
        };

        let sources = vec![
            source.clone(),
            stream::Source {
                name: "multi_source".to_string(),
                subject_transforms: vec![stream::SubjectTransform {
                    source: "stream2.foo.>".to_string(),
                    destination: "foo.>".to_string(),
                }],
                ..Default::default()
            },
        ];

        let mut stream = context
            .create_stream(stream::Config {
                name: "filtered".to_string(),
                subject_transform: Some(subject_transform.clone()),
                sources: Some(sources.clone()),
                ..Default::default()
            })
            .await
            .unwrap();

        let info = stream.info().await.unwrap();
        assert_eq!(info.config.sources, Some(sources.clone()));
        assert_eq!(info.config.subject_transform, Some(subject_transform));

        let mut stream = context
            .create_stream(stream::Config {
                name: "mirror".to_string(),
                mirror: Some(source.clone()),
                ..Default::default()
            })
            .await
            .unwrap();

        let info = stream.info().await.unwrap();

        assert_eq!(info.config.mirror, Some(source));
    }

    #[tokio::test]
    async fn pull_by_bytes() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context
            .create_stream(stream::Config {
                name: "events".into(),
                subjects: vec!["events".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let stream = context.get_stream("events").await.unwrap();
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("pull".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();

        tokio::task::spawn(async move {
            for i in 0..1000 {
                context
                    .publish(
                        "events",
                        format!("Some bytes to sent with sequence number included: {i}").into(),
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

    #[tokio::test]
    async fn stream_names_list() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        for i in 0..235 {
            context
                .create_stream(async_nats::jetstream::stream::Config {
                    name: i.to_string(),
                    subjects: vec![i.to_string()],
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        assert_eq!(context.stream_names().count().await, 235);
    }

    #[tokio::test]
    async fn streams_list() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        for i in 0..235 {
            context
                .create_stream(async_nats::jetstream::stream::Config {
                    name: i.to_string(),
                    subjects: vec![i.to_string()],
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        assert_eq!(context.streams().count().await, 235);
    }

    #[tokio::test]
    async fn consumer_names_list() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "TEST".into(),
                subjects: vec!["test".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        for i in 0..235 {
            stream
                .create_consumer(async_nats::jetstream::consumer::pull::Config {
                    name: Some(format!("consumer_{i}")),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        let consumers = stream
            .consumer_names()
            .try_collect::<Vec<String>>()
            .await
            .unwrap();

        assert_eq!(consumers.len(), 235);

        for i in 0..235 {
            assert!(consumers
                .iter()
                .any(|name| *name == format!("consumer_{i}")));
        }
        assert_eq!(stream.consumer_names().count().await, 235);
    }

    #[tokio::test]
    async fn consumers() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "TEST".into(),
                subjects: vec!["test".into()],
                ..Default::default()
            })
            .await
            .unwrap();

        for i in 0..1200 {
            stream
                .create_consumer(async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(format!("consumer_{i}")),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        let consumers = stream.consumers().try_collect::<Vec<Info>>().await.unwrap();

        assert_eq!(consumers.len(), 1200);

        for i in 0..1200 {
            assert!(consumers
                .iter()
                .any(|name| *name.name == format!("consumer_{i}")));
        }
        assert_eq!(stream.consumer_names().count().await, 1200);
    }

    #[tokio::test]
    async fn queue_push_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client.clone());

        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                subjects: vec!["filter".into()],
                name: "filter".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        for i in 0..50 {
            context
                .publish("filter", format!("{i}").into())
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::push::Config {
                deliver_group: Some("group".into()),
                durable_name: Some("group".into()),
                deliver_subject: client.new_inbox(),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer2: PushConsumer = stream.get_consumer("group").await.unwrap();

        let mut messages = consumer.messages().await.unwrap().take(5);
        let mut messages2 = consumer2.messages().await.unwrap().take(5);

        while let Some(message) = messages.next().await {
            let message = message.unwrap();
            message.ack().await.unwrap();
        }

        while let Some(message) = messages2.next().await {
            let message = message.unwrap();
            message.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn publish_no_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client.clone());
        assert_eq!(
            context
                .publish("test", "jghf".into())
                .await
                .unwrap()
                .await
                .unwrap_err()
                .kind(),
            PublishErrorKind::StreamNotFound
        );
    }

    #[cfg(feature = "server_2_10")]
    #[tokio::test]
    async fn multiple_filters_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client.clone());

        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                subjects: vec!["events".into(), "data".into(), "other".into()],
                name: "filter".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .publish("events", "0".into())
            .await
            .unwrap()
            .await
            .unwrap();
        context
            .publish("other", "100".into())
            .await
            .unwrap()
            .await
            .unwrap();
        context
            .publish("data", "1".into())
            .await
            .unwrap()
            .await
            .unwrap();
        context
            .publish("events", "2".into())
            .await
            .unwrap()
            .await
            .unwrap();

        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::push::Config {
                filter_subjects: vec!["events".into(), "data".into()],
                durable_name: Some("group".into()),
                deliver_subject: client.new_inbox(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut stream = consumer.messages().await.unwrap().take(3).enumerate();

        while let Some((i, message)) = stream.next().await {
            let message = message.unwrap();
            assert_eq!(from_utf8(&message.payload).unwrap(), i.to_string());
        }
    }

    #[cfg(feature = "server_2_10")]
    #[tokio::test]
    async fn metadata() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client.clone());

        let metadata = HashMap::from([
            ("key".into(), "value".into()),
            ("other".into(), "value".into()),
        ]);

        let mut stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                subjects: vec!["events".into()],
                name: "filter".into(),
                metadata: metadata.clone(),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(stream.info().await.unwrap().config.metadata, metadata);

        let mut consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                name: Some("consumer".into()),
                metadata: metadata.clone(),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(consumer.info().await.unwrap().config.metadata, metadata);
    }

    #[tokio::test]
    async fn backoff() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client.clone());

        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "stream".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut consumer = stream
            .create_consumer(async_nats::jetstream::consumer::Config {
                max_deliver: 10,
                backoff: vec![
                    std::time::Duration::from_secs(1),
                    std::time::Duration::from_secs(5),
                ],
                ..Default::default()
            })
            .await
            .unwrap();

        let info = consumer.info().await.unwrap();

        assert_eq!(info.config.backoff[0], Duration::from_secs(1));
        assert_eq!(info.config.backoff[1], Duration::from_secs(5));
    }

    #[tokio::test]
    async fn nak_with_delay() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client.clone());

        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "stream".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::push::Config {
                deliver_subject: "deliver".into(),
                max_deliver: 30,
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(5),
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .publish("stream", "data".into())
            .await
            .unwrap()
            .await
            .unwrap();

        let mut messages = consumer.messages().await.unwrap();
        let message = messages.next().await.unwrap().unwrap();

        // Send NAK with much shorter duration.
        message
            .ack_with(AckKind::Nak(Some(Duration::from_millis(1000))))
            .await
            .unwrap();

        // Check if we get a redelivery in that shortened duration.
        let message = tokio::time::timeout(Duration::from_secs(3), messages.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        // Send NAK with duration longer than `ack_wait` set on the consumer.
        message
            .ack_with(AckKind::Nak(Some(Duration::from_secs(10))))
            .await
            .unwrap();

        // Expect it to timeout, but in time longer than consumer `ack_wait`.
        tokio::time::timeout(Duration::from_secs(7), messages.next())
            .await
            .unwrap_err();
    }

    #[cfg(feature = "server_2_10")]
    #[tokio::test]
    async fn subject_transform() {
        use async_nats::jetstream::stream::SubjectTransform;

        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client.clone());

        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "origin".into(),
                subjects: vec!["test".into()],
                subject_transform: Some(async_nats::jetstream::stream::SubjectTransform {
                    source: ">".into(),
                    destination: "transformed.>".into(),
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .publish("test", "data".into())
            .await
            .unwrap()
            .await
            .unwrap();

        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "sourcing".into(),
                sources: Some(vec![async_nats::jetstream::stream::Source {
                    name: "origin".into(),
                    subject_transforms: vec![SubjectTransform {
                        source: ">".into(),
                        destination: "fromtest.>".into(),
                    }],
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                subjects: vec!["fromtest.>".into()],
                name: "events".into(),

                ..Default::default()
            })
            .await
            .unwrap();

        // ephemeral consumer
        let consumer = stream
            .get_or_create_consumer(
                "consumer",
                async_nats::jetstream::consumer::pull::Config {
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut messages = consumer.messages().await.unwrap().take(1000);
        let message = messages.next().await.unwrap().unwrap();

        assert_eq!(message.subject.as_str(), "fromtest.transformed.test");
    }

    #[tokio::test]
    async fn acker() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "origin".to_string(),
                subjects: vec!["test".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..10 {
            context
                .publish("test", "data".into())
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                name: Some("consumer".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut messages = consumer.messages().await.unwrap().take(10);

        while let Some((_, acker)) = messages
            .try_next()
            .await
            .unwrap()
            .map(|message| message.split())
        {
            acker.ack().await.unwrap();
        }
    }

    // This test was added to make sure that in case of slow consumers client can properly recreate
    // ordered consumer.
    #[tokio::test]
    async fn ordered_recreate() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::ConnectOptions::new()
            .read_buffer_capacity(1000)
            .subscription_capacity(1000)
            .connect(server.client_url())
            .await
            .unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                name: "origin".to_string(),
                subjects: vec!["test".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        debug!("start publishing");
        for _ in 0..5000 {
            context.publish("test", "data".into()).await.unwrap();
        }
        debug!("finished publishing");

        let mut messages = stream
            .create_consumer(push::OrderedConfig {
                deliver_subject: "deliver".to_string(),
                deliver_policy: DeliverPolicy::All,
                replay_policy: ReplayPolicy::Instant,
                ..Default::default()
            })
            .await
            .unwrap()
            .messages()
            .await
            .unwrap()
            .take(5000);

        tokio::time::timeout(Duration::from_secs(30), async move {
            while let Some(message) = messages.next().await {
                message.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[cfg(feature = "server_2_10")]
    #[tokio::test]
    async fn stream_config() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let config = async_nats::jetstream::stream::Config {
            name: "EVENTS".to_string(),
            max_bytes: 1024 * 1024,
            max_messages: 1_000_000,
            max_messages_per_subject: 100,
            discard: DiscardPolicy::New,
            discard_new_per_subject: true,
            subjects: vec!["events.>".to_string()],
            retention: stream::RetentionPolicy::WorkQueue,
            max_consumers: 10,
            max_age: Duration::from_secs(900),
            max_message_size: 1024 * 1024,
            storage: StorageType::Memory,
            num_replicas: 1,
            no_ack: true,
            duplicate_window: Duration::from_secs(90),
            template_owner: "".to_string(),
            sealed: false,
            description: Some("A Stream".to_string()),
            allow_rollup: true,
            deny_delete: false,
            deny_purge: false,
            republish: Some(stream::Republish {
                source: "data.>".to_string(),
                destination: "dest.>".to_string(),
                headers_only: true,
            }),
            allow_direct: true,
            mirror_direct: false,
            mirror: None,
            sources: Some(vec![Source {
                name: "source_one_of_many".to_string(),
                start_sequence: Some(5),
                start_time: Some(OffsetDateTime::now_utc()),
                filter_subject: Some("filter".to_string()),
                external: Some(stream::External {
                    api_prefix: "API.PREFIX".to_string(),
                    delivery_prefix: Some("delivery_prefix".to_string()),
                }),
                domain: None,
                subject_transforms: vec![SubjectTransform {
                    source: "source".to_string(),
                    destination: "dest".to_string(),
                }],
            }]),
            metadata: HashMap::from([("key".to_string(), "value".to_string())]),
            subject_transform: Some(SubjectTransform {
                source: "source".to_string(),
                destination: "dest".to_string(),
            }),
            compression: Some(Compression::S2),
            consumer_limits: Some(ConsumerLimits {
                inactive_threshold: Duration::from_secs(120),
                max_ack_pending: 150,
            }),
            first_sequence: Some(505),
            placement: Some(stream::Placement {
                cluster: Some("CLUSTER".to_string()),
                tags: vec!["tag".to_string()],
            }),
        };

        let mut stream = jetstream.create_stream(config.clone()).await.unwrap();
        assert_eq!(config, stream.info().await.unwrap().config);
    }

    #[tokio::test]
    async fn limits() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let stream = jetstream
            .create_stream(stream::Config {
                name: "events".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        stream
            .create_consumer(async_nats::jetstream::consumer::Config {
                durable_name: Some("name".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut config = stream.cached_info().config.clone();
        config.consumer_limits = Some(ConsumerLimits {
            inactive_threshold: Duration::from_secs(2),
            max_ack_pending: 10,
        });

        jetstream
            .update_stream(config)
            .await
            .expect_err("cannot update stream. consumer `name` exceeds new limits");
    }

    #[tokio::test]
    async fn consumer_create_strict() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let stream = jetstream
            .create_stream(stream::Config {
                name: "events".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // crate a consumer
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("name".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        // strict create with the same config should be ok.
        stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("name".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        // strict create with different config should fail.
        let err = stream
            .create_consumer_strict(consumer::pull::Config {
                durable_name: Some("name".to_string()),
                ack_policy: AckPolicy::All,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ConsumerCreateStrictErrorKind::AlreadyExists);
    }

    #[tokio::test]
    async fn consumer_update() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let stream = jetstream
            .create_stream(stream::Config {
                name: "events".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // crate a consumer
        let consumer = stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("name".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut config = consumer.cached_info().config.clone();
        config.description = Some("new description".to_string());

        // update existing consumer
        stream.update_consumer(config).await.unwrap();

        // update non-existing consumer
        let err = stream
            .update_consumer(consumer::pull::Config {
                durable_name: Some("non-existing".to_string()),
                ..Default::default()
            })
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ConsumerUpdateErrorKind::DoesNotExist);
    }

    #[tokio::test]
    async fn test_version_on_initial_connect() {
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("nats://localhost:4222")
            .await
            .unwrap();
        let jetstream = async_nats::jetstream::new(client.clone());

        jetstream
            .create_consumer_on_stream(
                consumer::pull::Config {
                    durable_name: Some("name".to_string()),
                    ..Default::default()
                },
                "events",
            )
            .await
            .expect_err("should fail but not panic because of lack of server info");
    }
}
