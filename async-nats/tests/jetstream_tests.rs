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

    use super::*;
    use async_nats::header::HeaderMap;
    use async_nats::jetstream::consumer::pull::{BatchConfig, Config};
    use async_nats::jetstream::consumer::{self, DeliverPolicy, PullConsumer, PushConsumer};
    use async_nats::jetstream::response::Response;
    use async_nats::jetstream::stream;
    use async_nats::ConnectOptions;
    use bytes::Bytes;
    use futures::stream::{StreamExt, TryStreamExt};
    use time::OffsetDateTime;

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
            .unwrap();

        assert_eq!(ack.stream, "TEST");
        assert_eq!(ack.sequence, 1);
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

    #[tokio::test]
    async fn get_stream() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        context.create_stream("events").await.unwrap();
        assert_eq!(
            context.get_stream("events").await.unwrap().info.config.name,
            "events".to_string()
        );
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
                .info
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
                .info
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
    async fn create_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        // durable consumer
        context
            .get_or_create_stream("events")
            .await
            .unwrap()
            .create_consumer(Config {
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
            .create_consumer(&Config {
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
            .create_consumer(&consumer::pull::Config {
                durable_name: Some("pull_explicit".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn delete_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("events").await.unwrap();
        stream
            .create_consumer(Config {
                durable_name: Some("consumer".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        stream.delete_consumer("consumer").await.unwrap();
        assert!(stream.get_consumer::<Config>("consumer").await.is_err());
    }

    #[tokio::test]
    async fn get_consumer() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("stream").await.unwrap();
        stream
            .create_consumer(Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        stream
            .create_consumer(&consumer::push::Config {
                durable_name: Some("push".to_string()),
                deliver_subject: Some("subject".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer: PullConsumer = stream.get_consumer("pull").await.unwrap();
        let _consumer: PushConsumer = stream.get_consumer("push").await.unwrap();

        let consumer = stream.get_consumer("pull").await.unwrap();
        consumer.fetch(10).await.unwrap();
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
        stream.get_consumer::<Config>("consumer").await.unwrap();

        // bind to previously created consumer.
        stream
            .get_or_create_consumer::<Config>(
                "consumer",
                Config {
                    durable_name: Some("consumer".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn request_batch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);

        let stream = context.get_or_create_stream("stream").await.unwrap();
        stream
            .create_consumer(&Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer = stream.get_consumer("pull").await.unwrap();
        consumer
            .request_batch(
                BatchConfig {
                    batch: 10,
                    expires: None,
                    no_wait: false,
                    ..Default::default()
                },
                "inbox".to_string(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pull_process() {
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
            .create_consumer(&Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer = stream.get_consumer("pull").await.unwrap();

        for _ in 0..1000 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut iter = consumer.process(50).unwrap().take(10);
        while let Ok(Some(mut batch)) = iter.try_next().await {
            while let Ok(Some(message)) = batch.try_next().await {
                assert_eq!(message.payload, Bytes::from(b"dat".as_ref()));
            }
        }
    }

    #[tokio::test]
    async fn pull_fetch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .error_callback(|err| async move { println!("error: {:?}", err) })
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
            .create_consumer(&Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer = stream.get_consumer("pull").await.unwrap();

        for _ in 0..10 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut iter = consumer.fetch(100).await.unwrap();

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
            .error_callback(|err| async move { println!("error: {:?}", err) })
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
            .create_consumer(&Config {
                durable_name: Some("pull".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer = stream.get_consumer("pull").await.unwrap();

        for _ in 0..100 {
            context
                .publish("events".to_string(), "dat".into())
                .await
                .unwrap();
        }

        let mut iter = consumer.batch(100, Some(10000000000)).await.unwrap();

        let mut i = 0;
        while (iter.next().await).is_some() {
            i += 1;
            if i >= 100 {
                return;
            }
        }
    }
}
