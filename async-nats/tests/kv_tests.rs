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

#[cfg(feature = "kv")]
mod kv {
    use std::{collections::HashMap, error::Error, str::from_utf8, time::Duration};

    use async_nats::{
        jetstream::{
            context::UpdateKeyValueErrorKind,
            kv::{self, Operation},
            stream::{self, DiscardPolicy, External, Republish, Source, StorageType},
        },
        ConnectOptions,
    };
    use bytes::Bytes;
    use futures_util::{StreamExt, TryStreamExt};

    #[tokio::test]
    async fn create_bucket() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let mut kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let info = kv.stream.info().await.unwrap();
        assert_eq!("KV_test", kv.stream_name);
        assert_eq!(info.config.discard, DiscardPolicy::New);
        assert!(info.config.allow_direct);
    }

    #[tokio::test]
    async fn create() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload: Bytes = "data".into();
        let create = kv.create("key", payload.clone()).await;
        assert!(create.is_ok());

        let create = kv.create("key", payload.clone()).await;
        assert!(create.is_err());
        assert_eq!(
            create.as_ref().unwrap_err().kind(),
            async_nats::jetstream::kv::CreateErrorKind::AlreadyExists
        );

        kv.delete("key").await.unwrap();
        let create = kv.create("key", payload.clone()).await;
        assert!(create.is_ok());

        kv.purge("key").await.unwrap();
        let create = kv.create("key", payload.clone()).await;
        assert!(create.is_ok());
    }

    #[tokio::test]
    async fn create_with_failure() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let payload: Bytes = "data".into();

        let create = kv.create("", payload.clone()).await;
        assert!(create.is_err());
        assert_eq!(
            create.as_ref().unwrap_err().kind(),
            async_nats::jetstream::kv::CreateErrorKind::InvalidKey
        );
        // Verify the source error contains the expected message
        let source = create.as_ref().unwrap_err().source().unwrap();
        assert!(source.to_string().contains("key cannot be empty"));
    }

    #[tokio::test]
    async fn put() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let payload: Bytes = "data".into();
        kv.put("key", payload.clone()).await.unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), payload);
        let payload: Bytes = "data2".into();
        kv.put("key", payload.clone()).await.unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), payload);
    }

    #[tokio::test]
    async fn get() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let payload: Bytes = "data".into();
        kv.put("key", payload.clone()).await.unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), payload);
        let payload: Bytes = "data2".into();
        kv.put("key", payload.clone()).await.unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), payload);
        let nothing = kv.get("nothing").await.unwrap();
        assert_eq!(None, nothing);
    }

    #[tokio::test]
    async fn entry() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let mut kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let payload: Bytes = "data".into();
        kv.put("key", payload.clone()).await.unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), payload);
        let payload: Bytes = "data2".into();
        kv.put("key", payload.clone()).await.unwrap();
        let value = kv.entry("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap().value).unwrap(), payload);
        let nothing = kv.get("nothing").await.unwrap();
        assert_eq!(None, nothing);

        let value = kv.entry_for_revision("key", 1).await.unwrap();
        assert_eq!(from_utf8(&value.unwrap().value).unwrap(), "data");

        context
            .update_stream(async_nats::jetstream::stream::Config {
                max_messages_per_subject: 10,
                name: "KV_test".into(),
                subjects: vec!["$KV.test.>".into()],
                deny_delete: true,
                allow_direct: false,
                ..Default::default()
            })
            .await
            .unwrap();

        let info = kv.stream.info().await.unwrap();
        assert!(!info.config.allow_direct);

        let nothing = kv.get("nothing").await.unwrap();
        assert_eq!(None, nothing);
        let value = kv.entry("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap().value).unwrap(), payload);

        let value = kv.entry_for_revision("key", 1).await.unwrap();
        assert_eq!(from_utf8(&value.unwrap().value).unwrap(), "data");

        let value = kv.entry_for_revision("key", 250).await.unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn update() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let payload: Bytes = "data".into();
        let rev = kv.put("key", payload.clone()).await.unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), payload);
        let updated_payload: Bytes = "updated".into();
        let rev2 = kv
            .update("key", updated_payload.clone(), rev)
            .await
            .unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(rev2, rev + 1);
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), updated_payload);
    }

    #[tokio::test]
    async fn delete() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "delete".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let payload: Bytes = "data".into();
        kv.put("key", payload.clone()).await.unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), payload);
        kv.delete("key").await.unwrap();
        let ss = kv.get("kv").await.unwrap();
        assert!(ss.is_none());

        let mut entries = kv.history("key").await.unwrap();

        let first_op = entries.next().await;
        assert_eq!(first_op.unwrap().unwrap().operation, Operation::Put);

        let first_op = entries.next().await;
        assert_eq!(first_op.unwrap().unwrap().operation, Operation::Delete);
    }

    #[tokio::test]
    async fn delete_expect_revision() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "delete".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let payload: Bytes = "data".into();
        let revision = kv.put("key", payload.clone()).await.unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), payload);

        let wrong_revision = 3;
        let failed = kv
            .delete_expect_revision("key", Some(wrong_revision))
            .await
            .is_err();
        assert!(failed);

        kv.delete_expect_revision("key", Some(revision))
            .await
            .unwrap();
        let ss = kv.get("kv").await.unwrap();
        assert!(ss.is_none());

        let mut entries = kv.history("key").await.unwrap();

        let first_op = entries.next().await;
        assert_eq!(first_op.unwrap().unwrap().operation, Operation::Put);

        let first_op = entries.next().await;
        assert_eq!(first_op.unwrap().unwrap().operation, Operation::Delete);
    }

    #[tokio::test]
    async fn purge() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "purge".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        kv.put("dz", "0".into()).await.unwrap();
        kv.put("dz", "1".into()).await.unwrap();
        kv.put("dz", "2".into()).await.unwrap();
        kv.put("dz", "3".into()).await.unwrap();
        kv.put("dz", "4".into()).await.unwrap();
        kv.put("dz", "5".into()).await.unwrap();

        kv.put("baz", "0".into()).await.unwrap();
        kv.put("baz", "1".into()).await.unwrap();
        kv.put("baz", "2".into()).await.unwrap();

        let history = kv.history("dz").await.unwrap().count().await;
        assert_eq!(history, 6);
        kv.purge("dz").await.unwrap();
        let history = kv.history("dz").await.unwrap().count().await;
        assert_eq!(history, 1);
    }

    #[tokio::test]
    async fn purge_expect_revision() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "purge".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        kv.put("dz", "0".into()).await.unwrap();
        kv.put("dz", "1".into()).await.unwrap();
        kv.put("dz", "2".into()).await.unwrap();
        kv.put("dz", "3".into()).await.unwrap();
        kv.put("dz", "4".into()).await.unwrap();
        let revision = kv.put("dz", "5".into()).await.unwrap();

        kv.put("baz", "0".into()).await.unwrap();
        kv.put("baz", "1".into()).await.unwrap();
        kv.put("baz", "2".into()).await.unwrap();

        let history = kv.history("dz").await.unwrap().count().await;
        assert_eq!(history, 6);

        let wrong_revision = 3;
        let failed = kv
            .purge_expect_revision("dz", Some(wrong_revision))
            .await
            .is_err();
        assert!(failed);

        kv.purge_expect_revision("dz", Some(revision))
            .await
            .unwrap();
        let history = kv.history("dz").await.unwrap().count().await;
        assert_eq!(history, 1);
    }

    #[tokio::test]
    async fn history() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        for i in 0..20 {
            kv.put("key", format!("{i}").into()).await.unwrap();
        }

        let mut history = kv.history("key").await.unwrap().enumerate();

        while let Some((i, entry)) = history.next().await {
            let entry = entry.unwrap();
            assert_eq!(
                i + 5,
                from_utf8(&entry.value).unwrap().parse::<usize>().unwrap()
            );
            assert_eq!(i + 6, entry.revision as usize);
        }
    }

    #[tokio::test]
    async fn watch_hearbeats_first() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client.clone());

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".to_string(),
                description: "test_description".to_string(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // check if we get only updated values. This should not pop up in watcher.
        kv.put("foo", 22.to_string().into()).await.unwrap();
        let mut watch = kv.watch("foo").await.unwrap().enumerate();
        client.flush().await.unwrap();

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    kv.put("foo", i.to_string().into()).await.unwrap();
                }
            }
        });

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_secs(7)).await;
                    kv.put("var", i.to_string().into()).await.unwrap();
                }
            }
        });
        while let Some((i, entry)) = watch.next().await {
            let entry = entry.unwrap();
            println!("ENTRY: {:?}", from_utf8(&entry.value).unwrap());
            assert_eq!(entry.key, "foo".to_string());
            assert_eq!(
                i,
                from_utf8(&entry.value).unwrap().parse::<usize>().unwrap()
            );
            if i == 9 {
                break;
            }
        }
    }

    #[tokio::test]
    async fn watch_no_messages() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let context = async_nats::jetstream::new(client);
        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".to_string(),
                description: "test_description".to_string(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // Add delayed message.
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let kv = context.get_key_value("history").await.unwrap();
            kv.put("foo", 22.to_string().into()).await.unwrap();
        });

        let mut watcher = kv.watch_with_history("foo").await.unwrap();
        let key = watcher.next().await.unwrap().unwrap();
        // Check if the delayed message has proper value for `seen_current`.
        assert!(key.seen_current);
    }

    #[tokio::test]
    async fn watch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // check if we get only updated values. This should not pop up in watcher.
        kv.put("foo", 22.to_string().into()).await.unwrap();
        let mut watch = kv.watch("foo").await.unwrap().enumerate();

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put("foo", i.to_string().into()).await.unwrap();
                }
            }
        });

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put("var", i.to_string().into()).await.unwrap();
                }
            }
        });
        while let Some((i, entry)) = watch.next().await {
            let entry = entry.unwrap();
            assert_eq!(entry.key, "foo");
            assert_eq!(
                i,
                from_utf8(&entry.value).unwrap().parse::<usize>().unwrap()
            );
            if i == 9 {
                break;
            }
        }
    }
    #[tokio::test]
    #[cfg(feature = "server_2_10")]
    async fn watch_many() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // check if we get only updated values. This should not pop up in watcher.
        kv.put("foo", 22.to_string().into()).await.unwrap();
        kv.put("bar", 22.to_string().into()).await.unwrap();
        let mut watch = kv.watch_many(["foo.>", "bar.>"]).await.unwrap();

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put(format!("foo.{i}"), i.to_string().into())
                        .await
                        .unwrap();
                }
            }
        });
        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put(format!("bar.{i}"), i.to_string().into())
                        .await
                        .unwrap();
                }
            }
        });

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put("var", i.to_string().into()).await.unwrap();
                }
            }
        });

        let mut keys = HashMap::new();
        for i in 0..10 {
            keys.insert(format!("foo.{i}"), ());
            keys.insert(format!("bar.{i}"), ());
        }
        while let Some(entry) = watch.next().await {
            let entry = entry.unwrap();
            assert!(keys.contains_key(&entry.key));
            keys.remove(&entry.key);
            if keys.is_empty() {
                break;
            }
        }
    }

    #[tokio::test]
    async fn watch_seen_current() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        for i in 0..10 {
            kv.put(format!("key.{i}"), i.to_string().into())
                .await
                .unwrap();
        }

        let mut watcher = kv
            .watch_with_history("key.>")
            .await
            .unwrap()
            .enumerate()
            .take(20);

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 10..20 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put(format!("key.{i}"), i.to_string().into())
                        .await
                        .unwrap();
                }
            }
        });

        while let Some((i, entry)) = watcher.next().await {
            let entry = entry.unwrap();
            assert_eq!(entry.key, format!("key.{i}"));
            assert_eq!(
                i,
                from_utf8(&entry.value).unwrap().parse::<usize>().unwrap()
            );
            if i >= 9 {
                assert!(entry.seen_current);
            } else {
                assert!(!entry.seen_current);
            }
        }
    }

    #[tokio::test]
    async fn watch_with_history() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // check if we get updated values. This should not pop up in watcher.
        kv.put("foo.bar", 42.to_string().into()).await.unwrap();
        let mut watch = kv.watch_with_history("foo.>").await.unwrap().enumerate();

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put(format!("foo.{i}"), i.to_string().into())
                        .await
                        .unwrap();
                }
            }
        });

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put("var", i.to_string().into()).await.unwrap();
                }
            }
        });

        // check to see if we get the values in accordance to the LastPerSubject deliver policy
        // we should get `foo.bar` as well
        let (_, entry) = watch.next().await.unwrap();
        let entry = entry.unwrap();
        assert_eq!("foo.bar", entry.key);
        assert_eq!(
            42,
            from_utf8(&entry.value).unwrap().parse::<usize>().unwrap()
        );

        // make sure we get the rest correctly
        while let Some((i, entry)) = watch.next().await {
            let entry = entry.unwrap();
            // we now start at 1, we've done one iteration
            let i = i - 1;
            assert_eq!(entry.key, format!("foo.{i}"));
            assert_eq!(
                i,
                from_utf8(&entry.value).unwrap().parse::<usize>().unwrap()
            );
            if i == 9 {
                break;
            }
        }
    }

    #[tokio::test]
    #[cfg(feature = "server_2_10")]
    async fn watch_many_with_history() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // check if we get updated values. This should not pop up in watcher.
        kv.put("foo.bar", 42.to_string().into()).await.unwrap();
        let mut watch = kv
            .watch_many_with_history(["foo.>", "bar.>"])
            .await
            .unwrap();

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put(format!("foo.{i}"), i.to_string().into())
                        .await
                        .unwrap();
                }
            }
        });

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put("var", i.to_string().into()).await.unwrap();
                }
            }
        });

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put(format!("bar.{i}"), i.to_string().into())
                        .await
                        .unwrap();
                }
            }
        });

        let entry = watch.next().await.unwrap();
        let entry = entry.unwrap();
        assert_eq!("foo.bar", entry.key);

        let mut keys = HashMap::new();
        for i in 0..10 {
            keys.insert(format!("foo.{i}"), ());
            keys.insert(format!("bar.{i}"), ());
        }

        // make sure we get the rest correctly
        while let Some(entry) = watch.next().await {
            let entry = entry.unwrap();
            // we now start at 1, we've done one iteration
            assert!(keys.contains_key(&entry.key));
            keys.remove(&entry.key);
            if keys.is_empty() {
                break;
            }
        }
    }

    #[tokio::test]
    async fn watch_all() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut watch = kv.watch_all().await.unwrap().enumerate();
        tokio::time::sleep(Duration::from_millis(100)).await;

        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put("bar", i.to_string().into()).await.unwrap();
                }
            }
        });
        tokio::task::spawn({
            let kv = kv.clone();
            async move {
                for i in 0..10 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    kv.put("foo", i.to_string().into()).await.unwrap();
                }
            }
        });

        while let Some((i, entry)) = watch.next().await {
            let entry = entry.unwrap();
            assert_eq!(i + 1, entry.revision as usize);
            if i == 19 {
                break;
            }
        }
    }

    #[tokio::test]
    async fn watch_from_revision() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "bucket".into(),
                description: "test_description".into(),
                history: 15,
                ..Default::default()
            })
            .await
            .unwrap();

        kv.put("foo", "bar".into()).await.unwrap();
        kv.put("foo", "baz".into()).await.unwrap();
        kv.put("bar", "foo".into()).await.unwrap();
        kv.put("bar", "bar".into()).await.unwrap();
        kv.put("baz", "foo".into()).await.unwrap();

        let mut watch = kv.watch_from_revision("foo", 2).await.unwrap().take(1);
        let key = watch.next().await.unwrap().unwrap();
        assert_eq!(key.key, "foo");
        assert_eq!(key.value, "baz".as_bytes());
        assert_eq!(key.revision, 2);

        let mut watch = kv.watch_all_from_revision(3).await.unwrap().take(3);
        let key = watch.next().await.unwrap().unwrap();
        assert_eq!(key.key, "bar");
        assert_eq!(key.value, "foo".as_bytes());
        assert_eq!(key.revision, 3);
        let key = watch.next().await.unwrap().unwrap();
        assert_eq!(key.key, "bar");
        assert_eq!(key.value, "bar".as_bytes());
        let key = watch.next().await.unwrap().unwrap();
        assert_eq!(key.key, "baz");
        assert_eq!(key.value, "foo".as_bytes());
    }

    #[tokio::test]
    async fn keys() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        for i in 0..10 {
            kv.put("bar", i.to_string().into()).await.unwrap();
        }
        for i in 0..10 {
            kv.put("foo", i.to_string().into()).await.unwrap();
        }

        let mut keys = kv
            .keys()
            .await
            .unwrap()
            .try_collect::<Vec<String>>()
            .await
            .unwrap();
        keys.sort();
        assert_eq!(vec!["bar", "foo"], keys);

        #[cfg(feature = "server_2_10")]
        {
            let mut keys_with_filter = kv
                .keys_with_filters(vec!["bar"])
                .await
                .unwrap()
                .try_collect::<Vec<String>>()
                .await
                .unwrap();
            keys_with_filter.sort();
            assert_eq!(vec!["bar"], keys_with_filter);

            kv.put("foo1.bar", 37.to_string().into()).await.unwrap();
            kv.put("foo1.baz.boo", 73.to_string().into()).await.unwrap();
            kv.put("foo1.baz.baz", 89.to_string().into()).await.unwrap();

            let mut keys_with_filters = kv
                .keys_with_filters(vec!["foo", "bar"])
                .await
                .unwrap()
                .try_collect::<Vec<String>>()
                .await
                .unwrap();
            keys_with_filters.sort();
            assert_eq!(vec!["bar", "foo"], keys_with_filters);

            let mut keys_with_filters = kv
                .keys_with_filters(vec!["foo1.*.*"])
                .await
                .unwrap()
                .try_collect::<Vec<String>>()
                .await
                .unwrap();
            keys_with_filters.sort();
            assert_eq!(vec!["foo1.baz.baz", "foo1.baz.boo"], keys_with_filters);

            let mut keys_with_filters = kv
                .keys_with_filters(vec!["foo1.*.*", "foo1.*"])
                .await
                .unwrap()
                .try_collect::<Vec<String>>()
                .await
                .unwrap();
            keys_with_filters.sort();
            assert_eq!(
                vec!["foo1.bar", "foo1.baz.baz", "foo1.baz.boo"],
                keys_with_filters
            );

            let mut keys_with_filters = kv
                .keys_with_filters(vec!["*.baz.*"])
                .await
                .unwrap()
                .try_collect::<Vec<String>>()
                .await
                .unwrap();

            keys_with_filters.sort();
            assert_eq!(vec!["foo1.baz.baz", "foo1.baz.boo"], keys_with_filters);

            // cleanup the keys
            kv.delete("foo1.bar").await.unwrap();
            kv.delete("foo1.baz.boo").await.unwrap();
            kv.delete("foo1.baz.baz").await.unwrap();
        }
        // filters like "foo.b*" should not return anything because it's not a valid filter

        // Delete a key and make sure it doesn't show up in the keys list
        kv.delete("bar").await.unwrap();
        let keys = kv
            .keys()
            .await
            .unwrap()
            .try_collect::<Vec<String>>()
            .await
            .unwrap();
        assert_eq!(vec!["foo"], keys, "Deleted key shouldn't appear in list");

        // Put the key back, and then purge and make sure the key doesn't show up
        for i in 0..10 {
            kv.put("bar", i.to_string().into()).await.unwrap();
        }
        kv.purge("foo").await.unwrap();
        let keys = kv
            .keys()
            .await
            .unwrap()
            .try_collect::<Vec<String>>()
            .await
            .unwrap();
        assert_eq!(vec!["bar"], keys, "Purged key shouldn't appear in the list");

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history2".into(),
                description: "test_description".into(),
                history: 15,
                max_age: Duration::from_millis(100),
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        kv.put("baz", "value".into()).await.unwrap();
        tryhard::retry_fn(|| async {
            match kv.keys().await {
                Ok(keys) => {
                    let keys = keys.try_collect::<Vec<String>>().await.unwrap();
                    if !keys.is_empty() {
                        return Err("keys not empty".into());
                    }
                    Ok::<(), async_nats::Error>(())
                }
                Err(e) => Err(e.into()),
            }
        })
        .retries(5)
        .exponential_backoff(Duration::from_millis(100))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn republish() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {event:?}") })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client.clone());

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".into(),
                description: "test_description".into(),
                history: 10,
                storage: StorageType::File,
                num_replicas: 1,
                republish: Some(Republish {
                    source: ">".into(),
                    destination: "bar.>".into(),
                    headers_only: false,
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut subscribe = client.subscribe("bar.>").await.unwrap();

        kv.put("key", "data".into()).await.unwrap();

        let message = subscribe.next().await.unwrap();
        assert_eq!("bar.$KV.test.key", message.subject.as_str());
    }

    // This test if flaky due to its assumptions.
    // It is set to ignored until those are resolved.
    #[ignore]
    #[tokio::test]
    async fn cross_account_mirrors() {
        let hub_server = nats_server::run_server("tests/configs/jetstream_hub.conf");
        let leaf_server = nats_server::run_server("tests/configs/jetstream_leaf.conf");

        let hub = async_nats::connect(hub_server.client_url()).await.unwrap();
        let leaf = async_nats::connect(leaf_server.client_url()).await.unwrap();

        let hub_js = async_nats::jetstream::new(hub);
        // create the bucket on the HUB.
        let hub_kv = hub_js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "TEST".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        hub_kv.put("name", "derek".into()).await.unwrap();
        hub_kv.put("age", "22".into()).await.unwrap();

        let mirror_bucket = async_nats::jetstream::kv::Config {
            bucket: "MIRROR".into(),
            mirror: Some(Source {
                name: "TEST".into(),
                external: Some(External {
                    api_prefix: "$JS.HUB.API".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let leaf_js = async_nats::jetstream::new(leaf.clone());
        leaf_js.create_key_value(mirror_bucket).await.unwrap();

        let mirror = leaf_js.get_stream("KV_MIRROR").await.unwrap();

        // Make sure mirror direct set.
        assert!(mirror.cached_info().config.mirror_direct);

        // Make sure we sync.
        tokio::time::sleep(Duration::from_secs(6)).await;

        // Bind locally from leafnode and make sure both get and put work.
        let local_kv = leaf_js.get_key_value("MIRROR").await.unwrap();

        local_kv.put("name", "rip".into()).await.unwrap();
        tryhard::retry_fn(|| async {
            match local_kv.get("name").await {
                Ok(Some(name)) => {
                    assert_eq!(from_utf8(&name).unwrap(), "rip");
                    Ok::<(), async_nats::Error>(())
                }
                _ => Err("key not found".into()),
            }
        })
        .retries(5)
        .exponential_backoff(Duration::from_millis(2000))
        .await
        .unwrap();

        // Bind through leafnode connection but to origin KV.
        let leaf_hub_js = async_nats::jetstream::with_domain(leaf, "HUB");

        let test = leaf_hub_js.get_key_value("TEST").await.unwrap();

        test.put("name", "ivan".into()).await.unwrap();
        let name = test.get("name").await.unwrap();
        assert_eq!(from_utf8(&name.unwrap()).unwrap(), "ivan");

        test.purge("name").await.unwrap();
        let name = test.get("name").await.unwrap();
        assert!(name.is_none());

        // Shutdown HUB and test get still work.
        drop(hub_server);

        local_kv.get("name").await.unwrap();
    }

    #[tokio::test]
    async fn republish_headers_handling() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");

        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client.clone());

        context
            .create_stream(async_nats::jetstream::stream::Config {
                subjects: vec!["A.>".into(), "B.>".into()],
                name: "source".into(),
                republish: Some(async_nats::jetstream::stream::Republish {
                    source: "A.>".into(),
                    destination: "$KV.test.>".into(),
                    headers_only: false,
                }),
                max_messages_per_subject: 10,
                ..Default::default()
            })
            .await
            .unwrap();

        let kv_stream = context
            .create_stream(stream::Config {
                subjects: vec!["$KV.test.>".into()],
                name: "KV_test".into(),
                max_messages_per_subject: 10,
                ..Default::default()
            })
            .await
            .unwrap();

        let kv = context.get_key_value("test").await.unwrap();

        // Publish some messages to alter the sequences for republished messages.
        context
            .publish("B.foo", "data".into())
            .await
            .unwrap()
            .await
            .unwrap();
        context
            .publish("B.bar", "data".into())
            .await
            .unwrap()
            .await
            .unwrap();

        // now, publish the actual KV keys.
        context
            .publish("A.orange", "key".into())
            .await
            .unwrap()
            .await
            .unwrap();
        context
            .publish("A.tomato", "hello".into())
            .await
            .unwrap()
            .await
            .unwrap();

        assert_eq!(1, kv.entry("orange").await.unwrap().unwrap().revision);
        assert_eq!(2, kv.entry("tomato").await.unwrap().unwrap().revision);

        let mut config = kv_stream.cached_info().config.clone();
        config.allow_direct = true;

        // Update the stream to allow direct access.
        context.update_stream(config).await.unwrap();
        // Get a fresh instance, so owe are using direct get.
        let kv = context.get_key_value("test").await.unwrap();

        assert_eq!(1, kv.entry("orange").await.unwrap().unwrap().revision);
        assert_eq!(2, kv.entry("tomato").await.unwrap().unwrap().revision);
    }

    #[cfg(feature = "server_2_11")]
    #[tokio::test]
    async fn limit_markers() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);
        context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "no_ttl".into(),
                description: "test_description".into(),
                history: 15,
                max_age: Duration::from_millis(100),
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let info = context.get_stream("KV_no_ttl").await.unwrap();

        assert_eq!(info.cached_info().config.subject_delete_marker_ttl, None);
        assert!(!info.cached_info().config.allow_message_ttl);

        context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "ttl".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                limit_markers: Some(Duration::from_secs(10)),
                ..Default::default()
            })
            .await
            .unwrap();

        let info = context.get_stream("KV_ttl").await.unwrap();
        assert_eq!(
            info.cached_info().config.subject_delete_marker_ttl,
            Some(Duration::from_secs(10))
        );
        assert!(info.cached_info().config.allow_message_ttl);
    }

    #[cfg(feature = "server_2_11")]
    #[tokio::test]
    async fn create_with_ttl() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "ttl".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                limit_markers: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .await
            .unwrap();

        kv.create_with_ttl("key", "value".into(), Duration::from_secs(1))
            .await
            .unwrap();

        kv.get("key").await.unwrap().unwrap();

        tokio::time::sleep(Duration::from_millis(2500)).await;
        let result = kv.get("key").await.unwrap();
        assert!(result.is_none(), "key should be expired");
    }

    #[cfg(feature = "server_2_11")]
    #[tokio::test]
    async fn purge_with_ttl() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "ttl".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                limit_markers: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..10 {
            kv.put("key1", "value".into()).await.unwrap();

            kv.put("key2", "value".into()).await.unwrap();
        }

        let mut stream = context.get_stream("KV_ttl").await.unwrap();
        assert_eq!(stream.cached_info().state.messages, 20);
        kv.purge_with_ttl("key1", Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(stream.info().await.unwrap().state.messages, 11);
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(stream.info().await.unwrap().state.messages, 10);
    }

    #[cfg(feature = "server_2_11")]
    #[tokio::test]
    async fn watch_with_markers() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "ttl".into(),
                description: "test_description".into(),
                history: 15,
                storage: StorageType::File,
                num_replicas: 1,
                limit_markers: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .await
            .unwrap();

        for _ in 0..10 {
            kv.put("key1", "value".into()).await.unwrap();

            kv.put("key2", "value".into()).await.unwrap();
        }

        let mut watcher = kv.watch_all().await.unwrap();

        kv.purge("key1").await.unwrap();

        let entry = watcher.next().await.unwrap().unwrap();
        assert_eq!(entry.key, "key1");
        assert_eq!(entry.value.as_ref(), b"");
        assert_eq!(Operation::Purge, entry.operation);
    }

    #[cfg(feature = "server_2_11")]
    #[tokio::test]
    async fn watch_with_limit_markers() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        // Create a bucket with a very short max_age to trigger automatic expiration
        // and limit_markers enabled to generate marker messages
        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "maxage".into(),
                description: "test maxage markers".into(),
                history: 10,
                max_age: Duration::from_millis(500),
                storage: StorageType::File,
                num_replicas: 1,
                limit_markers: Some(Duration::from_secs(10)),
                ..Default::default()
            })
            .await
            .unwrap();

        // Start watching all keys (this will watch for new updates)
        let mut watcher = kv.watch_all().await.unwrap();

        // Put a key that will age out
        kv.put("expiring_key", "value".into()).await.unwrap();

        // First entry should be the Put operation we just did
        let entry = watcher.next().await.unwrap().unwrap();
        assert_eq!(entry.key, "expiring_key");
        assert_eq!(entry.operation, Operation::Put);
        assert_eq!(entry.value.as_ref(), b"value");

        // Wait for the key to age out - the server will emit a marker message
        // We need to wait longer than max_age (500ms) for the server to process expiration
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // The next entry should be the MaxAge marker with Operation::Purge
        // This is the key assertion - before the fix, this would be Operation::Put with empty payload
        let entry = watcher.next().await.unwrap().unwrap();
        assert_eq!(entry.key, "expiring_key");
        assert_eq!(
            entry.operation,
            Operation::Purge,
            "MaxAge marker should result in Operation::Purge, not Operation::Put"
        );
        assert_eq!(
            entry.value.as_ref(),
            b"",
            "MaxAge marker should have empty payload"
        );
    }

    #[tokio::test]
    async fn create_or_update_key_value() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let err = context
            .update_key_value(kv::Config {
                bucket: "new".to_string(),
                history: 10,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind(), UpdateKeyValueErrorKind::NotFound);

        let bucket = context
            .create_or_update_key_value(kv::Config {
                bucket: "new".to_string(),
                history: 15,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(bucket.name, "new");
        assert_eq!(
            bucket.stream.cached_info().config.max_messages_per_subject,
            15
        );

        let bucket = context
            .create_or_update_key_value(kv::Config {
                bucket: "new".to_string(),
                history: 20,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(bucket.name, "new");
        assert_eq!(
            bucket.stream.cached_info().config.max_messages_per_subject,
            20
        );

        let bucket = context
            .update_key_value(kv::Config {
                bucket: "new".to_string(),
                history: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            bucket.stream.cached_info().config.max_messages_per_subject,
            50
        );
    }

    #[tokio::test]
    async fn test_direct_get_with_long_bucket_names() {
        // Regression test for issue #1457
        // Tests that KV get operations work correctly with long bucket names containing hyphens
        // The issue was that direct_get_last_for_subject was sending a JSON payload when the subject
        // was embedded in the URL, causing InvalidSubject errors
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        // Create KV bucket with a long name containing hyphens (as in the issue)
        let bucket_name = "local-test-stream-rust-jetstream-bucket-local-test-cluster";
        let store = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: bucket_name.into(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Use a key with hyphens as in the issue
        let key = "local-test-stream-rust-key";
        let value = "test_value_12345";

        // PUT operation
        store.put(key, value.into()).await.unwrap();

        // Small delay to ensure write is propagated
        tokio::time::sleep(Duration::from_millis(100)).await;

        // GET operation - this would fail with InvalidSubject before the fix
        let get_result = store.get(key).await;

        match get_result {
            Ok(Some(entry)) => {
                let retrieved_value = String::from_utf8_lossy(&entry);
                assert_eq!(retrieved_value, value, "Retrieved value doesn't match");
            }
            Ok(None) => {
                panic!("Key not found, but we just put it!");
            }
            Err(e) => {
                panic!("KV get failed with error: {:?}", e);
            }
        }
    }
}
