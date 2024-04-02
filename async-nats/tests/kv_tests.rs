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

mod kv {
    use std::{str::from_utf8, time::Duration};

    use async_nats::{
        jetstream::{
            kv::Operation,
            stream::{self, DiscardPolicy, Republish, Source, StorageType},
        },
        ConnectOptions,
    };
    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt};

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

        kv.delete("key").await.unwrap();
        let create = kv.create("key", payload.clone()).await;
        assert!(create.is_ok());

        kv.purge("key").await.unwrap();
        let create = kv.create("key", payload.clone()).await;
        assert!(create.is_ok());
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

        context
            .update_stream(async_nats::jetstream::stream::Config {
                max_messages_per_subject: 10,
                name: "KV_test".into(),
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
    async fn watch() {
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
                domain: Some("HUB".into()),
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
        .exponential_backoff(Duration::from_millis(500))
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
}
