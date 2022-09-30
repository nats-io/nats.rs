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
            stream::{DiscardPolicy, StorageType},
        },
        ConnectOptions,
    };
    use bytes::Bytes;
    use futures::StreamExt;

    #[tokio::test]
    async fn create() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let mut kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".to_string(),
                description: "test_description".to_string(),
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
    async fn put() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".to_string(),
                description: "test_description".to_string(),
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
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".to_string(),
                description: "test_description".to_string(),
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
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let mut kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".to_string(),
                description: "test_description".to_string(),
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
                name: "KV_test".to_string(),
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
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".to_string(),
                description: "test_description".to_string(),
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
        let updated_paylaod: Bytes = "updated".into();
        let rev2 = kv
            .update("key", updated_paylaod.clone(), rev)
            .await
            .unwrap();
        let value = kv.get("key").await.unwrap();
        assert_eq!(rev2, rev + 1);
        assert_eq!(from_utf8(&value.unwrap()).unwrap(), updated_paylaod);
    }

    #[tokio::test]
    async fn delete() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            // .connect(server.client_url())
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "delete".to_string(),
                description: "test_description".to_string(),
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
    async fn purge() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            // .connect(server.client_url())
            .connect(server.client_url())
            .await
            .unwrap();

        let context = async_nats::jetstream::new(client);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "purge".to_string(),
                description: "test_description".to_string(),
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
    async fn history() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            .await
            .unwrap();

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
        for i in 0..20 {
            kv.put("key", format!("{}", i).into()).await.unwrap();
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
    async fn watch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            .await
            .unwrap();

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
    async fn watch_all() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            .await
            .unwrap();

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
    async fn keys() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            .await
            .unwrap();

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

        for i in 0..10 {
            kv.put("bar", i.to_string().into()).await.unwrap();
        }
        for i in 0..10 {
            kv.put("foo", i.to_string().into()).await.unwrap();
        }

        let mut keys = kv.keys().await.unwrap().collect::<Vec<String>>();
        keys.sort();
        assert_eq!(vec!["bar", "foo"], keys);

        let kv = context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "history2".to_string(),
                description: "test_description".to_string(),
                history: 15,
                max_age: Duration::from_millis(100),
                storage: StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        kv.put("baz", "value".into()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(kv.keys().await.unwrap().count(), 0);
    }
}
