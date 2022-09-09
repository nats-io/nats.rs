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
    use std::str::from_utf8;

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
    async fn kv_create() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            // .connect(server.client_url())
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
        assert_eq!("KV_test", kv.stream_name);
        assert_eq!(
            kv.stream.info().await.unwrap().config.discard,
            DiscardPolicy::New
        );
    }

    #[tokio::test]
    async fn kv_put() {
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
    async fn kv_update() {
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
        println!("{:?}", kv.status().await.unwrap());
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
    async fn kv_delete() {
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
    async fn kv_purge() {
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
    async fn kv_history() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = ConnectOptions::new()
            .event_callback(|event| async move { println!("event: {:?}", event) })
            .connect(server.client_url())
            // .connect("localhost:4222")
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
}
