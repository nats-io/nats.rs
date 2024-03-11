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

mod object_store {
    use std::{io, time::Duration};

    use async_nats::jetstream::{
        object_store::{AddLinkErrorKind, ObjectMetadata, UpdateMetadata},
        stream::DirectGetErrorKind,
    };
    use base64::Engine;
    use futures::StreamExt;
    use rand::RngCore;
    use tokio::io::AsyncReadExt;

    use ring::digest::{self, SHA256};

    #[tokio::test]
    async fn get_and_put() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut rng = rand::thread_rng();
        let mut bytes = vec![0; 1024 * 1024 + 22];
        rng.try_fill_bytes(&mut bytes).unwrap();

        let digest = digest::digest(&SHA256, &bytes);

        bucket.put("FOO", &mut bytes.as_slice()).await.unwrap();

        let mut object = bucket.get("FOO").await.unwrap();

        let mut result = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            if let Ok(n) = object.read(&mut buffer).await {
                if n == 0 {
                    println!("finished");
                    break;
                }

                result.extend_from_slice(&buffer[..n]);
            }
        }
        assert_eq!(
            Some(format!(
                "SHA-256={}",
                base64::engine::general_purpose::URL_SAFE.encode(digest)
            )),
            object.info.digest
        );
        assert_eq!(result, bytes);

        // Check if following a link works.
        bucket.add_link("link", &object.info).await.unwrap();

        tracing::info!("getting link");
        let mut object_link = bucket.get("link").await.unwrap();
        let mut contents = Vec::new();

        tracing::info!("reading content");
        object_link.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, result);

        bucket
            .put(
                ObjectMetadata {
                    name: "BAR".to_string(),
                    description: Some("custom object".to_string()),
                    chunk_size: Some(64 * 1024),
                },
                &mut bytes.as_slice(),
            )
            .await
            .unwrap();

        let meta = bucket.get("BAR").await.unwrap();
        assert_eq!(
            64 * 1024,
            meta.info.options.unwrap().max_chunk_size.unwrap()
        );
    }

    #[tokio::test]
    async fn watch() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        bucket
            .put("FOO", &mut std::io::Cursor::new(vec![1, 2, 3, 4]))
            .await
            .unwrap();

        let mut watcher = bucket.watch().await.unwrap();

        tokio::task::spawn({
            let bucket = bucket.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                bucket
                    .put("BAR", &mut io::Cursor::new(vec![2, 3, 4, 5]))
                    .await
                    .unwrap();
                bucket.delete("BAR").await.unwrap();
            }
        });

        let object = watcher.next().await.unwrap().unwrap();
        assert_eq!(object.name, "BAR".to_string());
        let object = watcher.next().await.unwrap().unwrap();
        assert_eq!(object.name, "BAR".to_string());
        assert!(object.deleted);
    }

    #[tokio::test]
    async fn watch_with_history() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        bucket
            .put("FOO", &mut std::io::Cursor::new(vec![1, 2, 3, 4]))
            .await
            .unwrap();

        bucket
            .put("BAR", &mut std::io::Cursor::new(vec![5, 6, 7, 8]))
            .await
            .unwrap();

        bucket
            .put("FOO", &mut std::io::Cursor::new(vec![9, 0, 1, 2]))
            .await
            .unwrap();

        let mut watcher = bucket.watch_with_history().await.unwrap();

        tokio::task::spawn({
            let bucket = bucket.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                bucket
                    .put("BAR", &mut io::Cursor::new(vec![2, 3, 4, 5]))
                    .await
                    .unwrap();
                bucket.delete("BAR").await.unwrap();
            }
        });

        // check to see if we get the values in accordance to the LastPerSubject deliver policy
        // we should get `BAR` and only one `FOO`
        let object = watcher.next().await.unwrap().unwrap();
        assert_eq!(object.name, "BAR".to_string());

        let object = watcher.next().await.unwrap().unwrap();
        assert_eq!(object.name, "FOO".to_string());

        // make sure we get the rest correctly
        let object = watcher.next().await.unwrap().unwrap();
        assert_eq!(object.name, "BAR".to_string());
        let object = watcher.next().await.unwrap().unwrap();
        assert_eq!(object.name, "BAR".to_string());
        assert!(object.deleted);
    }

    #[tokio::test]
    async fn info() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        bucket
            .put("FOO", &mut io::Cursor::new(vec![2, 3, 4, 5]))
            .await
            .unwrap();

        let info = bucket.info("FOO").await.unwrap();

        assert_ne!(info.nuid, "");
        assert_eq!(info.name, "FOO");
        assert_eq!(info.bucket, "bucket");
        assert_eq!(info.size, 4);
        assert!(!info.deleted);

        let modified = info.modified;
        bucket
            .put("FOO", &mut io::Cursor::new(vec![2, 3, 4, 5]))
            .await
            .unwrap();

        let info = bucket.info("FOO").await.unwrap();
        assert_ne!(info.modified, modified);
    }

    #[tokio::test]
    async fn delete() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut rng = rand::thread_rng();
        let mut bytes = vec![0; 1024 * 1024 + 22];
        rng.try_fill_bytes(&mut bytes).unwrap();

        bucket.put("FOO", &mut bytes.as_slice()).await.unwrap();

        let info = bucket.info("FOO").await.unwrap();

        assert!(!info.deleted);
        assert!(info.size > 0);

        bucket.delete("FOO").await.unwrap();

        let info = bucket.info("FOO").await.unwrap();
        assert!(info.deleted);
        assert!(info.size == 0);
    }

    #[tokio::test]
    async fn seal() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let mut bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut rng = rand::thread_rng();
        let mut bytes = vec![0; 1024 * 1024 + 22];
        rng.try_fill_bytes(&mut bytes).unwrap();

        bucket.put("FOO", &mut bytes.as_slice()).await.unwrap();

        let info = bucket.info("FOO").await.unwrap();

        assert!(!info.deleted);
        assert!(info.size > 0);

        bucket.seal().await.unwrap();

        let mut stream = jetstream.get_stream("OBJ_bucket").await.unwrap();
        let info = stream.info().await.unwrap();
        assert!(info.config.sealed);
    }

    // check for digester parity https://github.com/nats-io/nats-architecture-and-design/issues/150
    #[tokio::test]
    async fn digest() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let cases = vec![
            (
                "tests/configs/digests/digester_test_bytes_000100.txt",
                "IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=",
            ),
            (
                "tests/configs/digests/digester_test_bytes_001000.txt",
                "DZj4RnBpuEukzFIY0ueZ-xjnHY4Rt9XWn4Dh8nkNfnI=",
            ),
            (
                "tests/configs/digests/digester_test_bytes_010000.txt",
                "RgaJ-VSJtjNvgXcujCKIvaheiX_6GRCcfdRYnAcVy38=",
            ),
            (
                "tests/configs/digests/digester_test_bytes_100000.txt",
                "yan7pwBVnC1yORqqgBfd64_qAw6q9fNA60_KRiMMooE=",
            ),
        ];

        for (filename, digest) in cases {
            let file = std::fs::read(filename).unwrap();

            bucket.put(filename, &mut file.as_slice()).await.unwrap();

            let mut object = bucket.get(filename).await.unwrap();
            assert_eq!(object.info.digest, Some(format!("SHA-256={digest}")));

            let mut result = Vec::new();
            object.read_to_end(&mut result).await.unwrap();
            assert_eq!(result, file);
        }
    }

    #[tokio::test]
    async fn list() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        bucket
            .put(
                ObjectMetadata {
                    name: "Foo".to_string(),
                    description: Some("foo desc".to_string()),
                    chunk_size: None,
                },
                &mut "dadada".as_bytes(),
            )
            .await
            .unwrap();
        bucket
            .put("DEL", &mut "32142421424".as_bytes())
            .await
            .unwrap();
        bucket.delete("DEL").await.unwrap();
        for i in 0..10 {
            bucket
                .put(format!("{i}").as_ref(), &mut "blalbalballba".as_bytes())
                .await
                .unwrap();
        }
        let mut list = bucket.list().await.unwrap();
        let obj = list.next().await.unwrap().unwrap();
        assert_eq!("Foo".to_string(), obj.name);
        assert_eq!(Some("foo desc".to_string()), obj.description);
        assert_eq!(list.next().await.unwrap().unwrap().name, "0");
        assert_eq!(list.next().await.unwrap().unwrap().name, "1");
        assert_eq!(list.count().await, 8);
    }

    #[tokio::test]
    async fn stack_overflow() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        bucket
            .put("DATA", &mut "some data".as_bytes())
            .await
            .unwrap();
        bucket
            .put("DATA", &mut "some data".as_bytes())
            .await
            .unwrap();
        bucket
            .put("DATA", &mut "some data".as_bytes())
            .await
            .unwrap();
        bucket
            .put("DATA", &mut "some data".as_bytes())
            .await
            .unwrap();
        bucket
            .put("DATA", &mut "some data".as_bytes())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn update_metadata() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        bucket
            .put("old_object", &mut "some data".as_bytes())
            .await
            .unwrap();

        let given_metadata = UpdateMetadata {
            name: "new_object".to_owned(),
            description: Some("description".to_string()),
        };

        bucket
            .update_metadata("old_object", given_metadata.clone())
            .await
            .unwrap();

        let stream = jetstream.get_stream("OBJ_bucket").await.unwrap();

        stream
            .direct_get_last_for_subject(format!(
                "$O.bucket.M.{}",
                base64::engine::general_purpose::URL_SAFE.encode("new_object")
            ))
            .await
            .unwrap();

        let old_meta_subject = stream
            .direct_get_last_for_subject(format!(
                "$O.bucket.M.{}",
                base64::engine::general_purpose::URL_SAFE.encode("old_object")
            ))
            .await
            .unwrap_err();

        assert_eq!(old_meta_subject.kind(), DirectGetErrorKind::NotFound);

        let info = bucket.info("new_object").await.unwrap();

        assert_eq!(info.name, given_metadata.name);
        assert_eq!(info.description, given_metadata.description);
    }

    #[tokio::test]
    async fn add_link() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        let object = bucket
            .put("object", &mut "some data".as_bytes())
            .await
            .unwrap();

        let another_object = bucket
            .put("another_object", &mut "other data".as_bytes())
            .await
            .unwrap();

        bucket.add_link("link", &object).await.unwrap();

        let link_info = bucket.info("link").await.unwrap();

        assert_eq!(
            link_info
                .options
                .as_ref()
                .unwrap()
                .link
                .as_ref()
                .unwrap()
                .name
                .as_ref()
                .unwrap()
                .as_str(),
            "object"
        );
        assert_eq!(
            link_info
                .options
                .as_ref()
                .unwrap()
                .link
                .as_ref()
                .unwrap()
                .bucket
                .as_str(),
            "bucket"
        );

        let result = bucket
            .add_link("object", &another_object)
            .await
            .unwrap_err();
        assert_eq!(result.kind(), AddLinkErrorKind::AlreadyExists);

        let result = bucket.add_link("", &another_object).await.unwrap_err();
        assert_eq!(result.kind(), AddLinkErrorKind::EmptyName);

        let result = bucket.add_link("new_link", &link_info).await.unwrap_err();
        assert_eq!(result.kind(), AddLinkErrorKind::LinkToLink);
    }

    #[tokio::test]
    async fn add_bucket_link() {
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "another".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        bucket.add_bucket_link("link", "another").await.unwrap();

        let link_info = bucket.info("link").await.unwrap();
        assert!(link_info
            .options
            .as_ref()
            .unwrap()
            .link
            .as_ref()
            .unwrap()
            .name
            .is_none());
        assert_eq!(
            link_info
                .options
                .as_ref()
                .unwrap()
                .link
                .as_ref()
                .unwrap()
                .bucket
                .as_str(),
            "another"
        );
    }
}
