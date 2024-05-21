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

#[cfg(feature = "compatibility_tests")]
mod compatibility {
    use futures::{pin_mut, stream::Peekable, StreamExt};
    use ring::digest::{self, SHA256};

    use core::panic;
    use std::{collections::HashMap, pin::Pin, str::from_utf8};

    use async_nats::{
        jetstream::{
            self,
            object_store::{self, ObjectMetadata, UpdateMetadata},
        },
        service::{self, ServiceExt},
    };
    use serde::{Deserialize, Serialize};
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn kv() {
        panic!("kv suite not implemented yet")
    }

    #[tokio::test]
    async fn object_store() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "localhost:4222".to_string());
        tracing::info!("staring client for object store tests at {}", url);
        let client = async_nats::ConnectOptions::new()
            .max_reconnects(10)
            .retry_on_initial_connect()
            .connect(&url)
            .await
            .unwrap();

        let tests = client
            .subscribe("tests.object-store.>")
            .await
            .unwrap()
            .peekable();
        pin_mut!(tests);

        let mut done = client.subscribe("tests.done").await.unwrap();

        loop {
            tokio::select! {
                _ = done.next() => {
                tracing::info!("object store tests done");
                 return;
                }
                message = tests.as_mut().peek() => {
                let test: Test = Test::try_from(message.unwrap()).unwrap();
                    match test.suite.as_str() {
                        "object-store" => {
                            let object_store = ObjectStore {
                                client: client.clone(),
                            };
                            match test.test.as_str() {
                                "default-bucket" => object_store.default_bucket(tests.as_mut()).await,
                                "custom-bucket" => object_store.custom_bucket(tests.as_mut()).await,
                                "get-object" => object_store.get_object(tests.as_mut()).await,
                                "put-object" => object_store.put_object(tests.as_mut()).await,
                                "update-metadata" => object_store.update_metadata(tests.as_mut()).await,
                                "watch-updates" => object_store.watch_updates(tests.as_mut()).await,
                                "watch" => object_store.watch(tests.as_mut()).await,
                                "get-link" => object_store.get_link(tests.as_mut()).await,
                                "put-link" => object_store.put_link(tests.as_mut()).await,
                                unknown => panic!("unkown test: {}", unknown),
                            }
                        }
                        unknown => panic!("not an object store suite: {}", unknown),
                    }
                }
            }
        }
    }
    struct Test {
        suite: String,
        test: String,
    }

    impl TryFrom<&async_nats::Message> for Test {
        type Error = String;

        fn try_from(message: &async_nats::Message) -> Result<Self, Self::Error> {
            let mut elements = message.subject.split('.').skip(1);

            let suite = elements
                .next()
                .ok_or("missing suite token".to_string())?
                .to_string();
            let test = elements
                .next()
                .ok_or("missing test token".to_string())?
                .to_string();

            Ok(Test { suite, test })
        }
    }

    struct ObjectStore {
        client: async_nats::Client,
    }

    type PinnedSubscriber<'a> = Pin<&'a mut Peekable<async_nats::Subscriber>>;

    impl ObjectStore {
        async fn default_bucket(&self, mut test_commands: PinnedSubscriber<'_>) {
            let create = test_commands.as_mut().next().await.unwrap();
            println!("received first request: {}", create.subject);

            let given: TestRequest<HashMap<String, String>> =
                serde_json::from_slice(&create.payload).unwrap();
            let jetstream = async_nats::jetstream::new(self.client.clone());
            jetstream
                .create_object_store(object_store::Config {
                    bucket: given.config.get("bucket").unwrap().to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();

            self.client
                .publish(create.reply.unwrap(), "".into())
                .await
                .unwrap();
            self.client.flush().await.unwrap();

            let done = test_commands.next().await.unwrap();
            if done.headers.is_some() {
                panic!("test failed: {:?}", done.headers);
            } else {
                println!("test default-bucket PASS");
            }
        }

        async fn custom_bucket(&self, mut commands: PinnedSubscriber<'_>) {
            let create = commands.as_mut().next().await.unwrap();
            println!("received custom request: {}", create.subject);

            let custom_config: TestRequest<object_store::Config> =
                serde_json::from_slice(&create.payload).unwrap();

            async_nats::jetstream::new(self.client.clone())
                .create_object_store(custom_config.config)
                .await
                .unwrap();

            self.client
                .publish(create.reply.unwrap(), "".into())
                .await
                .unwrap();
            self.client.flush().await.unwrap();

            let done = commands.next().await.unwrap();
            if done.headers.is_some() {
                panic!("test failed: {:?}", done.headers);
            } else {
                println!("test custom-bucket PASS");
            }
        }

        async fn put_object(&self, mut commands: PinnedSubscriber<'_>) {
            #[derive(Debug, Deserialize)]
            struct ObjectRequest {
                url: String,
                bucket: String,
                #[serde(flatten)]
                test_request: TestRequest<ObjectMetadata>,
            }

            let object_request = commands.as_mut().next().await.unwrap();
            println!("received third request: {}", object_request.subject);
            let reply = object_request.reply.unwrap().clone();
            let object_request: ObjectRequest =
                serde_json::from_slice(&object_request.payload).unwrap();

            let bucket = async_nats::jetstream::new(self.client.clone())
                .get_object_store(object_request.bucket.clone())
                .await
                .unwrap();

            let file = reqwest::get(object_request.url).await.unwrap();
            let contents = file.bytes().await.unwrap();

            bucket
                .put(object_request.test_request.config, &mut contents.as_ref())
                .await
                .unwrap();

            self.client.publish(reply, "".into()).await.unwrap();
            self.client.flush().await.unwrap();

            let done = commands.next().await.unwrap();
            if done.headers.is_some() {
                panic!("test failed: {:?}", done.headers);
            } else {
                println!("test put-object PASS");
            }
        }

        async fn get_object(&self, mut commands: PinnedSubscriber<'_>) {
            #[derive(Deserialize)]
            struct Command {
                object: String,
                bucket: String,
            }
            let get_request = commands.as_mut().next().await.unwrap();

            let request: Command = serde_json::from_slice(&get_request.payload).unwrap();

            let bucket = async_nats::jetstream::new(self.client.clone())
                .get_object_store(request.bucket)
                .await
                .unwrap();
            let mut object = bucket.get(request.object).await.unwrap();
            let mut contents = vec![];

            object.read_to_end(&mut contents).await.unwrap();

            let digest = digest::digest(&SHA256, &contents);

            self.client
                .publish(
                    get_request.reply.unwrap(),
                    digest.as_ref().to_owned().into(),
                )
                .await
                .unwrap();

            let done = commands.next().await.unwrap();
            if done.headers.is_some() {
                panic!("test failed: {:?}", done.headers);
            } else {
                println!("test get-object PASS");
            }
        }

        async fn put_link(&self, mut commands: PinnedSubscriber<'_>) {
            #[derive(Deserialize, Debug)]
            struct Command {
                object: String,
                bucket: String,
                link_name: String,
            }
            let get_request = commands.as_mut().next().await.unwrap();

            let request: Command = serde_json::from_slice(&get_request.payload).unwrap();

            let bucket = async_nats::jetstream::new(self.client.clone())
                .get_object_store(request.bucket)
                .await
                .unwrap();
            let object = bucket.get(request.object).await.unwrap();

            bucket.add_link(request.link_name, &object).await.unwrap();

            self.client
                .publish(get_request.reply.unwrap(), "".into())
                .await
                .unwrap();

            let done = commands.next().await.unwrap();
            if done.headers.is_some() {
                panic!("test failed: {:?}", done.headers);
            } else {
                println!("test put-link PASS");
            }
        }

        async fn get_link(&self, mut commands: PinnedSubscriber<'_>) {
            #[derive(Deserialize, Debug)]
            struct Command {
                object: String,
                bucket: String,
            }
            let get_request = commands.as_mut().next().await.unwrap();

            let request: Command = serde_json::from_slice(&get_request.payload).unwrap();

            let bucket = async_nats::jetstream::new(self.client.clone())
                .get_object_store(request.bucket)
                .await
                .unwrap();
            let mut object = bucket.get(request.object).await.unwrap();
            let mut contents = vec![];

            object.read_to_end(&mut contents).await.unwrap();

            let digest = digest::digest(&SHA256, &contents);

            self.client
                .publish(
                    get_request.reply.unwrap(),
                    digest.as_ref().to_owned().into(),
                )
                .await
                .unwrap();

            let done = commands.next().await.unwrap();
            if done.headers.is_some() {
                panic!("test failed: {:?}", done.headers);
            } else {
                println!("test get-object PASS");
            }
        }

        async fn update_metadata(&self, mut commands: PinnedSubscriber<'_>) {
            #[derive(Deserialize)]
            struct Metadata {
                object: String,
                bucket: String,
                config: UpdateMetadata,
            }

            let update_command = commands.as_mut().next().await.unwrap();

            let given: Metadata = serde_json::from_slice(&update_command.payload).unwrap();

            let object_store = jetstream::new(self.client.clone())
                .get_object_store(given.bucket)
                .await
                .unwrap();

            object_store
                .update_metadata(given.object, given.config)
                .await
                .unwrap();

            self.client
                .publish(update_command.reply.unwrap(), "".into())
                .await
                .unwrap();

            let done = commands.next().await.unwrap();
            if done.headers.is_some() {
                panic!("test failed: {:?}", done.headers);
            } else {
                println!("test update-metadata PASS");
            }
        }

        async fn watch_updates(&self, mut commands: PinnedSubscriber<'_>) {
            #[derive(Deserialize)]
            struct Command {
                #[allow(dead_code)]
                object: String,
                bucket: String,
            }
            let get_request = commands.as_mut().next().await.unwrap();

            let request: Command = serde_json::from_slice(&get_request.payload).unwrap();
            let bucket = async_nats::jetstream::new(self.client.clone())
                .get_object_store(request.bucket)
                .await
                .unwrap();

            let mut watch = bucket.watch().await.unwrap();

            let info = watch.next().await.unwrap().unwrap();

            self.client
                .publish(get_request.reply.unwrap(), info.digest.unwrap().into())
                .await
                .unwrap();

            let done = commands.next().await.unwrap();
            if done.headers.is_some() {
                panic!("test failed: {:?}", done.headers);
            } else {
                println!("test update-metadata PASS");
            }
        }

        async fn watch(&self, mut commands: PinnedSubscriber<'_>) {
            #[derive(Deserialize)]
            struct Command {
                #[allow(dead_code)]
                object: String,
                bucket: String,
            }
            let get_request = commands.as_mut().next().await.unwrap();

            let request: Command = serde_json::from_slice(&get_request.payload).unwrap();
            let bucket = async_nats::jetstream::new(self.client.clone())
                .get_object_store(request.bucket)
                .await
                .unwrap();

            let mut watch = bucket.watch_with_history().await.unwrap();

            let info = watch.next().await.unwrap().unwrap();
            let second_info = watch.next().await.unwrap().unwrap();

            let response = [info.digest.unwrap(), second_info.digest.unwrap()].join(",");

            self.client
                .publish(get_request.reply.unwrap(), response.into())
                .await
                .unwrap();

            let done = commands.next().await.unwrap();
            if done.headers.is_some() {
                panic!("test failed: {:?}", done.headers);
            } else {
                println!("test update-metadata PASS");
            }
        }
    }

    #[tokio::test]
    async fn service_core() {
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "localhost:4222".to_string());
        tracing::info!("staring client for service tests at {}", url);
        let client = async_nats::ConnectOptions::new()
            .max_reconnects(10)
            .retry_on_initial_connect()
            .connect(&url)
            .await
            .unwrap();

        let mut tests = client
            .subscribe("tests.service.core.>")
            .await
            .unwrap()
            .peekable();

        #[derive(Serialize, Deserialize)]
        struct ServiceConfig {
            #[serde(flatten)]
            service: service::Config,
            groups: Vec<GroupConfig>,
            endpoints: Vec<EndpointConfig>,
        }

        #[derive(Serialize, Deserialize)]
        struct GroupConfig {
            name: String,
            queue_group: Option<String>,
        }

        #[derive(Serialize, Deserialize)]
        struct EndpointConfig {
            name: String,
            subject: String,
            metadata: Option<HashMap<String, String>>,
            queue_group: Option<String>,
            group: Option<String>,
        }

        let test_request = tests.next().await.unwrap();
        println!(
            "received first request: {}",
            from_utf8(&test_request.payload).unwrap()
        );
        let config: TestRequest<ServiceConfig> =
            serde_json::from_slice(&test_request.payload).expect("failed to parse service config");
        let mut config = config.config;
        config.service.stats_handler = Some(service::StatsHandler(Box::new(
            |_, stats| serde_json::json!({ "endpoint": stats.name }),
        )));

        let service = client
            .add_service(config.service)
            .await
            .expect("failed to add service");

        let mut group_handlers = HashMap::new();
        for group in config.groups {
            match group.queue_group {
                Some(queue_group) => group_handlers.insert(
                    group.name.clone(),
                    service.group_with_queue_group(group.name.clone(), queue_group),
                ),
                None => group_handlers.insert(group.name.clone(), service.group(group.name)),
            };
        }

        for endpoint_config in config.endpoints {
            let mut endpoint = {
                if let Some(ref group) = endpoint_config.group {
                    group_handlers
                        .get(group)
                        .expect("unknown group")
                        .endpoint_builder()
                } else {
                    service.endpoint_builder()
                }
            };
            endpoint = endpoint.name(endpoint_config.name.clone());
            if let Some(ref queue_group) = endpoint_config.queue_group {
                endpoint = endpoint.queue_group(queue_group);
            }
            if let Some(ref metadata) = endpoint_config.metadata {
                endpoint = endpoint.metadata(metadata.to_owned());
            }
            let mut endpoint = endpoint
                .add(endpoint_config.subject)
                .await
                .expect("failed to add endpoint");
            tokio::task::spawn({
                let endpoint_name = endpoint_config.name.clone();
                async move {
                    while let Some(request) = endpoint.next().await {
                        if endpoint_name.as_str() == "faulty" {
                            request
                                .respond(Err(service::error::Error {
                                    status: "handler error".into(),
                                    code: 500,
                                }))
                                .await
                                .expect("failed to respond");
                        } else {
                            let response = request.message.payload.clone();
                            request
                                .respond(Ok(response))
                                .await
                                .expect("failed to echo respond");
                        }
                    }
                }
            });
        }
        client
            .publish(test_request.reply.unwrap(), "".into())
            .await
            .unwrap();

        let cleanup = tests.next().await.expect("failed to get cleanup");
        service.stop().await.expect("failed to stop service");
        client
            .publish(cleanup.reply.unwrap(), "".into())
            .await
            .expect("failed to publish cleanup ack");
        client.flush().await.expect("failed to flush");
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestRequest<T> {
        suite: String,
        test: String,
        command: String,
        config: T,
    }
}
