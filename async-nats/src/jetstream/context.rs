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
//
//! Manage operations on [Context], create/delete/update [Stream]

use crate::error::Error;
use crate::header::{IntoHeaderName, IntoHeaderValue};
use crate::jetstream::account::Account;
use crate::jetstream::publish::PublishAck;
use crate::jetstream::response::Response;
use crate::subject::ToSubject;
use crate::{header, Client, Command, HeaderMap, HeaderValue, Message, StatusCode};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{Future, TryFutureExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{self, json};
use std::borrow::Borrow;
use std::fmt::Display;
use std::future::IntoFuture;
use std::pin::Pin;
use std::str::from_utf8;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::debug;

use super::consumer::{self, Consumer, FromConsumer, IntoConsumerConfig};
use super::errors::ErrorCode;
use super::is_valid_name;
use super::kv::{Store, MAX_HISTORY};
use super::object_store::{is_valid_bucket_name, ObjectStore};
use super::stream::{
    self, Config, ConsumerError, ConsumerErrorKind, DeleteStatus, DiscardPolicy, External, Info,
    Stream,
};
#[cfg(feature = "server_2_10")]
use super::stream::{Compression, ConsumerCreateStrictError, ConsumerUpdateError};

/// A context which can perform jetstream scoped requests.
#[derive(Debug, Clone)]
pub struct Context {
    pub(crate) client: Client,
    pub(crate) prefix: String,
    pub(crate) timeout: Duration,
}

impl Context {
    pub(crate) fn new(client: Client) -> Context {
        Context {
            client,
            prefix: "$JS.API".to_string(),
            timeout: Duration::from_secs(5),
        }
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout
    }

    pub(crate) fn with_prefix<T: ToString>(client: Client, prefix: T) -> Context {
        Context {
            client,
            prefix: prefix.to_string(),
            timeout: Duration::from_secs(5),
        }
    }

    pub(crate) fn with_domain<T: AsRef<str>>(client: Client, domain: T) -> Context {
        Context {
            client,
            prefix: format!("$JS.{}.API", domain.as_ref()),
            timeout: Duration::from_secs(5),
        }
    }

    /// Publishes [jetstream::Message][super::message::Message] to the [Stream] without waiting for
    /// acknowledgment from the server that the message has been successfully delivered.
    ///
    /// Acknowledgment future that can be polled is returned instead.
    ///
    /// If the stream does not exist, `no responders` error will be returned.
    ///
    /// # Examples
    ///
    /// Publish, and after each publish, await for acknowledgment.
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let ack = jetstream.publish("events", "data".into()).await?;
    /// ack.await?;
    /// jetstream.publish("events", "data".into()).await?.await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Publish and do not wait for the acknowledgment. Await can be deferred to when needed or
    /// ignored entirely.
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let first_ack = jetstream.publish("events", "data".into()).await?;
    /// let second_ack = jetstream.publish("events", "data".into()).await?;
    /// first_ack.await?;
    /// second_ack.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish<S: ToSubject>(
        &self,
        subject: S,
        payload: Bytes,
    ) -> Result<PublishAckFuture, PublishError> {
        self.send_publish(subject, Publish::build().payload(payload))
            .await
    }

    /// Publish a message with headers to a given subject associated with a stream and returns an acknowledgment from
    /// the server that the message has been successfully delivered.
    ///
    /// If the stream does not exist, `no responders` error will be returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let mut headers = async_nats::HeaderMap::new();
    /// headers.append("X-key", "Value");
    /// let ack = jetstream
    ///     .publish_with_headers("events", headers, "data".into())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_with_headers<S: ToSubject>(
        &self,
        subject: S,
        headers: crate::header::HeaderMap,
        payload: Bytes,
    ) -> Result<PublishAckFuture, PublishError> {
        self.send_publish(subject, Publish::build().payload(payload).headers(headers))
            .await
    }

    /// Publish a message built by [Publish] and returns an acknowledgment future.
    ///
    /// If the stream does not exist, `no responders` error will be returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use async_nats::jetstream::context::Publish;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let ack = jetstream
    ///     .send_publish(
    ///         "events",
    ///         Publish::build().payload("data".into()).message_id("uuid"),
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_publish<S: ToSubject>(
        &self,
        subject: S,
        publish: Publish,
    ) -> Result<PublishAckFuture, PublishError> {
        let subject = subject.to_subject();
        let (sender, receiver) = oneshot::channel();

        let respond = self.client.new_inbox().into();

        let send_fut = self
            .client
            .sender
            .send(Command::Request {
                subject,
                payload: publish.payload,
                respond,
                headers: publish.headers,
                sender,
            })
            .map_err(|err| PublishError::with_source(PublishErrorKind::Other, err));

        tokio::time::timeout(self.timeout, send_fut)
            .map_err(|_elapsed| PublishError::new(PublishErrorKind::TimedOut))
            .await??;

        Ok(PublishAckFuture {
            timeout: self.timeout,
            subscription: receiver,
        })
    }

    /// Query the server for account information
    pub async fn query_account(&self) -> Result<Account, AccountError> {
        let response: Response<Account> = self.request("INFO", b"").await?;

        match response {
            Response::Err { error } => Err(AccountError::new(AccountErrorKind::JetStream(error))),
            Response::Ok(account) => Ok(account),
        }
    }

    /// Create a JetStream [Stream] with given config and return a handle to it.
    /// That handle can be used to manage and use [Consumer].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::stream::Config;
    /// use async_nats::jetstream::stream::DiscardPolicy;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .create_stream(Config {
    ///         name: "events".to_string(),
    ///         max_messages: 100_000,
    ///         discard: DiscardPolicy::Old,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_stream<S>(&self, stream_config: S) -> Result<Stream, CreateStreamError>
    where
        Config: From<S>,
    {
        let mut config: Config = stream_config.into();
        if config.name.is_empty() {
            return Err(CreateStreamError::new(
                CreateStreamErrorKind::EmptyStreamName,
            ));
        }
        if !is_valid_name(config.name.as_str()) {
            return Err(CreateStreamError::new(
                CreateStreamErrorKind::InvalidStreamName,
            ));
        }
        if let Some(ref mut mirror) = config.mirror {
            if let Some(ref mut domain) = mirror.domain {
                if mirror.external.is_some() {
                    return Err(CreateStreamError::new(
                        CreateStreamErrorKind::DomainAndExternalSet,
                    ));
                }
                mirror.external = Some(External {
                    api_prefix: format!("$JS.{domain}.API"),
                    delivery_prefix: None,
                })
            }
        }

        if let Some(ref mut sources) = config.sources {
            for source in sources {
                if let Some(ref mut domain) = source.domain {
                    if source.external.is_some() {
                        return Err(CreateStreamError::new(
                            CreateStreamErrorKind::DomainAndExternalSet,
                        ));
                    }
                    source.external = Some(External {
                        api_prefix: format!("$JS.{domain}.API"),
                        delivery_prefix: None,
                    })
                }
            }
        }
        let subject = format!("STREAM.CREATE.{}", config.name);
        let response: Response<Info> = self.request(subject, &config).await?;

        match response {
            Response::Err { error } => Err(error.into()),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

    /// Checks for [Stream] existence on the server and returns handle to it.
    /// That handle can be used to manage and use [Consumer].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_stream<T: AsRef<str>>(&self, stream: T) -> Result<Stream, GetStreamError> {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(GetStreamError::new(GetStreamErrorKind::EmptyName));
        }

        if !is_valid_name(stream) {
            return Err(GetStreamError::new(GetStreamErrorKind::InvalidStreamName));
        }

        let subject = format!("STREAM.INFO.{stream}");
        let request: Response<Info> = self
            .request(subject, &())
            .await
            .map_err(|err| GetStreamError::with_source(GetStreamErrorKind::Request, err))?;
        match request {
            Response::Err { error } => {
                Err(GetStreamError::new(GetStreamErrorKind::JetStream(error)))
            }
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

    /// Create a stream with the given configuration on the server if it is not present. Returns a handle to the stream on the server.
    ///
    /// Note: This does not validate if the Stream on the server is compatible with the configuration passed in.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::stream::Config;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .get_or_create_stream(Config {
    ///         name: "events".to_string(),
    ///         max_messages: 10_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_or_create_stream<S>(
        &self,
        stream_config: S,
    ) -> Result<Stream, CreateStreamError>
    where
        S: Into<Config>,
    {
        let config: Config = stream_config.into();

        if config.name.is_empty() {
            return Err(CreateStreamError::new(
                CreateStreamErrorKind::EmptyStreamName,
            ));
        }

        if !is_valid_name(config.name.as_str()) {
            return Err(CreateStreamError::new(
                CreateStreamErrorKind::InvalidStreamName,
            ));
        }
        let subject = format!("STREAM.INFO.{}", config.name);

        let request: Response<Info> = self.request(subject, &()).await?;
        match request {
            Response::Err { error } if error.code() == 404 => self.create_stream(&config).await,
            Response::Err { error } => Err(error.into()),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

    /// Deletes a [Stream] with a given name.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::stream::Config;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.delete_stream("events").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_stream<T: AsRef<str>>(
        &self,
        stream: T,
    ) -> Result<DeleteStatus, DeleteStreamError> {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(DeleteStreamError::new(DeleteStreamErrorKind::EmptyName));
        }

        if !is_valid_name(stream) {
            return Err(DeleteStreamError::new(
                DeleteStreamErrorKind::InvalidStreamName,
            ));
        }

        let subject = format!("STREAM.DELETE.{stream}");
        match self
            .request(subject, &json!({}))
            .await
            .map_err(|err| DeleteStreamError::with_source(DeleteStreamErrorKind::Request, err))?
        {
            Response::Err { error } => Err(DeleteStreamError::new(
                DeleteStreamErrorKind::JetStream(error),
            )),
            Response::Ok(delete_response) => Ok(delete_response),
        }
    }

    /// Updates a [Stream] with a given config. If specific field cannot be updated,
    /// error is returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::stream::Config;
    /// use async_nats::jetstream::stream::DiscardPolicy;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .update_stream(&Config {
    ///         name: "events".to_string(),
    ///         discard: DiscardPolicy::New,
    ///         max_messages: 50_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_stream<S>(&self, config: S) -> Result<Info, UpdateStreamError>
    where
        S: Borrow<Config>,
    {
        let config = config.borrow();

        if config.name.is_empty() {
            return Err(CreateStreamError::new(
                CreateStreamErrorKind::EmptyStreamName,
            ));
        }

        if !is_valid_name(config.name.as_str()) {
            return Err(CreateStreamError::new(
                CreateStreamErrorKind::InvalidStreamName,
            ));
        }

        let subject = format!("STREAM.UPDATE.{}", config.name);
        match self.request(subject, config).await? {
            Response::Err { error } => Err(error.into()),
            Response::Ok(info) => Ok(info),
        }
    }

    /// Lists names of all streams for current context.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::TryStreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let mut names = jetstream.stream_names();
    /// while let Some(stream) = names.try_next().await? {
    ///     println!("stream: {}", stream);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream_names(&self) -> StreamNames {
        StreamNames {
            context: self.clone(),
            offset: 0,
            page_request: None,
            streams: Vec::new(),
            done: false,
        }
    }

    /// Lists all streams info for current context.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::TryStreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let mut streams = jetstream.streams();
    /// while let Some(stream) = streams.try_next().await? {
    ///     println!("stream: {:?}", stream);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn streams(&self) -> Streams {
        Streams {
            context: self.clone(),
            offset: 0,
            page_request: None,
            streams: Vec::new(),
            done: false,
        }
    }
    /// Returns an existing key-value bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream.get_key_value("bucket").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_key_value<T: Into<String>>(&self, bucket: T) -> Result<Store, KeyValueError> {
        let bucket: String = bucket.into();
        if !crate::jetstream::kv::is_valid_bucket_name(&bucket) {
            return Err(KeyValueError::new(KeyValueErrorKind::InvalidStoreName));
        }

        let stream_name = format!("KV_{}", &bucket);
        let stream = self
            .get_stream(stream_name.clone())
            .map_err(|err| KeyValueError::with_source(KeyValueErrorKind::GetBucket, err))
            .await?;

        if stream.info.config.max_messages_per_subject < 1 {
            return Err(KeyValueError::new(KeyValueErrorKind::InvalidStoreName));
        }
        let mut store = Store {
            prefix: format!("$KV.{}.", &bucket),
            name: bucket,
            stream_name,
            stream: stream.clone(),
            put_prefix: None,
            use_jetstream_prefix: self.prefix != "$JS.API",
        };
        if let Some(ref mirror) = stream.info.config.mirror {
            let bucket = mirror.name.trim_start_matches("KV_");
            if let Some(ref external) = mirror.external {
                if !external.api_prefix.is_empty() {
                    store.use_jetstream_prefix = false;
                    store.prefix = format!("$KV.{bucket}.");
                    store.put_prefix = Some(format!("{}.$KV.{}.", external.api_prefix, bucket));
                } else {
                    store.put_prefix = Some(format!("$KV.{bucket}."));
                }
            }
        };

        Ok(store)
    }

    /// Creates a new key-value bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_key_value(
        &self,
        mut config: crate::jetstream::kv::Config,
    ) -> Result<Store, CreateKeyValueError> {
        if !crate::jetstream::kv::is_valid_bucket_name(&config.bucket) {
            return Err(CreateKeyValueError::new(
                CreateKeyValueErrorKind::InvalidStoreName,
            ));
        }

        let history = if config.history > 0 {
            if config.history > MAX_HISTORY {
                return Err(CreateKeyValueError::new(
                    CreateKeyValueErrorKind::TooLongHistory,
                ));
            }
            config.history
        } else {
            1
        };

        let num_replicas = if config.num_replicas == 0 {
            1
        } else {
            config.num_replicas
        };

        let mut subjects = Vec::new();
        if let Some(ref mut mirror) = config.mirror {
            if !mirror.name.starts_with("KV_") {
                mirror.name = format!("KV_{}", mirror.name);
            }
            config.mirror_direct = true;
        } else if let Some(ref mut sources) = config.sources {
            for source in sources {
                if !source.name.starts_with("KV_") {
                    source.name = format!("KV_{}", source.name);
                }
            }
        } else {
            subjects = vec![format!("$KV.{}.>", config.bucket)];
        }

        let stream = self
            .create_stream(stream::Config {
                name: format!("KV_{}", config.bucket),
                description: Some(config.description),
                subjects,
                max_messages_per_subject: history,
                max_bytes: config.max_bytes,
                max_age: config.max_age,
                max_message_size: config.max_value_size,
                storage: config.storage,
                republish: config.republish,
                allow_rollup: true,
                deny_delete: true,
                deny_purge: false,
                allow_direct: true,
                sources: config.sources,
                mirror: config.mirror,
                num_replicas,
                discard: stream::DiscardPolicy::New,
                mirror_direct: config.mirror_direct,
                #[cfg(feature = "server_2_10")]
                compression: if config.compression {
                    Some(stream::Compression::S2)
                } else {
                    None
                },
                placement: config.placement,
                ..Default::default()
            })
            .await
            .map_err(|err| {
                if err.kind() == CreateStreamErrorKind::TimedOut {
                    CreateKeyValueError::with_source(CreateKeyValueErrorKind::TimedOut, err)
                } else {
                    CreateKeyValueError::with_source(CreateKeyValueErrorKind::BucketCreate, err)
                }
            })?;

        let mut store = Store {
            prefix: format!("$KV.{}.", &config.bucket),
            name: config.bucket,
            stream: stream.clone(),
            stream_name: stream.info.config.name,
            put_prefix: None,
            use_jetstream_prefix: self.prefix != "$JS.API",
        };
        if let Some(ref mirror) = stream.info.config.mirror {
            let bucket = mirror.name.trim_start_matches("KV_");
            if let Some(ref external) = mirror.external {
                if !external.api_prefix.is_empty() {
                    store.use_jetstream_prefix = false;
                    store.prefix = format!("$KV.{bucket}.");
                    store.put_prefix = Some(format!("{}.$KV.{}.", external.api_prefix, bucket));
                } else {
                    store.put_prefix = Some(format!("$KV.{bucket}."));
                }
            }
        };

        Ok(store)
    }

    /// Deletes given key-value bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_key_value<T: AsRef<str>>(
        &self,
        bucket: T,
    ) -> Result<DeleteStatus, KeyValueError> {
        if !crate::jetstream::kv::is_valid_bucket_name(bucket.as_ref()) {
            return Err(KeyValueError::new(KeyValueErrorKind::InvalidStoreName));
        }

        let stream_name = format!("KV_{}", bucket.as_ref());
        self.delete_stream(stream_name)
            .map_err(|err| KeyValueError::with_source(KeyValueErrorKind::JetStream, err))
            .await
    }

    // pub async fn update_key_value<C: Borrow<kv::Config>>(&self, config: C) -> Result<(), crate::Error> {
    //     let config = config.borrow();
    //     if !crate::jetstream::kv::is_valid_bucket_name(&config.bucket) {
    //         return Err(Box::new(std::io::Error::new(
    //             ErrorKind::Other,
    //             "invalid bucket name",
    //         )));
    //     }

    //     let stream_name = format!("KV_{}", config.bucket);
    //     self.update_stream()
    //         .await
    //         .and_then(|info| Ok(()))
    // }

    /// Get a [crate::jetstream::consumer::Consumer] straight from [Context], without binding to a [Stream] first.
    ///
    /// It has one less interaction with the server when binding to only one
    /// [crate::jetstream::consumer::Consumer].
    ///
    /// # Examples:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer::PullConsumer;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_consumer_from_stream("consumer", "stream")
    ///     .await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_consumer_from_stream<T, C, S>(
        &self,
        consumer: C,
        stream: S,
    ) -> Result<Consumer<T>, ConsumerError>
    where
        T: FromConsumer + IntoConsumerConfig,
        S: AsRef<str>,
        C: AsRef<str>,
    {
        if !is_valid_name(stream.as_ref()) {
            return Err(ConsumerError::with_source(
                ConsumerErrorKind::InvalidName,
                "invalid stream",
            ));
        }

        if !is_valid_name(consumer.as_ref()) {
            return Err(ConsumerError::new(ConsumerErrorKind::InvalidName));
        }

        let subject = format!("CONSUMER.INFO.{}.{}", stream.as_ref(), consumer.as_ref());

        let info: super::consumer::Info = match self.request(subject, &json!({})).await? {
            Response::Ok(info) => info,
            Response::Err { error } => return Err(error.into()),
        };

        Ok(Consumer::new(
            T::try_from_consumer_config(info.config.clone()).map_err(|err| {
                ConsumerError::with_source(ConsumerErrorKind::InvalidConsumerType, err)
            })?,
            info,
            self.clone(),
        ))
    }

    /// Delete a [crate::jetstream::consumer::Consumer] straight from [Context], without binding to a [Stream] first.
    ///
    /// It has one less interaction with the server when binding to only one
    /// [crate::jetstream::consumer::Consumer].
    ///
    /// # Examples:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer::PullConsumer;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// jetstream
    ///     .delete_consumer_from_stream("consumer", "stream")
    ///     .await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_consumer_from_stream<C: AsRef<str>, S: AsRef<str>>(
        &self,
        consumer: C,
        stream: S,
    ) -> Result<DeleteStatus, ConsumerError> {
        if !is_valid_name(consumer.as_ref()) {
            return Err(ConsumerError::new(ConsumerErrorKind::InvalidName));
        }

        if !is_valid_name(stream.as_ref()) {
            return Err(ConsumerError::with_source(
                ConsumerErrorKind::Other,
                "invalid stream name",
            ));
        }

        let subject = format!("CONSUMER.DELETE.{}.{}", stream.as_ref(), consumer.as_ref());

        match self.request(subject, &json!({})).await? {
            Response::Ok(delete_status) => Ok(delete_status),
            Response::Err { error } => Err(error.into()),
        }
    }

    /// Create or update a `Durable` or `Ephemeral` Consumer (if `durable_name` was not provided) and
    /// returns the info from the server about created [Consumer] without binding to a [Stream] first.
    /// If you want a strict update or create, use [Context::create_consumer_strict_on_stream] or [Context::update_consumer_on_stream].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: consumer::PullConsumer = jetstream
    ///     .create_consumer_on_stream(
    ///         consumer::pull::Config {
    ///             durable_name: Some("pull".to_string()),
    ///             ..Default::default()
    ///         },
    ///         "stream",
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_consumer_on_stream<C: IntoConsumerConfig + FromConsumer, S: AsRef<str>>(
        &self,
        config: C,
        stream: S,
    ) -> Result<Consumer<C>, ConsumerError> {
        self.create_consumer_on_stream_action(config, stream, ConsumerAction::CreateOrUpdate)
            .await
    }

    /// Update an existing consumer.
    /// This call will fail if the consumer does not exist.
    /// returns the info from the server about updated [Consumer] without binding to a [Stream] first.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: consumer::PullConsumer = jetstream
    ///     .update_consumer_on_stream(
    ///         consumer::pull::Config {
    ///             durable_name: Some("pull".to_string()),
    ///             description: Some("updated pull consumer".to_string()),
    ///             ..Default::default()
    ///         },
    ///         "stream",
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "server_2_10")]
    pub async fn update_consumer_on_stream<C: IntoConsumerConfig + FromConsumer, S: AsRef<str>>(
        &self,
        config: C,
        stream: S,
    ) -> Result<Consumer<C>, ConsumerUpdateError> {
        self.create_consumer_on_stream_action(config, stream, ConsumerAction::Update)
            .await
            .map_err(|err| err.into())
    }

    /// Create consumer on stream, but only if it does not exist or the existing config is exactly
    /// the same.
    /// This method will fail if consumer is already present with different config.
    /// returns the info from the server about created [Consumer] without binding to a [Stream] first.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: consumer::PullConsumer = jetstream
    ///     .create_consumer_strict_on_stream(
    ///         consumer::pull::Config {
    ///             durable_name: Some("pull".to_string()),
    ///             ..Default::default()
    ///         },
    ///         "stream",
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "server_2_10")]
    pub async fn create_consumer_strict_on_stream<
        C: IntoConsumerConfig + FromConsumer,
        S: AsRef<str>,
    >(
        &self,
        config: C,
        stream: S,
    ) -> Result<Consumer<C>, ConsumerCreateStrictError> {
        self.create_consumer_on_stream_action(config, stream, ConsumerAction::Create)
            .await
            .map_err(|err| err.into())
    }

    async fn create_consumer_on_stream_action<
        C: IntoConsumerConfig + FromConsumer,
        S: AsRef<str>,
    >(
        &self,
        config: C,
        stream: S,
        action: ConsumerAction,
    ) -> Result<Consumer<C>, ConsumerError> {
        let config = config.into_consumer_config();

        let subject = {
            let filter = if config.filter_subject.is_empty() {
                "".to_string()
            } else {
                format!(".{}", config.filter_subject)
            };
            config
                .name
                .as_ref()
                .or(config.durable_name.as_ref())
                .map(|name| format!("CONSUMER.CREATE.{}.{}{}", stream.as_ref(), name, filter))
                .unwrap_or_else(|| format!("CONSUMER.CREATE.{}", stream.as_ref()))
        };

        match self
            .request(
                subject,
                &json!({"stream_name": stream.as_ref(), "config": config, "action": action}),
            )
            .await?
        {
            Response::Err { error } => Err(ConsumerError::new(ConsumerErrorKind::JetStream(error))),
            Response::Ok::<consumer::Info>(info) => Ok(Consumer::new(
                FromConsumer::try_from_consumer_config(info.clone().config)
                    .map_err(|err| ConsumerError::with_source(ConsumerErrorKind::Other, err))?,
                info,
                self.clone(),
            )),
        }
    }

    /// Send a request to the jetstream JSON API.
    ///
    /// This is a low level API used mostly internally, that should be used only in
    /// specific cases when this crate API on [Consumer] or [Stream] does not provide needed functionality.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use async_nats::jetstream::stream::Info;
    /// # use async_nats::jetstream::response::Response;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let response: Response<Info> = jetstream.request("STREAM.INFO.events", &()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request<S, T, V>(&self, subject: S, payload: &T) -> Result<V, RequestError>
    where
        S: ToSubject,
        T: ?Sized + Serialize,
        V: DeserializeOwned,
    {
        let subject = subject.to_subject();
        let request = serde_json::to_vec(&payload)
            .map(Bytes::from)
            .map_err(|err| RequestError::with_source(RequestErrorKind::Other, err))?;

        debug!("JetStream request sent: {:?}", request);

        let message = self
            .client
            .request(format!("{}.{}", self.prefix, subject.as_ref()), request)
            .await;
        let message = message?;
        debug!(
            "JetStream request response: {:?}",
            from_utf8(&message.payload)
        );
        let response = serde_json::from_slice(message.payload.as_ref())
            .map_err(|err| RequestError::with_source(RequestErrorKind::Other, err))?;

        Ok(response)
    }

    /// Creates a new object store bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let bucket = jetstream
    ///     .create_object_store(async_nats::jetstream::object_store::Config {
    ///         bucket: "bucket".to_string(),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_object_store(
        &self,
        config: super::object_store::Config,
    ) -> Result<super::object_store::ObjectStore, CreateObjectStoreError> {
        if !super::object_store::is_valid_bucket_name(&config.bucket) {
            return Err(CreateObjectStoreError::new(
                CreateKeyValueErrorKind::InvalidStoreName,
            ));
        }

        let bucket_name = config.bucket.clone();
        let stream_name = format!("OBJ_{bucket_name}");
        let chunk_subject = format!("$O.{bucket_name}.C.>");
        let meta_subject = format!("$O.{bucket_name}.M.>");

        let stream = self
            .create_stream(super::stream::Config {
                name: stream_name,
                description: config.description.clone(),
                subjects: vec![chunk_subject, meta_subject],
                max_age: config.max_age,
                max_bytes: config.max_bytes,
                storage: config.storage,
                num_replicas: config.num_replicas,
                discard: DiscardPolicy::New,
                allow_rollup: true,
                allow_direct: true,
                #[cfg(feature = "server_2_10")]
                compression: if config.compression {
                    Some(Compression::S2)
                } else {
                    None
                },
                placement: config.placement,
                ..Default::default()
            })
            .await
            .map_err(|err| {
                CreateObjectStoreError::with_source(CreateKeyValueErrorKind::BucketCreate, err)
            })?;

        Ok(ObjectStore {
            name: bucket_name,
            stream,
        })
    }

    /// Get an existing object store bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let bucket = jetstream.get_object_store("bucket").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_object_store<T: AsRef<str>>(
        &self,
        bucket_name: T,
    ) -> Result<ObjectStore, ObjectStoreError> {
        let bucket_name = bucket_name.as_ref();
        if !is_valid_bucket_name(bucket_name) {
            return Err(ObjectStoreError::new(
                ObjectStoreErrorKind::InvalidBucketName,
            ));
        }
        let stream_name = format!("OBJ_{bucket_name}");
        let stream = self
            .get_stream(stream_name)
            .await
            .map_err(|err| ObjectStoreError::with_source(ObjectStoreErrorKind::GetStore, err))?;

        Ok(ObjectStore {
            name: bucket_name.to_string(),
            stream,
        })
    }

    /// Delete a object store bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let bucket = jetstream.delete_object_store("bucket").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_object_store<T: AsRef<str>>(
        &self,
        bucket_name: T,
    ) -> Result<(), DeleteObjectStore> {
        let stream_name = format!("OBJ_{}", bucket_name.as_ref());
        self.delete_stream(stream_name)
            .await
            .map_err(|err| ObjectStoreError::with_source(ObjectStoreErrorKind::GetStore, err))?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PublishErrorKind {
    StreamNotFound,
    WrongLastMessageId,
    WrongLastSequence,
    TimedOut,
    BrokenPipe,
    Other,
}

impl Display for PublishErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StreamNotFound => write!(f, "no stream found for given subject"),
            Self::TimedOut => write!(f, "timed out: didn't receive ack in time"),
            Self::Other => write!(f, "publish failed"),
            Self::BrokenPipe => write!(f, "broken pipe"),
            Self::WrongLastMessageId => write!(f, "wrong last message id"),
            Self::WrongLastSequence => write!(f, "wrong last sequence"),
        }
    }
}

pub type PublishError = Error<PublishErrorKind>;

#[derive(Debug)]
pub struct PublishAckFuture {
    timeout: Duration,
    subscription: oneshot::Receiver<Message>,
}

impl PublishAckFuture {
    async fn next_with_timeout(self) -> Result<PublishAck, PublishError> {
        let next = tokio::time::timeout(self.timeout, self.subscription)
            .await
            .map_err(|_| PublishError::new(PublishErrorKind::TimedOut))?;
        next.map_or_else(
            |_| Err(PublishError::new(PublishErrorKind::BrokenPipe)),
            |m| {
                if m.status == Some(StatusCode::NO_RESPONDERS) {
                    return Err(PublishError::new(PublishErrorKind::StreamNotFound));
                }
                let response = serde_json::from_slice(m.payload.as_ref())
                    .map_err(|err| PublishError::with_source(PublishErrorKind::Other, err))?;
                match response {
                    Response::Err { error } => match error.error_code() {
                        ErrorCode::STREAM_WRONG_LAST_MESSAGE_ID => Err(PublishError::with_source(
                            PublishErrorKind::WrongLastMessageId,
                            error,
                        )),
                        ErrorCode::STREAM_WRONG_LAST_SEQUENCE => Err(PublishError::with_source(
                            PublishErrorKind::WrongLastSequence,
                            error,
                        )),
                        _ => Err(PublishError::with_source(PublishErrorKind::Other, error)),
                    },
                    Response::Ok(publish_ack) => Ok(publish_ack),
                }
            },
        )
    }
}
impl IntoFuture for PublishAckFuture {
    type Output = Result<PublishAck, PublishError>;

    type IntoFuture = Pin<Box<dyn Future<Output = Result<PublishAck, PublishError>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(std::future::IntoFuture::into_future(
            self.next_with_timeout(),
        ))
    }
}

#[derive(Deserialize, Debug)]
struct StreamPage {
    total: usize,
    streams: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
struct StreamInfoPage {
    total: usize,
    streams: Option<Vec<super::stream::Info>>,
}

type PageRequest = BoxFuture<'static, Result<StreamPage, RequestError>>;

pub struct StreamNames {
    context: Context,
    offset: usize,
    page_request: Option<PageRequest>,
    streams: Vec<String>,
    done: bool,
}

impl futures::Stream for StreamNames {
    type Item = Result<String, StreamsError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.page_request.as_mut() {
            Some(page) => match page.try_poll_unpin(cx) {
                std::task::Poll::Ready(page) => {
                    self.page_request = None;
                    let page = page
                        .map_err(|err| StreamsError::with_source(StreamsErrorKind::Other, err))?;
                    if let Some(streams) = page.streams {
                        self.offset += streams.len();
                        self.streams = streams;
                        if self.offset >= page.total {
                            self.done = true;
                        }
                        match self.streams.pop() {
                            Some(stream) => Poll::Ready(Some(Ok(stream))),
                            None => Poll::Ready(None),
                        }
                    } else {
                        Poll::Ready(None)
                    }
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            None => {
                if let Some(stream) = self.streams.pop() {
                    Poll::Ready(Some(Ok(stream)))
                } else {
                    if self.done {
                        return Poll::Ready(None);
                    }
                    let context = self.context.clone();
                    let offset = self.offset;
                    self.page_request = Some(Box::pin(async move {
                        match context
                            .request(
                                "STREAM.NAMES",
                                &json!({
                                    "offset": offset,
                                }),
                            )
                            .await?
                        {
                            Response::Err { error } => {
                                Err(RequestError::with_source(RequestErrorKind::Other, error))
                            }
                            Response::Ok(page) => Ok(page),
                        }
                    }));
                    self.poll_next(cx)
                }
            }
        }
    }
}

type PageInfoRequest = BoxFuture<'static, Result<StreamInfoPage, RequestError>>;

pub type StreamsErrorKind = RequestErrorKind;
pub type StreamsError = RequestError;

pub struct Streams {
    context: Context,
    offset: usize,
    page_request: Option<PageInfoRequest>,
    streams: Vec<super::stream::Info>,
    done: bool,
}

impl futures::Stream for Streams {
    type Item = Result<super::stream::Info, StreamsError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.page_request.as_mut() {
            Some(page) => match page.try_poll_unpin(cx) {
                std::task::Poll::Ready(page) => {
                    self.page_request = None;
                    let page = page
                        .map_err(|err| StreamsError::with_source(StreamsErrorKind::Other, err))?;
                    if let Some(streams) = page.streams {
                        self.offset += streams.len();
                        self.streams = streams;
                        if self.offset >= page.total {
                            self.done = true;
                        }
                        match self.streams.pop() {
                            Some(stream) => Poll::Ready(Some(Ok(stream))),
                            None => Poll::Ready(None),
                        }
                    } else {
                        Poll::Ready(None)
                    }
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            None => {
                if let Some(stream) = self.streams.pop() {
                    Poll::Ready(Some(Ok(stream)))
                } else {
                    if self.done {
                        return Poll::Ready(None);
                    }
                    let context = self.context.clone();
                    let offset = self.offset;
                    self.page_request = Some(Box::pin(async move {
                        match context
                            .request(
                                "STREAM.LIST",
                                &json!({
                                    "offset": offset,
                                }),
                            )
                            .await?
                        {
                            Response::Err { error } => {
                                Err(RequestError::with_source(RequestErrorKind::Other, error))
                            }
                            Response::Ok(page) => Ok(page),
                        }
                    }));
                    self.poll_next(cx)
                }
            }
        }
    }
}
/// Used for building customized `publish` message.
#[derive(Default, Clone, Debug)]
pub struct Publish {
    payload: Bytes,
    headers: Option<header::HeaderMap>,
}
impl Publish {
    /// Creates a new custom Publish struct to be used with.
    pub fn build() -> Self {
        Default::default()
    }

    /// Sets the payload for the message.
    pub fn payload(mut self, payload: Bytes) -> Self {
        self.payload = payload;
        self
    }
    /// Adds headers to the message.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = Some(headers);
        self
    }
    /// A shorthand to add a single header.
    pub fn header<N: IntoHeaderName, V: IntoHeaderValue>(mut self, name: N, value: V) -> Self {
        self.headers
            .get_or_insert(header::HeaderMap::new())
            .insert(name, value);
        self
    }
    /// Sets the `Nats-Msg-Id` header, that is used by stream deduplicate window.
    pub fn message_id<T: AsRef<str>>(self, id: T) -> Self {
        self.header(header::NATS_MESSAGE_ID, id.as_ref())
    }
    /// Sets expected last message ID.
    /// It sets the `Nats-Expected-Last-Msg-Id` header with provided value.
    pub fn expected_last_message_id<T: AsRef<str>>(self, last_message_id: T) -> Self {
        self.header(
            header::NATS_EXPECTED_LAST_MESSAGE_ID,
            last_message_id.as_ref(),
        )
    }
    /// Sets the last expected stream sequence.
    /// It sets the `Nats-Expected-Last-Sequence` header with provided value.
    pub fn expected_last_sequence(self, last_sequence: u64) -> Self {
        self.header(
            header::NATS_EXPECTED_LAST_SEQUENCE,
            HeaderValue::from(last_sequence),
        )
    }
    /// Sets the last expected stream sequence for a subject this message will be published to.
    /// It sets the `Nats-Expected-Last-Subject-Sequence` header with provided value.
    pub fn expected_last_subject_sequence(self, subject_sequence: u64) -> Self {
        self.header(
            header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
            HeaderValue::from(subject_sequence),
        )
    }
    /// Sets the expected stream name.
    /// It sets the `Nats-Expected-Stream` header with provided value.
    pub fn expected_stream<T: AsRef<str>>(self, stream: T) -> Self {
        self.header(
            header::NATS_EXPECTED_STREAM,
            HeaderValue::from(stream.as_ref()),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RequestErrorKind {
    NoResponders,
    TimedOut,
    Other,
}

impl Display for RequestErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out"),
            Self::Other => write!(f, "request failed"),
            Self::NoResponders => write!(f, "requested JetStream resource does not exist"),
        }
    }
}

pub type RequestError = Error<RequestErrorKind>;

impl From<crate::RequestError> for RequestError {
    fn from(error: crate::RequestError) -> Self {
        match error.kind() {
            crate::RequestErrorKind::TimedOut => {
                RequestError::with_source(RequestErrorKind::TimedOut, error)
            }
            crate::RequestErrorKind::NoResponders => {
                RequestError::new(RequestErrorKind::NoResponders)
            }
            crate::RequestErrorKind::Other => {
                RequestError::with_source(RequestErrorKind::Other, error)
            }
        }
    }
}

impl From<super::errors::Error> for RequestError {
    fn from(err: super::errors::Error) -> Self {
        RequestError::with_source(RequestErrorKind::Other, err)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CreateStreamErrorKind {
    EmptyStreamName,
    InvalidStreamName,
    DomainAndExternalSet,
    JetStreamUnavailable,
    JetStream(super::errors::Error),
    TimedOut,
    Response,
    ResponseParse,
}

impl Display for CreateStreamErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyStreamName => write!(f, "stream name cannot be empty"),
            Self::InvalidStreamName => write!(f, "stream name cannot contain `.`, `_`"),
            Self::DomainAndExternalSet => write!(f, "domain and external are both set"),
            Self::JetStream(err) => write!(f, "jetstream error: {}", err),
            Self::TimedOut => write!(f, "jetstream request timed out"),
            Self::JetStreamUnavailable => write!(f, "jetstream unavailable"),
            Self::ResponseParse => write!(f, "failed to parse server response"),
            Self::Response => write!(f, "response error"),
        }
    }
}

pub type CreateStreamError = Error<CreateStreamErrorKind>;

impl From<super::errors::Error> for CreateStreamError {
    fn from(error: super::errors::Error) -> Self {
        CreateStreamError::new(CreateStreamErrorKind::JetStream(error))
    }
}

impl From<RequestError> for CreateStreamError {
    fn from(error: RequestError) -> Self {
        match error.kind() {
            RequestErrorKind::NoResponders => {
                CreateStreamError::new(CreateStreamErrorKind::JetStreamUnavailable)
            }
            RequestErrorKind::TimedOut => CreateStreamError::new(CreateStreamErrorKind::TimedOut),
            RequestErrorKind::Other => {
                CreateStreamError::with_source(CreateStreamErrorKind::Response, error)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum GetStreamErrorKind {
    EmptyName,
    Request,
    InvalidStreamName,
    JetStream(super::errors::Error),
}

impl Display for GetStreamErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyName => write!(f, "empty name cannot be empty"),
            Self::Request => write!(f, "request error"),
            Self::InvalidStreamName => write!(f, "invalid stream name"),
            Self::JetStream(err) => write!(f, "jetstream error: {}", err),
        }
    }
}

pub type GetStreamError = Error<GetStreamErrorKind>;

pub type UpdateStreamError = CreateStreamError;
pub type UpdateStreamErrorKind = CreateStreamErrorKind;
pub type DeleteStreamError = GetStreamError;
pub type DeleteStreamErrorKind = GetStreamErrorKind;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum KeyValueErrorKind {
    InvalidStoreName,
    GetBucket,
    JetStream,
}

impl Display for KeyValueErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidStoreName => write!(f, "invalid Key Value Store name"),
            Self::GetBucket => write!(f, "failed to get the bucket"),
            Self::JetStream => write!(f, "JetStream error"),
        }
    }
}

pub type KeyValueError = Error<KeyValueErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CreateKeyValueErrorKind {
    InvalidStoreName,
    TooLongHistory,
    JetStream,
    BucketCreate,
    TimedOut,
}

impl Display for CreateKeyValueErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidStoreName => write!(f, "invalid Key Value Store name"),
            Self::TooLongHistory => write!(f, "too long history"),
            Self::JetStream => write!(f, "JetStream error"),
            Self::BucketCreate => write!(f, "bucket creation failed"),
            Self::TimedOut => write!(f, "timed out"),
        }
    }
}

pub type CreateKeyValueError = Error<CreateKeyValueErrorKind>;

pub type CreateObjectStoreError = CreateKeyValueError;
pub type CreateObjectStoreErrorKind = CreateKeyValueErrorKind;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ObjectStoreErrorKind {
    InvalidBucketName,
    GetStore,
}

impl Display for ObjectStoreErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBucketName => write!(f, "invalid Object Store bucket name"),
            Self::GetStore => write!(f, "failed to get Object Store"),
        }
    }
}

pub type ObjectStoreError = Error<ObjectStoreErrorKind>;

pub type DeleteObjectStore = ObjectStoreError;
pub type DeleteObjectStoreKind = ObjectStoreErrorKind;

#[derive(Clone, Debug, PartialEq)]
pub enum AccountErrorKind {
    TimedOut,
    JetStream(super::errors::Error),
    JetStreamUnavailable,
    Other,
}

impl Display for AccountErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out"),
            Self::JetStream(err) => write!(f, "JetStream error: {}", err),
            Self::Other => write!(f, "error"),
            Self::JetStreamUnavailable => write!(f, "JetStream unavailable"),
        }
    }
}

pub type AccountError = Error<AccountErrorKind>;

impl From<RequestError> for AccountError {
    fn from(err: RequestError) -> Self {
        match err.kind {
            RequestErrorKind::NoResponders => {
                AccountError::with_source(AccountErrorKind::JetStreamUnavailable, err)
            }
            RequestErrorKind::TimedOut => AccountError::new(AccountErrorKind::TimedOut),
            RequestErrorKind::Other => AccountError::with_source(AccountErrorKind::Other, err),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
enum ConsumerAction {
    #[serde(rename = "")]
    CreateOrUpdate,
    #[serde(rename = "create")]
    #[cfg(feature = "server_2_10")]
    Create,
    #[serde(rename = "update")]
    #[cfg(feature = "server_2_10")]
    Update,
}
