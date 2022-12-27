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
//! Manage operations on [Context], create/delete/update [Stream][crate::jetstream::stream::Stream]

use crate::header::{IntoHeaderName, IntoHeaderValue};
use crate::jetstream::account::Account;
use crate::jetstream::publish::PublishAck;
use crate::jetstream::response::Response;
use crate::{header, Client, Command, Error, HeaderMap, HeaderValue};
use bytes::Bytes;
use futures::{Future, StreamExt, TryFutureExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{self, json};
use std::borrow::Borrow;
use std::future::IntoFuture;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::str::from_utf8;
use std::task::Poll;
use std::time::Duration;
use tracing::debug;

use super::kv::{Store, MAX_HISTORY};
use super::object_store::{is_valid_bucket_name, ObjectStore};
use super::stream::{self, Config, DeleteStatus, DiscardPolicy, External, Info, Stream};

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
    /// let ack = jetstream.publish("events".to_string(), "data".into()).await?;
    /// ack.await?;
    /// jetstream.publish("events".to_string(), "data".into())
    ///     .await?
    ///     .await?;
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
    /// let first_ack = jetstream.publish("events".to_string(), "data".into()).await?;
    /// let second_ack = jetstream.publish("events".to_string(), "data".into()).await?;
    /// first_ack.await?;
    /// second_ack.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish(
        &self,
        subject: String,
        payload: Bytes,
    ) -> Result<PublishAckFuture, Error> {
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
    /// let ack = jetstream.publish_with_headers("events".to_string(), headers, "data".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_with_headers(
        &self,
        subject: String,
        headers: crate::header::HeaderMap,
        payload: Bytes,
    ) -> Result<PublishAckFuture, Error> {
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
    /// let ack =
    /// jetstream.send_publish("events".to_string(),
    ///     Publish::build().payload("data".into()).message_id("uuid")
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_publish(
        &self,
        subject: String,
        publish: Publish,
    ) -> Result<PublishAckFuture, Error> {
        let inbox = self.client.new_inbox();
        let response = self.client.subscribe(inbox.clone()).await?;
        tokio::time::timeout(self.timeout, async {
            if let Some(headers) = publish.headers {
                self.client
                    .publish_with_reply_and_headers(
                        subject,
                        inbox.clone(),
                        headers,
                        publish.payload,
                    )
                    .await
            } else {
                self.client
                    .publish_with_reply(subject, inbox.clone(), publish.payload)
                    .await
            }
        })
        .map_err(|_| {
            std::io::Error::new(ErrorKind::TimedOut, "JetStream publish request timed out")
        })
        .await??;

        Ok(PublishAckFuture {
            timeout: self.timeout,
            subscription: response,
        })
    }

    /// Query the server for account information
    pub async fn query_account(&self) -> Result<Account, Error> {
        let response: Response<Account> = self.request("INFO".into(), b"").await?;

        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while querying account information: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(account) => Ok(account),
        }
    }

    /// Create a JetStream [Stream] with given config and return a handle to it.
    /// That handle can be used to manage and use [Consumer][crate::jetstream::consumer::Consumer].
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
    /// let stream = jetstream.create_stream(Config {
    ///     name: "events".to_string(),
    ///     max_messages: 100_000,
    ///     discard: DiscardPolicy::Old,
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_stream<S>(&self, stream_config: S) -> Result<Stream, Error>
    where
        Config: From<S>,
    {
        let mut config: Config = stream_config.into();
        if config.name.is_empty() {
            return Err(Box::new(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            )));
        }
        if let Some(ref mut mirror) = config.mirror {
            if let Some(ref mut domain) = mirror.domain {
                if mirror.external.is_some() {
                    return Err(Box::new(io::Error::new(
                        ErrorKind::Other,
                        "domain and external are both set",
                    )));
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
                        return Err(Box::new(io::Error::new(
                            ErrorKind::Other,
                            "domain and external are both set",
                        )));
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
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while creating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

    /// Checks for [Stream] existence on the server and returns handle to it.
    /// That handle can be used to manage and use [Consumer][crate::jetstream::consumer::Consumer].
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
    pub async fn get_stream<T: AsRef<str>>(&self, stream: T) -> Result<Stream, Error> {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(Box::new(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            )));
        }

        let subject = format!("STREAM.INFO.{stream}");
        let request: Response<Info> = self.request(subject, &()).await?;
        match request {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

    /// Create a stream with the given configuration on the server if it is not present. Returns a handle to the stream  on the server.
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
    /// let stream = jetstream.get_or_create_stream(Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_or_create_stream<S>(&self, stream_config: S) -> Result<Stream, Error>
    where
        S: Into<Config>,
    {
        let config: Config = stream_config.into();
        let subject = format!("STREAM.INFO.{}", config.name);

        let request: Response<Info> = self.request(subject, &()).await?;
        match request {
            Response::Err { error } if error.status == 404 => self.create_stream(&config).await,
            Response::Err { error } => Err(Box::new(io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting or creating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
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
    pub async fn delete_stream<T: AsRef<str>>(&self, stream: T) -> Result<DeleteStatus, Error> {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(Box::new(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            )));
        }
        let subject = format!("STREAM.DELETE.{stream}");
        match self.request(subject, &json!({})).await? {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while deleting stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
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
    /// let stream = jetstream.update_stream(&Config {
    ///     name: "events".to_string(),
    ///     discard: DiscardPolicy::New,
    ///     max_messages: 50_000,
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_stream<S>(&self, config: S) -> Result<Info, Error>
    where
        S: Borrow<Config>,
    {
        let config = config.borrow();
        let subject = format!("STREAM.UPDATE.{}", config.name);
        match self.request(subject, config).await? {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while updating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
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
    /// let mut names = jetstream.list_stream_names();
    /// while let Some(stream) = names.try_next().await? {
    ///     println!("stream: {}", stream);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_stream_names(&self) -> StreamNamesList {
        StreamNamesList {
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
    pub async fn get_key_value<T: Into<String>>(&self, bucket: T) -> Result<Store, Error> {
        let bucket: String = bucket.into();
        if !crate::jetstream::kv::is_valid_bucket_name(&bucket) {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "invalid bucket name",
            )));
        }

        let stream_name = format!("KV_{}", &bucket);
        let stream = self.get_stream(stream_name.clone()).await?;

        if stream.info.config.max_messages_per_subject < 1 {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "not a valid key-value store",
            )));
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
    /// let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
    ///     bucket: "kv".to_string(),
    ///     history: 10,
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_key_value(
        &self,
        mut config: crate::jetstream::kv::Config,
    ) -> Result<Store, Error> {
        if !crate::jetstream::kv::is_valid_bucket_name(&config.bucket) {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "invalid bucket name",
            )));
        }

        let history = if config.history > 0 {
            if config.history > MAX_HISTORY {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::Other,
                    "history limited to a max of 64",
                )));
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
                ..Default::default()
            })
            .await?;

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
    /// let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
    ///     bucket: "kv".to_string(),
    ///     history: 10,
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_key_value<T: AsRef<str>>(&self, bucket: T) -> Result<DeleteStatus, Error> {
        if !crate::jetstream::kv::is_valid_bucket_name(bucket.as_ref()) {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "invalid bucket name",
            )));
        }

        let stream_name = format!("KV_{}", bucket.as_ref());
        self.delete_stream(stream_name).await
    }

    // pub async fn update_key_value<C: Borrow<kv::Config>>(&self, config: C) -> Result<(), Error> {
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

    /// Send a request to the jetstream JSON API.
    ///
    /// This is a low level API used mostly internally, that should be used only in
    /// specific cases when this crate API on [Consumer][crate::jetstream::consumer::Consumer] or [Stream] does not provide needed functionality.
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
    /// let response: Response<Info> = jetstream
    /// .request("STREAM.INFO.events".to_string(), &()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request<T, V>(&self, subject: String, payload: &T) -> Result<Response<V>, Error>
    where
        T: ?Sized + Serialize,
        V: DeserializeOwned,
    {
        let request = serde_json::to_vec(&payload).map(Bytes::from)?;

        debug!("JetStream request sent: {:?}", request);

        let message = self
            .client
            .request(format!("{}.{}", self.prefix, subject), request)
            .await?;
        debug!(
            "JetStream request response: {:?}",
            from_utf8(&message.payload)
        );
        let response = serde_json::from_slice(message.payload.as_ref())?;

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
    /// let bucket = jetstream.create_object_store(async_nats::jetstream::object_store::Config {
    ///     bucket: "bucket".to_string(),
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_object_store(
        &self,
        config: super::object_store::Config,
    ) -> Result<super::object_store::ObjectStore, Error> {
        if !super::object_store::is_valid_bucket_name(&config.bucket) {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "invalid bucket name",
            )));
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
                storage: config.storage,
                num_replicas: config.num_replicas,
                discard: DiscardPolicy::New,
                allow_rollup: true,
                ..Default::default()
            })
            .await?;

        Ok(ObjectStore {
            name: bucket_name,
            stream,
        })
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
    /// let bucket = jetstream.get_object_store("bucket").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_object_store<T: AsRef<str>>(
        &self,
        bucket_name: T,
    ) -> Result<ObjectStore, Error> {
        if !self.client.is_server_compatible(2, 6, 2) {
            return Err(Box::new(io::Error::new(
                ErrorKind::Other,
                "object-store requires at least server version 2.6.2",
            )));
        }
        let bucket_name = bucket_name.as_ref();
        if !is_valid_bucket_name(bucket_name) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid bucket name",
            )));
        }
        let stream_name = format!("OBJ_{bucket_name}");
        let stream = self.get_stream(stream_name).await?;

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
    pub async fn delete_object_store<T: AsRef<str>>(&self, bucket_name: T) -> Result<(), Error> {
        let stream_name = format!("OBJ_{}", bucket_name.as_ref());
        self.delete_stream(stream_name).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PublishAckFuture {
    timeout: Duration,
    subscription: crate::Subscriber,
}

impl PublishAckFuture {
    async fn next_with_timeout(mut self) -> Result<PublishAck, Error> {
        self.subscription.sender.send(Command::TryFlush).await.ok();
        let next = tokio::time::timeout(self.timeout, self.subscription.next())
            .await
            .map_err(|_| std::io::Error::new(ErrorKind::TimedOut, "acknowledgment timed out"))?;
        next.map_or_else(
            || {
                Err(Box::from(std::io::Error::new(
                    ErrorKind::Other,
                    "broken pipe",
                )))
            },
            |m| {
                let response = serde_json::from_slice(m.payload.as_ref())?;
                match response {
                    Response::Err { error } => Err(Box::from(std::io::Error::new(
                        ErrorKind::Other,
                        format!(
                            "nats: error while publishing message: {}, {}, {}",
                            error.code, error.status, error.description
                        ),
                    ))),
                    Response::Ok(publish_ack) => Ok(publish_ack),
                }
            },
        )
    }
}
impl IntoFuture for PublishAckFuture {
    type Output = Result<PublishAck, Error>;

    type IntoFuture = Pin<Box<dyn Future<Output = Result<PublishAck, Error>> + Send>>;

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

type PageRequest = Pin<Box<dyn Future<Output = Result<StreamPage, Error>>>>;

pub struct StreamNamesList {
    context: Context,
    offset: usize,
    page_request: Option<PageRequest>,
    streams: Vec<String>,
    done: bool,
}

impl futures::Stream for StreamNamesList {
    type Item = Result<String, Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.page_request.as_mut() {
            Some(page) => match page.try_poll_unpin(cx) {
                std::task::Poll::Ready(page) => {
                    self.page_request = None;
                    let page = page?;
                    if let Some(streams) = page.streams {
                        self.offset += streams.len();
                        self.streams = streams;
                        if self.offset >= page.total {
                            self.done = true;
                        }
                        match self.streams.pop() {
                            Some(stream) => return Poll::Ready(Some(Ok(stream))),
                            None => return Poll::Ready(None),
                        };
                    }
                    Poll::Ready(None)
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
                                "STREAM.NAMES".to_string(),
                                &json!({
                                    "offset": offset,
                                }),
                            )
                            .await?
                        {
                            Response::Err { error } => {
                                Err(Box::from(std::io::Error::new(ErrorKind::Other, error)))
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
