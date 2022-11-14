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

pub mod bucket;

use std::{
    collections::{self, HashSet},
    io::{self, ErrorKind},
    task::Poll,
};

use crate::{HeaderValue, StatusCode};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use regex::Regex;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::{header, jetstream::response, Error, Message};

use self::bucket::Status;

use super::{
    consumer::DeliverPolicy,
    stream::{RawMessage, Republish, Source, StorageType, Stream},
};

// Helper to extract key value operation from message headers
fn kv_operation_from_maybe_headers(maybe_headers: Option<&String>) -> Operation {
    if let Some(headers) = maybe_headers {
        return match headers.as_str() {
            KV_OPERATION_DELETE => Operation::Delete,
            KV_OPERATION_PURGE => Operation::Purge,
            _ => Operation::Put,
        };
    }

    Operation::Put
}

fn kv_operation_from_stream_message(message: &RawMessage) -> Operation {
    kv_operation_from_maybe_headers(message.headers.as_ref())
}
lazy_static! {
    static ref VALID_BUCKET_RE: Regex = Regex::new(r#"\A[a-zA-Z0-9_-]+\z"#).unwrap();
    static ref VALID_KEY_RE: Regex = Regex::new(r#"\A[-/_=\.a-zA-Z0-9]+\z"#).unwrap();
}

pub(crate) const MAX_HISTORY: i64 = 64;
const ALL_KEYS: &str = ">";

const KV_OPERATION: &str = "KV-Operation";
const KV_OPERATION_DELETE: &str = "DEL";
const KV_OPERATION_PURGE: &str = "PURGE";
const KV_OPERATION_PUT: &str = "PUT";

const NATS_ROLLUP: &str = "Nats-Rollup";
const ROLLUP_SUBJECT: &str = "sub";

pub(crate) fn is_valid_bucket_name(bucket_name: &str) -> bool {
    VALID_BUCKET_RE.is_match(bucket_name)
}

pub(crate) fn is_valid_key(key: &str) -> bool {
    if key.is_empty() || key.starts_with('.') || key.ends_with('.') {
        return false;
    }

    VALID_KEY_RE.is_match(key)
}

/// Configuration values for key value stores.
#[derive(Debug, Default)]
pub struct Config {
    /// Name of the bucket
    pub bucket: String,
    /// Human readable description.
    pub description: String,
    /// Maximum size of a single value.
    pub max_value_size: i32,
    /// Maximum historical entries.
    pub history: i64,
    /// Maximum age of any entry in the bucket, expressed in nanoseconds
    pub max_age: std::time::Duration,
    /// How large the bucket may become in total bytes before the configured discard policy kicks in
    pub max_bytes: i64,
    /// The type of storage backend, `File` (default) and `Memory`
    pub storage: StorageType,
    /// How many replicas to keep for each entry in a cluster.
    pub num_replicas: usize,
    /// Republish is for republishing messages once persistent in the Key Value Bucket.
    pub republish: Option<Republish>,
    /// Bucket mirror configuration.
    pub mirror: Option<Source>,
    /// Bucket sources configuration.
    pub sources: Option<Vec<Source>>,
    /// Allow mirrors using direct API.
    pub mirror_direct: bool,
}

/// Describes what kind of operation and entry represents
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Operation {
    /// A value was put into the bucket
    Put,
    /// A value was deleted from a bucket
    Delete,
    /// A value was purged from a bucket
    Purge,
}

/// A struct used as a handle for the bucket.
#[derive(Debug, Clone)]
pub struct Store {
    pub name: String,
    pub stream_name: String,
    pub prefix: String,
    pub put_prefix: Option<String>,
    pub use_jetstream_prefix: bool,
    pub stream: Stream,
}

impl Store {
    /// Queries the server and returns status from the server.
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
    /// let status = kv.status().await?;
    /// println!("status: {:?}", status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn status(&self) -> Result<Status, Error> {
        // TODO: should we poll for fresh info here? probably yes.
        let info = self.stream.info.clone();

        Ok(Status {
            info,
            bucket: self.name.to_string(),
        })
    }

    /// Puts new key value pair into the bucket.
    /// If key didn't exist, it is created. If it did exist, a new value with a new version is
    /// added.
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
    /// let status = kv.put("key", "value".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put<T: AsRef<str>>(&self, key: T, value: bytes::Bytes) -> Result<u64, Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }
        let mut subject = String::new();
        if self.use_jetstream_prefix {
            subject.push_str(&self.stream.context.prefix);
            subject.push('.');
        }
        subject.push_str(self.put_prefix.as_ref().unwrap_or(&self.prefix));
        subject.push_str(key.as_ref());

        let publish_ack = self.stream.context.publish(subject, value).await?;
        let ack = publish_ack.await?;

        Ok(ack.sequence)
    }

    /// Retrieves the last [Entry] for a given key from a bucket.
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
    /// let status = kv.put("key", "value".into()).await?;
    /// let entry = kv.entry("key").await?;
    /// println!("entry: {:?}", entry);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn entry<T: Into<String>>(&self, key: T) -> Result<Option<Entry>, Error> {
        let key: String = key.into();
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }

        let subject = format!("{}{}", self.prefix.as_str(), &key);

        let result: Option<(Message, Operation, u64, OffsetDateTime)> = {
            if self.stream.info.config.allow_direct {
                let message = self
                    .stream
                    .direct_get_last_for_subject(subject.as_str())
                    .await;

                match message {
                    Ok(message) => {
                        let headers = message.headers.as_ref().ok_or_else(|| {
                            std::io::Error::new(io::ErrorKind::Other, "did not found headers")
                        })?;
                        let operation = headers.get(KV_OPERATION).map_or_else(
                            || Operation::Put,
                            |operation| match operation
                                .iter()
                                .next()
                                .cloned()
                                .unwrap_or_else(|| KV_OPERATION_PUT.to_string())
                                .as_ref()
                            {
                                KV_OPERATION_PURGE => Operation::Purge,
                                KV_OPERATION_DELETE => Operation::Delete,
                                _ => Operation::Put,
                            },
                        );
                        let sequence = headers
                            .get(header::NATS_SEQUENCE)
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::NotFound,
                                    "did not found sequence header",
                                )
                            })?
                            .iter()
                            .next()
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::NotFound,
                                    "did not found sequence header value",
                                )
                            })?
                            .parse()?;
                        let created = headers
                            .get(header::NATS_TIME_STAMP)
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::NotFound,
                                    "did not found timestamp header",
                                )
                            })?
                            .iter()
                            .next()
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::NotFound,
                                    "did not found timestamp header value",
                                )
                            })
                            .and_then(|created| {
                                OffsetDateTime::parse(created, &Rfc3339).map_err(|err| {
                                    std::io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("failed to parse Nats-Time-Stamp: {}", err),
                                    )
                                })
                            })?;

                        Some((message.message, operation, sequence, created))
                    }
                    Err(err) => {
                        let e: std::io::Error = *err.downcast().unwrap();
                        if e.kind() == ErrorKind::NotFound {
                            None
                        } else {
                            return Err(Box::new(e));
                        }
                    }
                }
            } else {
                let raw_message = self
                    .stream
                    .get_last_raw_message_by_subject(subject.as_str())
                    .await;
                match raw_message {
                    Ok(raw_message) => {
                        let operation = kv_operation_from_stream_message(&raw_message);
                        // TODO: unnecessary expensive, cloning whole Message.
                        let nats_message = Message::try_from(raw_message.clone())?;
                        Some((
                            nats_message,
                            operation,
                            raw_message.sequence,
                            raw_message.time,
                        ))
                    }
                    Err(err) => {
                        let e: std::io::Error = *err.downcast().unwrap();
                        let d = e.get_ref().unwrap();
                        let de = d.downcast_ref::<response::Error>().unwrap();
                        // 10037 is returned when there are no messages found.
                        if de.code == 10037 {
                            None
                        } else {
                            return Err(Box::new(e));
                        }
                    }
                }
            }
        };

        match result {
            Some((message, operation, revision, created)) => {
                if message.status == Some(StatusCode::NO_RESPONDERS) {
                    return Ok(None);
                }

                let entry = Entry {
                    bucket: self.name.clone(),
                    key,
                    value: message.payload.to_vec(),
                    revision,
                    created,
                    operation,
                    delta: 0,
                };
                Ok(Some(entry))
            }
            // TODO: remember to touch this when Errors are in place.
            None => Ok(None),
        }
    }

    /// Creates a [futures::Stream] over [Entries][Entry]  a given key in the bucket, which yields
    /// values whenever there are changes for that key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
    ///     bucket: "kv".to_string(),
    ///     history: 10,
    ///     ..Default::default()
    /// }).await?;
    /// let mut entries = kv.watch("kv").await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch<T: AsRef<str>>(&self, key: T) -> Result<Watch<'_>, Error> {
        let subject = format!("{}{}", self.prefix.as_str(), key.as_ref());

        let consumer = self
            .stream
            .create_consumer(super::consumer::push::OrderedConfig {
                deliver_subject: self.stream.context.client.new_inbox(),
                description: Some("kv watch consumer".to_string()),
                filter_subject: subject,
                replay_policy: super::consumer::ReplayPolicy::Instant,
                deliver_policy: DeliverPolicy::New,
                ..Default::default()
            })
            .await?;

        Ok(Watch {
            subscription: consumer.messages().await?,
            prefix: self.prefix.clone(),
            bucket: self.name.clone(),
        })
    }

    /// Creates a [futures::Stream] over [Entries][Entry] for all keys, which yields
    /// values whenever there are changes in the bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
    ///     bucket: "kv".to_string(),
    ///     history: 10,
    ///     ..Default::default()
    /// }).await?;
    /// let mut entries = kv.watch_all().await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_all(&self) -> Result<Watch<'_>, Error> {
        self.watch(ALL_KEYS).await
    }

    pub async fn get<T: Into<String>>(&self, key: T) -> Result<Option<Vec<u8>>, Error> {
        match self.entry(key).await {
            Ok(Some(entry)) => match entry.operation {
                Operation::Put => Ok(Some(entry.value)),
                _ => Ok(None),
            },
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Updates a value for a given key, but only if passed `revision` is the last `revision` in
    /// the bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
    ///     bucket: "kv".to_string(),
    ///     history: 10,
    ///     ..Default::default()
    /// }).await?;
    /// let revision = kv.put("key", "value".into()).await?;
    /// kv.update("key", "updated".into(), revision).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update<T: AsRef<str>>(
        &self,
        key: T,
        value: Bytes,
        revision: u64,
    ) -> Result<u64, Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }
        let subject = format!("{}{}", self.prefix.as_str(), key.as_ref());

        let mut headers = crate::HeaderMap::default();
        headers.insert(
            header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
            HeaderValue::from(revision),
        );

        self.stream
            .context
            .publish_with_headers(subject, headers, value)
            .await?
            .await
            .map(|publish_ack| publish_ack.sequence)
    }

    /// Deletes a given key. This is a non-destructive operation, which sets a `DELETE` marker.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
    ///     bucket: "kv".to_string(),
    ///     history: 10,
    ///     ..Default::default()
    /// }).await?;
    /// kv.put("key", "value".into()).await?;
    /// kv.delete("key").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete<T: AsRef<str>>(&self, key: T) -> Result<(), Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }
        let mut subject = String::new();
        if self.use_jetstream_prefix {
            subject.push_str(&self.stream.context.prefix);
            subject.push('.');
        }
        subject.push_str(self.put_prefix.as_ref().unwrap_or(&self.prefix));
        subject.push_str(key.as_ref());

        let mut headers = crate::HeaderMap::default();
        // TODO: figure out which headers k/v should be where.
        headers.insert(KV_OPERATION, KV_OPERATION_DELETE.parse::<HeaderValue>()?);

        self.stream
            .context
            .publish_with_headers(subject, headers, "".into())
            .await?;
        Ok(())
    }

    /// Purges all the revisions of a entry destructively, leaving behind a single purge entry in-place.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
    ///     bucket: "kv".to_string(),
    ///     history: 10,
    ///     ..Default::default()
    /// }).await?;
    /// kv.put("key", "value".into()).await?;
    /// kv.put("key", "another".into()).await?;
    /// kv.purge("key").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge<T: AsRef<str>>(&self, key: T) -> Result<(), Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }

        let subject = format!("{}{}", self.prefix.as_str(), key.as_ref());

        let mut headers = crate::HeaderMap::default();
        headers.insert(KV_OPERATION, HeaderValue::from(KV_OPERATION_PURGE));
        headers.insert(NATS_ROLLUP, HeaderValue::from(ROLLUP_SUBJECT));

        self.stream
            .context
            .publish_with_headers(subject, headers, "".into())
            .await?;
        Ok(())
    }

    /// Returns a [futures::Stream] that allows iterating over all [Operations][Operation] that
    /// happen for given key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
    ///     bucket: "kv".to_string(),
    ///     history: 10,
    ///     ..Default::default()
    /// }).await?;
    /// let mut entries = kv.history("kv").await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn history<T: AsRef<str>>(&self, key: T) -> Result<History<'_>, Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }
        let subject = format!("{}{}", self.prefix.as_str(), key.as_ref());

        let consumer = self
            .stream
            .create_consumer(super::consumer::push::OrderedConfig {
                deliver_subject: self.stream.context.client.new_inbox(),
                description: Some("kv history consumer".to_string()),
                filter_subject: subject,
                replay_policy: super::consumer::ReplayPolicy::Instant,
                ..Default::default()
            })
            .await?;

        Ok(History {
            subscription: consumer.messages().await?,
            done: false,
            prefix: self.prefix.clone(),
            bucket: self.name.clone(),
        })
    }

    /// Returns a [futures::Stream] that allows iterating over all keys in the bucket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
    ///     bucket: "kv".to_string(),
    ///     history: 10,
    ///     ..Default::default()
    /// }).await?;
    /// let mut entries = kv.keys().await?;
    /// while let Some(key) = entries.next() {
    ///     println!("key: {:?}", key);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn keys(&self) -> Result<collections::hash_set::IntoIter<String>, Error> {
        let subject = format!("{}>", self.prefix.as_str());

        let consumer = self
            .stream
            .create_consumer(super::consumer::push::OrderedConfig {
                deliver_subject: self.stream.context.client.new_inbox(),
                description: Some("kv history consumer".to_string()),
                filter_subject: subject,
                headers_only: true,
                replay_policy: super::consumer::ReplayPolicy::Instant,
                ..Default::default()
            })
            .await?;

        let mut entries = History {
            done: consumer.info.num_pending == 0,
            subscription: consumer.messages().await?,
            prefix: self.prefix.clone(),
            bucket: self.name.clone(),
        };

        let mut keys = HashSet::new();
        while let Some(entry) = entries.try_next().await? {
            keys.insert(entry.key);
        }
        Ok(keys.into_iter())
    }
}

pub struct Watch<'a> {
    subscription: super::consumer::push::Ordered<'a>,
    prefix: String,
    bucket: String,
}

impl<'a> futures::Stream for Watch<'a> {
    type Item = Result<Entry, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.subscription.poll_next_unpin(cx) {
            Poll::Ready(message) => match message {
                None => Poll::Ready(None),
                Some(message) => {
                    let message = message?;
                    let info = message.info()?;

                    let operation = match message
                        .headers
                        .as_ref()
                        .and_then(|headers| headers.get(KV_OPERATION))
                        .unwrap_or(&HeaderValue::from(KV_OPERATION_PUT))
                        .iter()
                        .next()
                        .unwrap()
                        .as_str()
                    {
                        KV_OPERATION_DELETE => Operation::Delete,
                        KV_OPERATION_PURGE => Operation::Purge,
                        _ => Operation::Put,
                    };

                    let key = message
                        .subject
                        .strip_prefix(&self.prefix)
                        .map(|s| s.to_string())
                        .unwrap();

                    Poll::Ready(Some(Ok(Entry {
                        bucket: self.bucket.clone(),
                        key,
                        value: message.payload.to_vec(),
                        revision: info.stream_sequence,
                        created: info.published,
                        delta: info.pending,
                        operation,
                    })))
                }
            },
            std::task::Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub struct History<'a> {
    subscription: super::consumer::push::Ordered<'a>,
    done: bool,
    prefix: String,
    bucket: String,
}

impl<'a> futures::Stream for History<'a> {
    type Item = Result<Entry, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.subscription.poll_next_unpin(cx) {
            Poll::Ready(message) => match message {
                None => Poll::Ready(None),
                Some(message) => {
                    let message = message?;
                    let info = message.info()?;
                    if info.pending == 0 {
                        self.done = true;
                    }

                    let operation = match message
                        .headers
                        .as_ref()
                        .and_then(|headers| headers.get(KV_OPERATION))
                        .unwrap_or(&HeaderValue::from(KV_OPERATION_PUT))
                        .iter()
                        .next()
                        .unwrap()
                        .as_str()
                    {
                        KV_OPERATION_DELETE => Operation::Delete,
                        KV_OPERATION_PURGE => Operation::Purge,
                        _ => Operation::Put,
                    };

                    let key = message
                        .subject
                        .strip_prefix(&self.prefix)
                        .map(|s| s.to_string())
                        .unwrap();

                    Poll::Ready(Some(Ok(Entry {
                        bucket: self.bucket.clone(),
                        key,
                        value: message.payload.to_vec(),
                        revision: info.stream_sequence,
                        created: info.published,
                        delta: info.pending,
                        operation,
                    })))
                }
            },
            std::task::Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

/// An entry in a key-value bucket.
#[derive(Debug, Clone)]
pub struct Entry {
    /// Name of the bucket the entry is in.
    pub bucket: String,
    /// The key that was retrieved.
    pub key: String,
    /// The value that was retrieved.
    // TODO: should we use Bytes?
    pub value: Vec<u8>,
    /// A unique sequence for this value.
    pub revision: u64,
    /// Distance from the latest value.
    pub delta: u64,
    /// The time the data was put in the bucket.
    pub created: OffsetDateTime,
    /// The kind of operation that caused this entry.
    pub operation: Operation,
}
