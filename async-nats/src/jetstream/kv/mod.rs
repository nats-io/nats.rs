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

//! A Key-Value store built on top of JetStream, allowing you to store and retrieve data using simple key-value pairs.

pub mod bucket;

use std::{
    fmt::{self, Display},
    str::FromStr,
    task::Poll,
    time::Duration,
};

use crate::HeaderValue;
use bytes::Bytes;
use futures::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use time::OffsetDateTime;
use tracing::debug;

use crate::error::Error;
use crate::header;

use self::bucket::Status;

use super::{
    consumer::{push::OrderedError, DeliverPolicy, StreamError, StreamErrorKind},
    context::{PublishError, PublishErrorKind},
    message::StreamMessage,
    stream::{
        self, ConsumerError, ConsumerErrorKind, DirectGetError, DirectGetErrorKind, Republish,
        Source, StorageType, Stream,
    },
};

fn kv_operation_from_stream_message(message: &StreamMessage) -> Result<Operation, EntryError> {
    if let Some(op) = message.headers.get(KV_OPERATION) {
        Operation::from_str(op.as_str())
            .map_err(|err| EntryError::with_source(EntryErrorKind::Other, err))
    } else if let Some(reason) = message.headers.get(header::NATS_MARKER_REASON) {
        match reason.as_str() {
            "MaxAge" | "Purge" => Ok(Operation::Purge),
            "Remove" => Ok(Operation::Delete),
            _ => Err(EntryError::with_source(
                EntryErrorKind::Other,
                "invalid marker reason",
            )),
        }
    } else {
        Err(EntryError::with_source(
            EntryErrorKind::Other,
            "missing operation",
        ))
    }
}
fn kv_operation_from_message(message: &crate::message::Message) -> Result<Operation, EntryError> {
    let headers = match message.headers.as_ref() {
        Some(headers) => headers,
        None => return Ok(Operation::Put),
    };
    if let Some(op) = headers.get(KV_OPERATION) {
        Operation::from_str(op.as_str())
            .map_err(|err| EntryError::with_source(EntryErrorKind::Other, err))
    } else {
        Ok(Operation::Put)
    }
}

static VALID_BUCKET_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\A[a-zA-Z0-9_-]+\z").unwrap());
static VALID_KEY_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\A[-/_=\.a-zA-Z0-9]+\z").unwrap());

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
#[derive(Debug, Clone, Default)]
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
    /// Compression
    #[cfg(feature = "server_2_10")]
    pub compression: bool,
    /// Cluster and tag placement for the bucket.
    pub placement: Option<stream::Placement>,
    /// Enables per-message TTL and delete marker TTL for a bucket.
    #[cfg(feature = "server_2_11")]
    pub limit_markers: Option<Duration>,
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

impl FromStr for Operation {
    type Err = ParseOperationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            KV_OPERATION_DELETE => Ok(Operation::Delete),
            KV_OPERATION_PURGE => Ok(Operation::Purge),
            KV_OPERATION_PUT => Ok(Operation::Put),
            _ => Err(ParseOperationError),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseOperationError;

impl fmt::Display for ParseOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid value found for operation (value can only be {KV_OPERATION_PUT}, {KV_OPERATION_PURGE} or {KV_OPERATION_DELETE}")
    }
}

impl std::error::Error for ParseOperationError {}

/// A struct used as a handle for the bucket.
#[derive(Debug, Clone)]
pub struct Store {
    /// The name of the Store.
    pub name: String,
    /// The name of the stream associated with the Store.
    pub stream_name: String,
    /// The prefix for keys in the Store.
    pub prefix: String,
    /// The optional prefix to use when putting new key-value pairs.
    pub put_prefix: Option<String>,
    /// Indicates whether to use the JetStream prefix.
    pub use_jetstream_prefix: bool,
    /// The stream associated with the Store.
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
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let status = kv.status().await?;
    /// println!("status: {:?}", status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn status(&self) -> Result<Status, StatusError> {
        // TODO: should we poll for fresh info here? probably yes.
        let info = self.stream.info.clone();

        Ok(Status {
            info,
            bucket: self.name.to_string(),
        })
    }

    /// Create will add the key/value pair if it does not exist. If it does exist, it will return an error.
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
    ///
    /// let status = kv.create("key", "value".into()).await;
    /// assert!(status.is_ok());
    ///
    /// let status = kv.create("key", "value".into()).await;
    /// assert!(status.is_err());
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create<T: AsRef<str>>(
        &self,
        key: T,
        value: bytes::Bytes,
    ) -> Result<u64, CreateError> {
        self.create_maybe_ttl(key, value, None).await
    }

    /// Create will add the key/value pair if it does not exist. If it does exist, it will return an error.
    /// It will set a TTL specific for that key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use std::time::Duration;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// let status = kv
    ///     .create_with_ttl("key", "value".into(), Duration::from_secs(10))
    ///     .await;
    /// assert!(status.is_ok());
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_with_ttl<T: AsRef<str>>(
        &self,
        key: T,
        value: bytes::Bytes,
        ttl: Duration,
    ) -> Result<u64, CreateError> {
        self.create_maybe_ttl(key, value, Some(ttl)).await
    }

    async fn create_maybe_ttl<T: AsRef<str>>(
        &self,
        key: T,
        value: bytes::Bytes,
        ttl: Option<Duration>,
    ) -> Result<u64, CreateError> {
        let update_err = match self
            .update_maybe_ttl(key.as_ref(), value.clone(), 0, ttl)
            .await
        {
            Ok(revision) => return Ok(revision),
            Err(err) => err,
        };

        match self.entry(key.as_ref()).await? {
            // Deleted or Purged key, we can create it again.
            Some(Entry {
                operation: Operation::Delete | Operation::Purge,
                revision,
                ..
            }) => {
                let revision = self.update(key, value, revision).await?;
                Ok(revision)
            }

            // key already exists.
            Some(_) => Err(CreateError::new(CreateErrorKind::AlreadyExists)),

            // Something went wrong with the initial update, return that error
            None => Err(update_err.into()),
        }
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
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let status = kv.put("key", "value".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put<T: AsRef<str>>(&self, key: T, value: bytes::Bytes) -> Result<u64, PutError> {
        if !is_valid_key(key.as_ref()) {
            return Err(PutError::new(PutErrorKind::InvalidKey));
        }
        let mut subject = String::new();
        if self.use_jetstream_prefix {
            subject.push_str(&self.stream.context.prefix);
            subject.push('.');
        }
        subject.push_str(self.put_prefix.as_ref().unwrap_or(&self.prefix));
        subject.push_str(key.as_ref());

        let publish_ack = self
            .stream
            .context
            .publish(subject, value)
            .await
            .map_err(|err| PutError::with_source(PutErrorKind::Publish, err))?;
        let ack = publish_ack
            .await
            .map_err(|err| PutError::with_source(PutErrorKind::Ack, err))?;

        Ok(ack.sequence)
    }

    async fn entry_maybe_revision<T: Into<String>>(
        &self,
        key: T,
        revision: Option<u64>,
    ) -> Result<Option<Entry>, EntryError> {
        let key: String = key.into();
        if !is_valid_key(key.as_ref()) {
            return Err(EntryError::new(EntryErrorKind::InvalidKey));
        }

        let subject = format!("{}{}", self.prefix.as_str(), &key);

        let result: Option<(StreamMessage, Operation)> = {
            if self.stream.info.config.allow_direct {
                let message = match revision {
                    Some(revision) => {
                        let message = self.stream.direct_get(revision).await;
                        if let Ok(message) = message.as_ref() {
                            if message.subject.as_str() != subject {
                                println!("subject mismatch {}", message.subject);
                                return Ok(None);
                            }
                        }
                        message
                    }
                    None => {
                        self.stream
                            .direct_get_last_for_subject(subject.as_str())
                            .await
                    }
                };

                match message {
                    Ok(message) => {
                        let operation =
                            kv_operation_from_stream_message(&message).unwrap_or(Operation::Put);

                        Some((message, operation))
                    }
                    Err(err) => {
                        if err.kind() == DirectGetErrorKind::NotFound {
                            None
                        } else {
                            return Err(err.into());
                        }
                    }
                }
            } else {
                let raw_message = match revision {
                    Some(revision) => {
                        let message = self.stream.get_raw_message(revision).await;
                        if let Ok(message) = message.as_ref() {
                            if message.subject.as_str() != subject {
                                return Ok(None);
                            }
                        }
                        message
                    }
                    None => {
                        self.stream
                            .get_last_raw_message_by_subject(subject.as_str())
                            .await
                    }
                };
                match raw_message {
                    Ok(raw_message) => {
                        let operation = kv_operation_from_stream_message(&raw_message)
                            .unwrap_or(Operation::Put);
                        // TODO: unnecessary expensive, cloning whole Message.
                        Some((raw_message, operation))
                    }
                    Err(err) => match err.kind() {
                        crate::jetstream::stream::LastRawMessageErrorKind::NoMessageFound => None,
                        crate::jetstream::stream::LastRawMessageErrorKind::InvalidSubject => {
                            return Err(EntryError::new(EntryErrorKind::InvalidKey))
                        }
                        crate::jetstream::stream::LastRawMessageErrorKind::Other => {
                            return Err(EntryError::with_source(EntryErrorKind::Other, err))
                        }
                        crate::jetstream::stream::LastRawMessageErrorKind::JetStream(err) => {
                            return Err(EntryError::with_source(EntryErrorKind::Other, err))
                        }
                    },
                }
            }
        };

        match result {
            Some((message, operation)) => {
                let entry = Entry {
                    bucket: self.name.clone(),
                    key,
                    value: message.payload,
                    revision: message.sequence,
                    created: message.time,
                    operation,
                    delta: 0,
                    seen_current: false,
                };
                Ok(Some(entry))
            }
            // TODO: remember to touch this when Errors are in place.
            None => Ok(None),
        }
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
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let status = kv.put("key", "value".into()).await?;
    /// let entry = kv.entry("key").await?;
    /// println!("entry: {:?}", entry);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn entry<T: Into<String>>(&self, key: T) -> Result<Option<Entry>, EntryError> {
        self.entry_maybe_revision(key, None).await
    }

    /// Retrieves the [Entry] for a given key revision from a bucket.
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
    /// let status = kv.put("key", "value".into()).await?;
    /// let status = kv.put("key", "value2".into()).await?;
    /// let entry = kv.entry_for_revision("key", 2).await?;
    /// println!("entry: {:?}", entry);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn entry_for_revision<T: Into<String>>(
        &self,
        key: T,
        revision: u64,
    ) -> Result<Option<Entry>, EntryError> {
        self.entry_maybe_revision(key, Some(revision)).await
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
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let mut entries = kv.watch("kv").await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch<T: AsRef<str>>(&self, key: T) -> Result<Watch, WatchError> {
        self.watch_with_deliver_policy(key, DeliverPolicy::New)
            .await
    }

    /// Creates a [futures::Stream] over [Entries][Entry] in the bucket, which yields
    /// values whenever there are changes for given keys.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let mut entries = kv.watch_many(["foo", "bar"]).await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "server_2_10")]
    pub async fn watch_many<T, K>(&self, keys: K) -> Result<Watch, WatchError>
    where
        T: AsRef<str>,
        K: IntoIterator<Item = T>,
    {
        self.watch_many_with_deliver_policy(keys, DeliverPolicy::New)
            .await
    }

    /// Creates a [futures::Stream] over [Entries][Entry] for a given key in the bucket, starting from
    /// provided revision. This is useful to resume watching over big KV buckets without a need to
    /// replay all the history.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let mut entries = kv.watch_from_revision("kv", 5).await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_from_revision<T: AsRef<str>>(
        &self,
        key: T,
        revision: u64,
    ) -> Result<Watch, WatchError> {
        self.watch_with_deliver_policy(
            key,
            DeliverPolicy::ByStartSequence {
                start_sequence: revision,
            },
        )
        .await
    }

    /// Creates a [futures::Stream] over [Entries][Entry]  a given key in the bucket, which yields
    /// values whenever there are changes for that key with as well as last value.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let mut entries = kv.watch_with_history("kv").await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_with_history<T: AsRef<str>>(&self, key: T) -> Result<Watch, WatchError> {
        self.watch_with_deliver_policy(key, DeliverPolicy::LastPerSubject)
            .await
    }

    /// Creates a [futures::Stream] over [Entries][Entry]  a given keys in the bucket, which yields
    /// values whenever there are changes for those keys with as well as last value.
    /// This requires server version > 2.10 as it uses consumers with multiple subject filters.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let mut entries = kv.watch_many_with_history(["key1", "key2"]).await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "server_2_10")]
    pub async fn watch_many_with_history<T: AsRef<str>, K: IntoIterator<Item = T>>(
        &self,
        keys: K,
    ) -> Result<Watch, WatchError> {
        self.watch_many_with_deliver_policy(keys, DeliverPolicy::LastPerSubject)
            .await
    }

    #[cfg(feature = "server_2_10")]
    async fn watch_many_with_deliver_policy<T: AsRef<str>, K: IntoIterator<Item = T>>(
        &self,
        keys: K,
        deliver_policy: DeliverPolicy,
    ) -> Result<Watch, WatchError> {
        let subjects = keys
            .into_iter()
            .map(|key| {
                let key = key.as_ref();
                format!("{}{}", self.prefix.as_str(), key)
            })
            .collect::<Vec<_>>();

        debug!("initial consumer creation");
        let consumer = self
            .stream
            .create_consumer(super::consumer::push::OrderedConfig {
                deliver_subject: self.stream.context.client.new_inbox(),
                description: Some("kv watch consumer".to_string()),
                filter_subjects: subjects,
                replay_policy: super::consumer::ReplayPolicy::Instant,
                deliver_policy,
                ..Default::default()
            })
            .await
            .map_err(|err| match err.kind() {
                crate::jetstream::stream::ConsumerErrorKind::TimedOut => {
                    WatchError::new(WatchErrorKind::TimedOut)
                }
                _ => WatchError::with_source(WatchErrorKind::Other, err),
            })?;

        let seen_current = consumer.cached_info().num_pending == 0;

        Ok(Watch {
            subscription: consumer.messages().await.map_err(|err| match err.kind() {
                crate::jetstream::consumer::StreamErrorKind::TimedOut => {
                    WatchError::new(WatchErrorKind::TimedOut)
                }
                crate::jetstream::consumer::StreamErrorKind::Other => {
                    WatchError::with_source(WatchErrorKind::Other, err)
                }
            })?,
            prefix: self.prefix.clone(),
            bucket: self.name.clone(),
            seen_current,
        })
    }

    async fn watch_with_deliver_policy<T: AsRef<str>>(
        &self,
        key: T,
        deliver_policy: DeliverPolicy,
    ) -> Result<Watch, WatchError> {
        let subject = format!("{}{}", self.prefix.as_str(), key.as_ref());

        debug!("initial consumer creation");
        let consumer = self
            .stream
            .create_consumer(super::consumer::push::OrderedConfig {
                deliver_subject: self.stream.context.client.new_inbox(),
                description: Some("kv watch consumer".to_string()),
                filter_subject: subject,
                replay_policy: super::consumer::ReplayPolicy::Instant,
                deliver_policy,
                ..Default::default()
            })
            .await
            .map_err(|err| match err.kind() {
                crate::jetstream::stream::ConsumerErrorKind::TimedOut => {
                    WatchError::new(WatchErrorKind::TimedOut)
                }
                _ => WatchError::with_source(WatchErrorKind::Other, err),
            })?;

        let seen_current = consumer.cached_info().num_pending == 0;

        Ok(Watch {
            subscription: consumer.messages().await.map_err(|err| match err.kind() {
                crate::jetstream::consumer::StreamErrorKind::TimedOut => {
                    WatchError::new(WatchErrorKind::TimedOut)
                }
                crate::jetstream::consumer::StreamErrorKind::Other => {
                    WatchError::with_source(WatchErrorKind::Other, err)
                }
            })?,
            prefix: self.prefix.clone(),
            bucket: self.name.clone(),
            seen_current,
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
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let mut entries = kv.watch_all().await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_all(&self) -> Result<Watch, WatchError> {
        self.watch(ALL_KEYS).await
    }

    /// Creates a [futures::Stream] over [Entries][Entry] for all keys starting
    /// from a provider revision. This can be useful when resuming watching over a big bucket
    /// without the need to replay all the history.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let mut entries = kv.watch_all_from_revision(40).await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_all_from_revision(&self, revision: u64) -> Result<Watch, WatchError> {
        self.watch_from_revision(ALL_KEYS, revision).await
    }

    /// Retrieves the [Entry] for a given key from a bucket.
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
    /// let value = kv.get("key").await?;
    /// match value {
    ///     Some(bytes) => {
    ///         let value_str = std::str::from_utf8(&bytes)?;
    ///         println!("Value: {}", value_str);
    ///     }
    ///     None => {
    ///         println!("Key not found or value not set");
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get<T: Into<String>>(&self, key: T) -> Result<Option<Bytes>, EntryError> {
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
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
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
    ) -> Result<u64, UpdateError> {
        self.update_maybe_ttl(key, value, revision, None).await
    }

    async fn update_maybe_ttl<T: AsRef<str>>(
        &self,
        key: T,
        value: Bytes,
        revision: u64,
        ttl: Option<Duration>,
    ) -> Result<u64, UpdateError> {
        if !is_valid_key(key.as_ref()) {
            return Err(UpdateError::new(UpdateErrorKind::InvalidKey));
        }
        let mut subject = String::new();
        if self.use_jetstream_prefix {
            subject.push_str(&self.stream.context.prefix);
            subject.push('.');
        }
        subject.push_str(self.put_prefix.as_ref().unwrap_or(&self.prefix));
        subject.push_str(key.as_ref());

        let mut headers = crate::HeaderMap::default();
        headers.insert(
            header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
            HeaderValue::from(revision),
        );

        if let Some(ttl) = ttl {
            headers.insert(header::NATS_MESSAGE_TTL, HeaderValue::from(ttl.as_secs()));
        }

        self.stream
            .context
            .publish_with_headers(subject, headers, value)
            .await?
            .await
            .map_err(|err| err.into())
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
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// kv.put("key", "value".into()).await?;
    /// kv.delete("key").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete<T: AsRef<str>>(&self, key: T) -> Result<(), DeleteError> {
        self.delete_expect_revision(key, None).await
    }

    /// Deletes a given key if the revision matches. This is a non-destructive operation, which
    /// sets a `DELETE` marker.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let revision = kv.put("key", "value".into()).await?;
    /// kv.delete_expect_revision("key", Some(revision)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_expect_revision<T: AsRef<str>>(
        &self,
        key: T,
        revison: Option<u64>,
    ) -> Result<(), DeleteError> {
        if !is_valid_key(key.as_ref()) {
            return Err(DeleteError::new(DeleteErrorKind::InvalidKey));
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
        headers.insert(
            KV_OPERATION,
            KV_OPERATION_DELETE
                .parse::<HeaderValue>()
                .map_err(|err| DeleteError::with_source(DeleteErrorKind::Other, err))?,
        );

        if let Some(revision) = revison {
            headers.insert(
                header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
                HeaderValue::from(revision),
            );
        }

        self.stream
            .context
            .publish_with_headers(subject, headers, "".into())
            .await?
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
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// kv.put("key", "value".into()).await?;
    /// kv.put("key", "another".into()).await?;
    /// kv.purge("key").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge<T: AsRef<str>>(&self, key: T) -> Result<(), PurgeError> {
        self.purge_expect_revision(key, None).await
    }

    /// Purges all the revisions of a entry destructively, leaving behind a single purge entry in-place.
    /// The purge entry will remain valid for the given `ttl`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use std::time::Duration;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// kv.put("key", "value".into()).await?;
    /// kv.put("key", "another".into()).await?;
    /// kv.purge_with_ttl("key", Duration::from_secs(10)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge_with_ttl<T: AsRef<str>>(
        &self,
        key: T,
        ttl: Duration,
    ) -> Result<(), PurgeError> {
        self.purge_expect_revision_maybe_ttl(key, None, Some(ttl))
            .await
    }

    /// Purges all the revisions of a entry destructively if the revision matches, leaving behind a single
    /// purge entry in-place.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// kv.put("key", "value".into()).await?;
    /// let revision = kv.put("key", "another".into()).await?;
    /// kv.purge_expect_revision("key", Some(revision)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge_expect_revision<T: AsRef<str>>(
        &self,
        key: T,
        revision: Option<u64>,
    ) -> Result<(), PurgeError> {
        self.purge_expect_revision_maybe_ttl(key, revision, None)
            .await
    }

    /// Purges all the revisions of a entry destructively if the revision matches, leaving behind a single
    /// purge entry in-place. The purge entry will remain valid for the given `ttl`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use std::time::Duration;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// kv.put("key", "value".into()).await?;
    /// let revision = kv.put("key", "another".into()).await?;
    /// kv.purge_expect_revision_with_ttl("key", revision, Duration::from_secs(10))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge_expect_revision_with_ttl<T: AsRef<str>>(
        &self,
        key: T,
        revision: u64,
        ttl: Duration,
    ) -> Result<(), PurgeError> {
        self.purge_expect_revision_maybe_ttl(key, Some(revision), Some(ttl))
            .await
    }

    async fn purge_expect_revision_maybe_ttl<T: AsRef<str>>(
        &self,
        key: T,
        revision: Option<u64>,
        ttl: Option<Duration>,
    ) -> Result<(), PurgeError> {
        if !is_valid_key(key.as_ref()) {
            return Err(PurgeError::new(PurgeErrorKind::InvalidKey));
        }

        let mut subject = String::new();
        if self.use_jetstream_prefix {
            subject.push_str(&self.stream.context.prefix);
            subject.push('.');
        }
        subject.push_str(self.put_prefix.as_ref().unwrap_or(&self.prefix));
        subject.push_str(key.as_ref());

        let mut headers = crate::HeaderMap::default();
        headers.insert(KV_OPERATION, HeaderValue::from(KV_OPERATION_PURGE));
        headers.insert(NATS_ROLLUP, HeaderValue::from(ROLLUP_SUBJECT));
        if let Some(ttl) = ttl {
            headers.insert(header::NATS_MESSAGE_TTL, HeaderValue::from(ttl.as_secs()));
        }

        if let Some(revision) = revision {
            headers.insert(
                header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
                HeaderValue::from(revision),
            );
        }

        self.stream
            .context
            .publish_with_headers(subject, headers, "".into())
            .await?
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
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let mut entries = kv.history("kv").await?;
    /// while let Some(entry) = entries.next().await {
    ///     println!("entry: {:?}", entry);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn history<T: AsRef<str>>(&self, key: T) -> Result<History, HistoryError> {
        if !is_valid_key(key.as_ref()) {
            return Err(HistoryError::new(HistoryErrorKind::InvalidKey));
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
    /// Iterating over each each key individually
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::{StreamExt, TryStreamExt};
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let mut keys = kv.keys().await?.boxed();
    /// while let Some(key) = keys.try_next().await? {
    ///     println!("key: {:?}", key);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Collecting it into a vector of keys
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::TryStreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let kv = jetstream
    ///     .create_key_value(async_nats::jetstream::kv::Config {
    ///         bucket: "kv".to_string(),
    ///         history: 10,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let keys = kv.keys().await?.try_collect::<Vec<String>>().await?;
    /// println!("Keys: {:?}", keys);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn keys(&self) -> Result<Keys, HistoryError> {
        let subject = format!("{}>", self.prefix.as_str());

        let consumer = self
            .stream
            .create_consumer(super::consumer::push::OrderedConfig {
                deliver_subject: self.stream.context.client.new_inbox(),
                description: Some("kv history consumer".to_string()),
                filter_subject: subject,
                headers_only: true,
                replay_policy: super::consumer::ReplayPolicy::Instant,
                // We only need to know the latest state for each key, not the whole history
                deliver_policy: DeliverPolicy::LastPerSubject,
                ..Default::default()
            })
            .await?;

        let entries = History {
            done: consumer.info.num_pending == 0,
            subscription: consumer.messages().await?,
            prefix: self.prefix.clone(),
            bucket: self.name.clone(),
        };

        Ok(Keys { inner: entries })
    }
}

/// A structure representing a watch on a key-value bucket, yielding values whenever there are changes.
pub struct Watch {
    seen_current: bool,
    subscription: super::consumer::push::Ordered,
    prefix: String,
    bucket: String,
}

impl futures::Stream for Watch {
    type Item = Result<Entry, WatcherError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.subscription.poll_next_unpin(cx) {
            Poll::Ready(message) => match message {
                None => Poll::Ready(None),
                Some(message) => {
                    let message = message?;
                    let info = message.info().map_err(|err| {
                        WatcherError::with_source(
                            WatcherErrorKind::Other,
                            format!("failed to parse message metadata: {err}"),
                        )
                    })?;

                    let operation =
                        kv_operation_from_message(&message.message).unwrap_or(Operation::Put);

                    let key = message
                        .subject
                        .strip_prefix(&self.prefix)
                        .map(|s| s.to_string())
                        .unwrap();

                    if !self.seen_current && info.pending == 0 {
                        self.seen_current = true;
                    }

                    Poll::Ready(Some(Ok(Entry {
                        bucket: self.bucket.clone(),
                        key,
                        value: message.payload.clone(),
                        revision: info.stream_sequence,
                        created: info.published,
                        delta: info.pending,
                        operation,
                        seen_current: self.seen_current,
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

/// A structure representing the history of a key-value bucket, yielding past values.
pub struct History {
    subscription: super::consumer::push::Ordered,
    done: bool,
    prefix: String,
    bucket: String,
}

impl futures::Stream for History {
    type Item = Result<Entry, WatcherError>;

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
                    let info = message.info().map_err(|err| {
                        WatcherError::with_source(
                            WatcherErrorKind::Other,
                            format!("failed to parse message metadata: {err}"),
                        )
                    })?;
                    if info.pending == 0 {
                        self.done = true;
                    }

                    let operation = kv_operation_from_message(&message).unwrap_or(Operation::Put);

                    let key = message
                        .subject
                        .strip_prefix(&self.prefix)
                        .map(|s| s.to_string())
                        .unwrap();

                    Poll::Ready(Some(Ok(Entry {
                        bucket: self.bucket.clone(),
                        key,
                        value: message.payload.clone(),
                        revision: info.stream_sequence,
                        created: info.published,
                        delta: info.pending,
                        operation,
                        seen_current: self.done,
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

pub struct Keys {
    inner: History,
}

impl futures::Stream for Keys {
    type Item = Result<String, WatcherError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(res)) => match res {
                    Ok(entry) => {
                        // Skip purged and deleted keys
                        if matches!(entry.operation, Operation::Purge | Operation::Delete) {
                            // Try to poll again if we skip this one
                            continue;
                        } else {
                            return Poll::Ready(Some(Ok(entry.key)));
                        }
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// An entry in a key-value bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Name of the bucket the entry is in.
    pub bucket: String,
    /// The key that was retrieved.
    pub key: String,
    /// The value that was retrieved.
    pub value: Bytes,
    /// A unique sequence for this value.
    pub revision: u64,
    /// Distance from the latest value.
    pub delta: u64,
    /// The time the data was put in the bucket.
    pub created: OffsetDateTime,
    /// The kind of operation that caused this entry.
    pub operation: Operation,
    /// Set to true after all historical messages have been received, and
    /// now all Entries are the new ones.
    pub seen_current: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StatusErrorKind {
    JetStream(crate::jetstream::Error),
    TimedOut,
}

impl Display for StatusErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::JetStream(err) => write!(f, "jetstream request failed: {err}"),
            Self::TimedOut => write!(f, "timed out"),
        }
    }
}

pub type StatusError = Error<StatusErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CreateErrorKind {
    AlreadyExists,
    InvalidKey,
    Publish,
    Ack,
    Other,
}

impl From<UpdateError> for CreateError {
    fn from(error: UpdateError) -> Self {
        match error.kind() {
            UpdateErrorKind::InvalidKey => Error::from(CreateErrorKind::InvalidKey),
            UpdateErrorKind::TimedOut => Error::from(CreateErrorKind::Publish),
            UpdateErrorKind::WrongLastRevision => Error::from(CreateErrorKind::AlreadyExists),
            UpdateErrorKind::Other => Error::from(CreateErrorKind::Other),
        }
    }
}

impl From<PutError> for CreateError {
    fn from(error: PutError) -> Self {
        match error.kind() {
            PutErrorKind::InvalidKey => Error::from(CreateErrorKind::InvalidKey),
            PutErrorKind::Publish => Error::from(CreateErrorKind::Publish),
            PutErrorKind::Ack => Error::from(CreateErrorKind::Ack),
        }
    }
}

impl From<EntryError> for CreateError {
    fn from(error: EntryError) -> Self {
        match error.kind() {
            EntryErrorKind::InvalidKey => Error::from(CreateErrorKind::InvalidKey),
            EntryErrorKind::TimedOut => Error::from(CreateErrorKind::Publish),
            EntryErrorKind::Other => Error::from(CreateErrorKind::Other),
        }
    }
}

impl Display for CreateErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists => write!(f, "key already exists"),
            Self::Publish => write!(f, "failed to create key in store"),
            Self::Ack => write!(f, "ack error"),
            Self::InvalidKey => write!(f, "key cannot be empty or start/end with `.`"),
            Self::Other => write!(f, "other error"),
        }
    }
}

pub type CreateError = Error<CreateErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PutErrorKind {
    InvalidKey,
    Publish,
    Ack,
}

impl Display for PutErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Publish => write!(f, "failed to put key into store"),
            Self::Ack => write!(f, "ack error"),
            Self::InvalidKey => write!(f, "key cannot be empty or start/end with `.`"),
        }
    }
}

pub type PutError = Error<PutErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EntryErrorKind {
    InvalidKey,
    TimedOut,
    Other,
}

impl Display for EntryErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKey => write!(f, "key cannot be empty or start/end with `.`"),
            Self::TimedOut => write!(f, "timed out"),
            Self::Other => write!(f, "failed getting entry"),
        }
    }
}

pub type EntryError = Error<EntryErrorKind>;

crate::from_with_timeout!(
    EntryError,
    EntryErrorKind,
    DirectGetError,
    DirectGetErrorKind
);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WatchErrorKind {
    InvalidKey,
    TimedOut,
    ConsumerCreate,
    Other,
}

impl Display for WatchErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConsumerCreate => write!(f, "watch consumer creation failed"),
            Self::Other => write!(f, "watch failed"),
            Self::TimedOut => write!(f, "timed out"),
            Self::InvalidKey => write!(f, "key cannot be empty or start/end with `.`"),
        }
    }
}

pub type WatchError = Error<WatchErrorKind>;

crate::from_with_timeout!(WatchError, WatchErrorKind, ConsumerError, ConsumerErrorKind);
crate::from_with_timeout!(WatchError, WatchErrorKind, StreamError, StreamErrorKind);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum UpdateErrorKind {
    InvalidKey,
    TimedOut,
    WrongLastRevision,
    Other,
}

impl Display for UpdateErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKey => write!(f, "key cannot be empty or start/end with `.`"),
            Self::TimedOut => write!(f, "timed out"),
            Self::WrongLastRevision => write!(f, "wrong last revision"),
            Self::Other => write!(f, "failed getting entry"),
        }
    }
}

pub type UpdateError = Error<UpdateErrorKind>;

impl From<PublishError> for UpdateError {
    fn from(err: PublishError) -> Self {
        match err.kind() {
            PublishErrorKind::TimedOut => Self::new(UpdateErrorKind::TimedOut),
            PublishErrorKind::WrongLastSequence => {
                Self::with_source(UpdateErrorKind::WrongLastRevision, err)
            }
            _ => Self::with_source(UpdateErrorKind::Other, err),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WatcherErrorKind {
    Consumer,
    Other,
}

impl Display for WatcherErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Consumer => write!(f, "watcher consumer error"),
            Self::Other => write!(f, "watcher error"),
        }
    }
}

pub type WatcherError = Error<WatcherErrorKind>;

impl From<OrderedError> for WatcherError {
    fn from(err: OrderedError) -> Self {
        WatcherError::with_source(WatcherErrorKind::Consumer, err)
    }
}

pub type DeleteError = UpdateError;
pub type DeleteErrorKind = UpdateErrorKind;

pub type PurgeError = UpdateError;
pub type PurgeErrorKind = UpdateErrorKind;

pub type HistoryError = WatchError;
pub type HistoryErrorKind = WatchErrorKind;
