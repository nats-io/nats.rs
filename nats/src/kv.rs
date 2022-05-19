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

//! Support for Key Value Store.
//! This feature is experimental and the API may change.

use std::io;
use std::time::Duration;

use crate::header::{self, HeaderMap};
use crate::jetstream::{
    DateTime, DiscardPolicy, Error, ErrorCode, JetStream, PushSubscription, StorageType,
    StreamConfig, StreamInfo, StreamMessage, SubscribeOptions,
};
use crate::message::Message;
use lazy_static::lazy_static;
use regex::Regex;

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
    pub max_age: Duration,
    /// How large the bucket may become in total bytes before the configured discard policy kicks in
    pub max_bytes: i64,
    /// The type of storage backend, `File` (default) and `Memory`
    pub storage: StorageType,
    /// How many replicas to keep for each entry in a cluster.
    pub num_replicas: usize,
}

const MAX_HISTORY: i64 = 64;
const ALL_KEYS: &str = ">";

const KV_OPERATION: &str = "KV-Operation";
const KV_OPERATION_DELETE: &str = "DEL";
const KV_OPERATION_PURGE: &str = "PURGE";

const NATS_ROLLUP: &str = "Nats-Rollup";
const ROLLUP_SUBJECT: &str = "sub";

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

// Helper to extract key value operation from message headers
fn kv_operation_from_maybe_headers(maybe_headers: Option<&HeaderMap>) -> Operation {
    if let Some(headers) = maybe_headers {
        if let Some(op) = headers.get(KV_OPERATION) {
            return match op.as_str() {
                KV_OPERATION_DELETE => Operation::Delete,
                KV_OPERATION_PURGE => Operation::Purge,
                _ => Operation::Put,
            };
        }
    }

    Operation::Put
}

fn kv_operation_from_stream_message(message: &StreamMessage) -> Operation {
    kv_operation_from_maybe_headers(message.headers.as_ref())
}

lazy_static! {
    static ref VALID_BUCKET_RE: Regex = Regex::new(r#"\A[a-zA-Z0-9_-]+\z"#).unwrap();
    static ref VALID_KEY_RE: Regex = Regex::new(r#"\A[-/_=\.a-zA-Z0-9]+\z"#).unwrap();
}

fn is_valid_bucket_name(bucket_name: &str) -> bool {
    VALID_BUCKET_RE.is_match(bucket_name)
}

fn is_valid_key(key: &str) -> bool {
    if key.is_empty() || key.starts_with('.') || key.ends_with('.') {
        return false;
    }

    VALID_KEY_RE.is_match(key)
}

impl JetStream {
    /// Bind to an existing key-value store bucket.
    ///
    /// # Example
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// context.create_key_value(&Config {
    ///   bucket: "key_value".to_string(),
    ///   ..Default::default()
    /// })?;
    ///
    /// let key_value = context.key_value("key_value")?;
    ///
    /// # context.delete_key_value("key_value")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn key_value(&self, bucket: &str) -> io::Result<Store> {
        if !self.connection.is_server_compatible_version(2, 6, 2) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "key-value requires at least server version 2.6.2",
            ));
        }

        if !is_valid_bucket_name(bucket) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid bucket name",
            ));
        }

        let stream_name = format!("KV_{}", bucket);
        let stream_info = self.stream_info(&stream_name)?;

        // Do some quick sanity checks that this is a correctly formed stream for KV.
        // Max msgs per subject should be > 0.
        if stream_info.config.max_msgs_per_subject < 1 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "bucket not valid key-value store",
            ));
        }

        Ok(Store {
            name: bucket.to_string(),
            stream_name,
            prefix: format!("$KV.{}.", bucket),
            context: self.clone(),
            domain_prefix: self
                .options
                .has_domain
                .then(|| self.options.api_prefix.clone()),
        })
    }

    /// Create a new key-value store bucket.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// let bucket = context.create_key_value(&Config {
    ///   bucket: "create_key_value".to_string(),
    ///   ..Default::default()
    /// })?;
    ///
    /// # context.delete_key_value("create_key_value")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_key_value(&self, config: &Config) -> io::Result<Store> {
        if !self.connection.is_server_compatible_version(2, 6, 2) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "key-value requires at least server version 2.6.2",
            ));
        }

        let discard_policy = {
            if self.connection.is_server_compatible_version(2, 7, 2) {
                DiscardPolicy::New
            } else {
                DiscardPolicy::Old
            }
        };

        if !is_valid_bucket_name(&config.bucket) {
            return Err(io::Error::new(io::ErrorKind::Other, "invalid bucket name"));
        }

        self.account_info()?;

        // Default to 1 for history. Max is 64 for now.
        let history = if config.history > 0 {
            if config.history > MAX_HISTORY {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "history limited to a max of 64",
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

        let stream_info = self.add_stream(&StreamConfig {
            name: format!("KV_{}", config.bucket),
            description: Some(config.description.to_string()),
            subjects: vec![format!("$KV.{}.>", config.bucket)],
            max_msgs_per_subject: history,
            max_bytes: config.max_bytes,
            max_age: config.max_age,
            max_msg_size: config.max_value_size,
            storage: config.storage,
            allow_rollup: true,
            deny_delete: true,
            num_replicas,
            discard: discard_policy,
            ..Default::default()
        })?;

        Ok(Store {
            name: config.bucket.to_string(),
            stream_name: stream_info.config.name,
            prefix: format!("$KV.{}.", config.bucket),
            context: self.clone(),
            domain_prefix: self
                .options
                .has_domain
                .then(|| self.options.api_prefix.clone()),
        })
    }

    /// Delete the specified key value store bucket.
    ///
    /// # Example
    ///
    /// ```
    /// use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # let bucket = context.create_key_value(&Config {
    /// #  bucket: "delete_key_value".to_string(),
    /// #  ..Default::default()
    /// # })?;
    ///
    /// context.delete_key_value("delete_key_value")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn delete_key_value(&self, bucket: &str) -> io::Result<()> {
        if !self.connection.is_server_compatible_version(2, 6, 2) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "key-value requires at least server version 2.6.2",
            ));
        }

        if !is_valid_bucket_name(bucket) {
            return Err(io::Error::new(io::ErrorKind::Other, "invalid bucket name"));
        }

        let stream_name = format!("KV_{}", bucket);
        self.delete_stream(&stream_name)?;

        Ok(())
    }
}

/// An entry in a key-value bucket.
#[derive(Debug, Clone)]
pub struct Entry {
    /// Name of the bucket the entry is in.
    pub bucket: String,
    /// The key that was retrieved.
    pub key: String,
    /// The value that was retreived.
    pub value: Vec<u8>,
    /// A unique sequence for this value.
    pub revision: u64,
    /// Distance from the latest value.
    pub delta: u64,
    /// The time the data was put in the bucket.
    pub created: DateTime,
    /// The kind of operation that caused this entry.
    pub operation: Operation,
}

/// A key value store
#[derive(Debug, Clone)]
pub struct Store {
    name: String,
    stream_name: String,
    prefix: String,
    context: JetStream,
    domain_prefix: Option<String>,
}

impl Store {
    /// Returns the status of the bucket
    pub fn status(&self) -> io::Result<BucketStatus> {
        let info = self.context.stream_info(&self.stream_name)?;

        Ok(BucketStatus {
            bucket: self.name.to_string(),
            info,
        })
    }

    /// Returns the latest entry for the key, if any.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # let bucket = context.create_key_value(&Config {
    /// #  bucket: "entry_bucket".to_string(),
    /// #  ..Default::default()
    /// # })?;
    /// #
    /// bucket.put("foo", b"bar")?;
    /// let maybe_entry = bucket.entry("foo")?;
    /// if let Some(entry) = maybe_entry {
    ///   println!("Found entry {:?}", entry);
    /// }
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn entry(&self, key: &str) -> io::Result<Option<Entry>> {
        if !is_valid_key(key) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid key"));
        }

        let mut subject = String::new();
        subject.push_str(&self.prefix);
        subject.push_str(key);

        match self.context.get_last_message(&self.stream_name, &subject) {
            Ok(message) => {
                let operation = kv_operation_from_stream_message(&message);
                let entry = Entry {
                    bucket: self.name.clone(),
                    key: key.to_string(),
                    value: message.data,
                    revision: message.sequence,
                    created: message.time,
                    operation,
                    delta: 0,
                };

                Ok(Some(entry))
            }
            Err(err) => {
                if let Some(inner_err) = err.get_ref() {
                    if let Some(error) = inner_err.downcast_ref::<Error>() {
                        if error.error_code() == ErrorCode::NoMessageFound {
                            return Ok(None);
                        }
                    }
                }

                Err(err)
            }
        }
    }

    /// Returns the latest value for the key, if any.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # let bucket = context.create_key_value(&Config {
    /// #  bucket: "get".to_string(),
    /// #  ..Default::default()
    /// # })?;
    /// #
    /// bucket.put("foo", b"bar")?;
    /// let maybe_value = bucket.get("foo")?;
    /// if let Some(value) = maybe_value {
    ///   println!("Found value {:?}", value);
    /// }
    /// #
    /// # context.delete_key_value("get")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        match self.entry(key) {
            Ok(Some(entry)) => match entry.operation {
                Operation::Put => Ok(Some(entry.value)),
                _ => Ok(None),
            },
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Places the new value for the key into the bucket.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # let bucket = context.create_key_value(&Config {
    /// #  bucket: "put".to_string(),
    /// #  ..Default::default()
    /// # })?;
    /// #
    /// bucket.put("foo", b"bar")?;
    /// # context.delete_key_value("put")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn put(&self, key: &str, value: impl AsRef<[u8]>) -> io::Result<u64> {
        if !is_valid_key(key) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid key"));
        }

        let mut subject = String::new();
        if let Some(api_prefix) = self.domain_prefix.as_ref() {
            subject.push_str(api_prefix);
        }
        subject.push_str(&self.prefix);
        subject.push_str(key);

        let publish_ack = self.context.publish(&subject, value)?;

        Ok(publish_ack.sequence)
    }

    /// Creates the key/value pair if it does not exist or is marked for deletion.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # let bucket = context.create_key_value(&Config {
    /// #  bucket: "create".to_string(),
    /// #  ..Default::default()
    /// # })?;
    /// #
    /// bucket.purge("foo")?;
    /// bucket.create("foo", b"bar")?;
    /// #
    /// # context.delete_key_value("create")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create(&self, key: &str, value: impl AsRef<[u8]>) -> io::Result<u64> {
        let result = self.update(key, &value, 0);
        if result.is_ok() {
            return result;
        }

        // Check if the last entry is a delete marker
        if let Ok(Some(entry)) = self.entry(key) {
            if entry.operation != Operation::Put {
                return self.update(key, &value, entry.revision);
            }
        }

        result
    }

    /// Updates the value if the latest revision matches.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # let bucket = context.create_key_value(&Config {
    /// #  bucket: "update".to_string(),
    /// #  ..Default::default()
    /// # })?;
    /// #
    /// let revision = bucket.put("foo", b"bar")?;
    /// let new_revision = bucket.update("foo", b"baz", revision)?;
    /// # context.delete_key_value("update")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn update(&self, key: &str, value: impl AsRef<[u8]>, revision: u64) -> io::Result<u64> {
        if !is_valid_key(key) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid key"));
        }

        let mut subject = String::new();
        if let Some(api_prefix) = self.domain_prefix.as_ref() {
            subject.push_str(api_prefix);
        }
        subject.push_str(&self.prefix);
        subject.push_str(key);

        let mut headers = HeaderMap::default();
        headers.insert(
            header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
            revision.to_string(),
        );

        let message = Message::new(&subject, None, value, Some(headers));
        let publish_ack = self.context.publish_message(&message)?;

        Ok(publish_ack.sequence)
    }

    /// Marks an entry as deleted by placing a delete marker but leaves the revision history intact.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # let bucket = context.create_key_value(&Config {
    /// #  bucket: "delete".to_string(),
    /// #  ..Default::default()
    /// # })?;
    /// #
    /// bucket.create("foo", b"bar")?;
    /// bucket.delete("foo")?;
    /// #
    /// # context.delete_key_value("delete");
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete(&self, key: &str) -> io::Result<()> {
        if !is_valid_key(key) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid key"));
        }

        let mut subject = String::new();
        if let Some(api_prefix) = self.domain_prefix.as_ref() {
            subject.push_str(api_prefix);
        }
        subject.push_str(&self.prefix);
        subject.push_str(key);

        let mut headers = HeaderMap::default();
        headers.insert(KV_OPERATION, KV_OPERATION_DELETE.to_string());

        let message = Message::new(&subject, None, b"", Some(headers));
        self.context.publish_message(&message)?;

        Ok(())
    }

    /// Remove any entries associated with the key and all historical revisions.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # let bucket = context.create_key_value(&Config {
    /// #  bucket: "purge".to_string(),
    /// #  ..Default::default()
    /// # })?;
    /// #
    /// bucket.create("foo", b"bar")?;
    /// bucket.purge("foo")?;
    /// # context.delete_key_value("purge")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn purge(&self, key: &str) -> io::Result<()> {
        if !is_valid_key(key) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid key"));
        }

        let mut subject = String::new();
        subject.push_str(&self.prefix);
        subject.push_str(key);

        let mut headers = HeaderMap::default();
        headers.insert(KV_OPERATION, KV_OPERATION_PURGE.to_string());
        headers.insert(NATS_ROLLUP, ROLLUP_SUBJECT.to_string());

        let message = Message::new(&subject, None, b"", Some(headers));
        self.context.publish_message(&message)?;

        Ok(())
    }

    /// Returns an iterator which iterate over all the current keys.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # let bucket = context.create_key_value(&Config {
    /// #  bucket: "keys".to_string(),
    /// #  ..Default::default()
    /// # })?;
    /// #
    /// bucket.put("foo", b"fizz")?;
    /// bucket.put("bar", b"buzz")?;
    ///
    /// let mut keys = bucket.keys()?;
    ///
    /// assert_eq!(keys.next(), Some("foo".to_string()));
    /// assert_eq!(keys.next(), Some("bar".to_string()));
    /// #
    /// # context.delete_key_value("keys")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn keys(&self) -> io::Result<Keys> {
        let mut subject = String::new();
        subject.push_str(&self.prefix);
        subject.push_str(ALL_KEYS);

        let subscription = self.context.subscribe_with_options(
            &subject,
            &SubscribeOptions::ordered()
                .headers_only()
                .deliver_last_per_subject(),
        )?;

        Ok(Keys {
            prefix: self.prefix.clone(),
            subscription,
            done: false,
        })
    }

    /// Returns an iterator which iterates over each entry in historical order.
    ///
    /// # Examples
    ///
    /// ```
    /// # use nats::kv::Config;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// let bucket = context.create_key_value(&Config {
    ///   bucket: "history_iter".to_string(),
    ///   history: 2,
    ///   ..Default::default()
    /// })?;
    ///
    /// bucket.put("foo", b"fizz")?;
    /// bucket.put("foo", b"buzz")?;
    ///
    /// let mut history = bucket.history("foo")?;
    ///
    /// let next = history.next().unwrap();
    /// assert_eq!(next.key, "foo".to_string());
    /// assert_eq!(next.value, b"fizz");
    ///
    /// let next = history.next().unwrap();
    /// assert_eq!(next.key, "foo".to_string());
    /// assert_eq!(next.value, b"buzz");
    ///
    /// # context.delete_key_value("history_iter")?;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn history(&self, key: &str) -> io::Result<History> {
        let mut subject = String::new();
        subject.push_str(&self.prefix);
        subject.push_str(key);

        let subscription = self.context.subscribe_with_options(
            &subject,
            &SubscribeOptions::ordered()
                .deliver_all()
                .enable_flow_control()
                .idle_heartbeat(Duration::from_millis(5000)),
        )?;

        Ok(History {
            bucket: self.name.clone(),
            prefix: self.prefix.clone(),
            subscription,
            done: false,
        })
    }

    /// Returns an iterator which iterates over each entry as they happen.
    pub fn watch_all(&self) -> io::Result<Watch> {
        self.watch(">")
    }

    /// Returns an iterator which iterates over each entry for specific key pattern as they happen.
    pub fn watch<T: AsRef<str>>(&self, key: T) -> io::Result<Watch> {
        let subject = format!("{}{}", self.prefix, key.as_ref());
        let subscription = self.context.subscribe_with_options(
            subject.as_str(),
            &SubscribeOptions::ordered()
                .deliver_last_per_subject()
                .enable_flow_control()
                .idle_heartbeat(Duration::from_millis(5000)),
        )?;

        Ok(Watch {
            bucket: self.name.clone(),
            prefix: self.prefix.clone(),
            subscription,
        })
    }

    /// Returns the name of the bucket
    pub fn bucket(&self) -> &String {
        &self.name
    }
}

/// An iterator used to iterate through the keys of a bucket.
pub struct Keys {
    prefix: String,
    subscription: PushSubscription,
    done: bool,
}

impl Iterator for Keys {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            return match self.subscription.next() {
                Some(message) => {
                    // If there are no more pending messages we'll stop after delivering the key
                    // derived from this message.
                    if let Some(info) = message.jetstream_message_info() {
                        if info.pending == 0 {
                            self.done = true;
                        }
                    }

                    // We are only interested in unique current keys from subjects so we skip delete
                    // and purge markers.
                    let operation = kv_operation_from_maybe_headers(message.headers.as_ref());
                    if operation != Operation::Put {
                        continue;
                    }

                    message
                        .subject
                        .strip_prefix(&self.prefix)
                        .map(|s| s.to_string())
                }
                None => None,
            };
        }
    }
}

/// An iterator used to iterate through the history of a bucket.
pub struct History {
    bucket: String,
    prefix: String,
    subscription: PushSubscription,
    done: bool,
}

impl Iterator for History {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match self.subscription.next() {
            Some(message) => {
                if let Some(info) = message.jetstream_message_info() {
                    if info.pending == 0 {
                        self.done = true;
                    }

                    let operation = kv_operation_from_maybe_headers(message.headers.as_ref());

                    let key = message
                        .subject
                        .strip_prefix(&self.prefix)
                        .map(|s| s.to_string())
                        .unwrap();

                    Some(Entry {
                        bucket: self.bucket.clone(),
                        key,
                        value: message.data.clone(),
                        revision: info.stream_seq,
                        created: info.published,
                        delta: info.pending,
                        operation,
                    })
                } else {
                    None
                }
            }

            None => None,
        }
    }
}

/// An iterator used to watch changes in a bucket.
pub struct Watch {
    bucket: String,
    prefix: String,
    subscription: PushSubscription,
}

impl Iterator for Watch {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        match self.subscription.next() {
            Some(message) => {
                if let Some(info) = message.jetstream_message_info() {
                    let operation = kv_operation_from_maybe_headers(message.headers.as_ref());

                    let key = message
                        .subject
                        .strip_prefix(&self.prefix)
                        .map(|s| s.to_string())
                        .unwrap();

                    Some(Entry {
                        bucket: self.bucket.clone(),
                        key,
                        value: message.data.clone(),
                        revision: info.stream_seq,
                        created: info.published,
                        delta: info.pending,
                        operation,
                    })
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

/// Represents status information about a key value store bucket
pub struct BucketStatus {
    info: StreamInfo,
    bucket: String,
}

impl BucketStatus {
    /// The name of the bucket
    pub fn bucket(&self) -> &String {
        &self.bucket
    }

    /// How many messages are in the bucket, including historical values
    pub fn values(&self) -> u64 {
        self.info.state.messages
    }

    /// Configured history kept per key
    pub fn history(&self) -> i64 {
        self.info.config.max_msgs_per_subject
    }

    /// How long the bucket keeps values for
    pub fn max_age(&self) -> Duration {
        self.info.config.max_age
    }
}
