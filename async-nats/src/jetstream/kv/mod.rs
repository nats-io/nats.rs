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

use std::{io, task::Poll};

use crate::HeaderValue;
use bytes::Bytes;
use futures::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;
use time::OffsetDateTime;

use crate::{header, jetstream::response, Error, Message};

use self::bucket::Status;

use super::stream::{RawMessage, StorageType, Stream};

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
// TODO: will be used in this PR
#[allow(dead_code)]
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
    // TODO: add placement
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

#[derive(Debug)]
pub struct Store {
    pub name: String,
    pub stream_name: String,
    pub prefix: String,
    pub stream: Stream,
}

impl Store {
    pub async fn status(&self) -> Result<Status, Error> {
        // TODO: should we poll for fresh info here? probably yes.
        let info = self.stream.info.clone();

        Ok(Status {
            info,
            bucket: self.name.to_string(),
        })
    }

    pub async fn put<T: AsRef<str>>(&self, key: T, value: bytes::Bytes) -> Result<u64, Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }

        // TODO: figure out what to do with the domain prefix (it's a litle weird)
        let mut subject = String::new();
        subject.push_str(&self.prefix);
        subject.push_str(key.as_ref());

        let publish_ack = self.stream.context.publish(subject, value).await?;

        Ok(publish_ack.sequence)
    }

    pub async fn entry<T: AsRef<str>>(&self, key: T) -> Result<Option<Entry>, Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }

        let mut subject = String::new();
        subject.push_str(&self.prefix);
        subject.push_str(key.as_ref());

        match self.stream.get_last_message(subject).await {
            Ok(message) => {
                let operation = kv_operation_from_stream_message(&message);
                // TODO: unnecessary expensive, cloning whole Message.
                let nats_message = Message::try_from(message.clone())?;
                let entry = Entry {
                    bucket: self.name.clone(),
                    key: key.as_ref().to_string(),
                    value: nats_message.payload.to_vec(),
                    revision: message.sequence,
                    created: message.time,
                    operation,
                    delta: 0,
                };
                Ok(Some(entry))
            }
            // TODO: remember to touch this when Errors are in place.
            Err(err) => {
                let e: std::io::Error = *err.downcast().unwrap();
                let d = e.get_ref().unwrap();
                let de = d.downcast_ref::<response::Error>().unwrap();
                if de.code == 10037 {
                    return Ok(None);
                }

                Err(Box::new(e))
            }
        }
    }

    pub async fn get<T: AsRef<str>>(&self, key: T) -> Result<Option<Vec<u8>>, Error> {
        match self.entry(key).await {
            Ok(Some(entry)) => match entry.operation {
                Operation::Put => Ok(Some(entry.value)),
                _ => Ok(None),
            },
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

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
        let mut subject = String::new();
        subject.push_str(&self.prefix);
        subject.push_str(key.as_ref());

        let mut headers = crate::HeaderMap::default();
        headers.insert(
            header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
            HeaderValue::from(revision),
        );

        self.stream
            .context
            .publish_with_headers(subject, headers, value)
            .await
            .map(|publish_ack| publish_ack.sequence)
    }

    pub async fn delete<T: AsRef<str>>(&self, key: T) -> Result<(), Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }
        let mut subject = String::new();
        subject.push_str(&self.prefix);
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

    pub async fn purge<T: AsRef<str>>(&self, key: T) -> Result<(), Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }

        let mut subject = String::new();
        subject.push_str(&self.prefix);
        subject.push_str(key.as_ref());

        let mut headers = crate::HeaderMap::default();
        headers.insert(KV_OPERATION, HeaderValue::from(KV_OPERATION_PURGE));
        headers.insert(NATS_ROLLUP, HeaderValue::from(ROLLUP_SUBJECT));

        self.stream
            .context
            .publish_with_headers(subject, headers, "".into())
            .await?;
        Ok(())
    }

    pub async fn history<T: AsRef<str>>(&self, key: T) -> Result<History, Error> {
        if !is_valid_key(key.as_ref()) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid key",
            )));
        }
        let mut subject = String::new();
        subject.push_str(&self.prefix);
        subject.push_str(key.as_ref());

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
    /// The value that was retreived.
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
