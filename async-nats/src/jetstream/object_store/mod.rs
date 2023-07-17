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

//! Object Store module
use std::collections::VecDeque;
use std::fmt::Display;
use std::{cmp, str::FromStr, task::Poll, time::Duration};

use crate::{HeaderMap, HeaderValue};
use base64::engine::general_purpose::{STANDARD, URL_SAFE};
use base64::engine::Engine;
use once_cell::sync::Lazy;
use ring::digest::SHA256;
use tokio::io::AsyncReadExt;

use futures::{Stream, StreamExt};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

use super::consumer::push::OrderedError;
use super::consumer::{StreamError, StreamErrorKind};
use super::context::{PublishError, PublishErrorKind};
use super::stream::{ConsumerError, ConsumerErrorKind, PurgeError, PurgeErrorKind};
use super::{consumer::push::Ordered, stream::StorageType};
use time::{serde::rfc3339, OffsetDateTime};

const DEFAULT_CHUNK_SIZE: usize = 128 * 1024;
const NATS_ROLLUP: &str = "Nats-Rollup";
const ROLLUP_SUBJECT: &str = "sub";

static BUCKET_NAME_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\A[a-zA-Z0-9_-]+\z"#).unwrap());
static OBJECT_NAME_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\A[-/_=\.a-zA-Z0-9]+\z"#).unwrap());

pub(crate) fn is_valid_bucket_name(bucket_name: &str) -> bool {
    BUCKET_NAME_RE.is_match(bucket_name)
}

pub(crate) fn is_valid_object_name(object_name: &str) -> bool {
    if object_name.is_empty() || object_name.starts_with('.') || object_name.ends_with('.') {
        return false;
    }

    OBJECT_NAME_RE.is_match(object_name)
}

pub(crate) fn encode_object_name(object_name: &str) -> String {
    URL_SAFE.encode(object_name)
}

/// Configuration values for object store buckets.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Name of the storage bucket.
    pub bucket: String,
    /// A short description of the purpose of this storage bucket.
    pub description: Option<String>,
    /// Maximum age of any value in the bucket, expressed in nanoseconds
    #[serde(with = "serde_nanos")]
    pub max_age: Duration,
    /// The type of storage backend, `File` (default) and `Memory`
    pub storage: StorageType,
    /// How many replicas to keep for each value in a cluster, maximum 5.
    pub num_replicas: usize,
}

/// A blob store capable of storing large objects efficiently in streams.
#[derive(Clone)]
pub struct ObjectStore {
    pub(crate) name: String,
    pub(crate) stream: crate::jetstream::stream::Stream,
}

impl ObjectStore {
    /// Gets an [Object] from the [ObjectStore].
    ///
    /// [Object] implements [tokio::io::AsyncRead] that allows
    /// to read the data from Object Store.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use tokio::io::AsyncReadExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let bucket = jetstream.get_object_store("store").await?;
    /// let mut object = bucket.get("FOO").await?;
    ///
    /// // Object implements `tokio::io::AsyncRead`.
    /// let mut bytes = vec![];
    /// object.read_to_end(&mut bytes).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get<T: AsRef<str>>(&self, object_name: T) -> Result<Object<'_>, GetError> {
        let object_info = self.info(object_name).await?;
        // if let Some(link) = object_info.link {
        //     return self.get(link.name).await;
        // }

        let chunk_subject = format!("$O.{}.C.{}", self.name, object_info.nuid);

        let subscription = self
            .stream
            .create_consumer(crate::jetstream::consumer::push::OrderedConfig {
                filter_subject: chunk_subject,
                deliver_subject: self.stream.context.client.new_inbox(),
                ..Default::default()
            })
            .await?
            .messages()
            .await?;

        Ok(Object::new(subscription, object_info))
    }

    /// Gets an [Object] from the [ObjectStore].
    ///
    /// [Object] implements [tokio::io::AsyncRead] that allows
    /// to read the data from Object Store.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let bucket = jetstream.get_object_store("store").await?;
    /// bucket.delete("FOO").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete<T: AsRef<str>>(&self, object_name: T) -> Result<(), DeleteError> {
        let object_name = object_name.as_ref();
        let mut object_info = self.info(object_name).await?;
        object_info.chunks = 0;
        object_info.size = 0;
        object_info.deleted = true;

        let data = serde_json::to_vec(&object_info).map_err(|err| {
            DeleteError::with_source(
                DeleteErrorKind::Other,
                format!("failed deserializing object info: {}", err),
            )
        })?;

        let mut headers = HeaderMap::default();
        headers.insert(
            NATS_ROLLUP,
            HeaderValue::from_str(ROLLUP_SUBJECT).map_err(|err| {
                DeleteError::with_source(
                    DeleteErrorKind::Other,
                    format!("failed parsing header: {}", err),
                )
            })?,
        );

        let subject = format!("$O.{}.M.{}", &self.name, encode_object_name(object_name));

        self.stream
            .context
            .publish_with_headers(subject, headers, data.into())
            .await?
            .await?;

        let chunk_subject = format!("$O.{}.C.{}", self.name, object_info.nuid);

        self.stream.purge().filter(&chunk_subject).await?;

        Ok(())
    }

    /// Retrieves [Object] [ObjectInfo].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let bucket = jetstream.get_object_store("store").await?;
    /// let info = bucket.info("FOO").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn info<T: AsRef<str>>(&self, object_name: T) -> Result<ObjectInfo, InfoError> {
        let object_name = object_name.as_ref();
        let object_name = encode_object_name(object_name);
        if !is_valid_object_name(&object_name) {
            return Err(InfoError::new(InfoErrorKind::InvalidName));
        }

        // Grab last meta value we have.
        let subject = format!("$O.{}.M.{}", &self.name, &object_name);

        let message = self
            .stream
            .get_last_raw_message_by_subject(subject.as_str())
            .await
            .map_err(|err| match err.kind() {
                super::stream::LastRawMessageErrorKind::NoMessageFound => {
                    InfoError::new(InfoErrorKind::NotFound)
                }
                _ => InfoError::with_source(InfoErrorKind::Other, err),
            })?;
        let decoded_payload = STANDARD
            .decode(message.payload)
            .map_err(|err| InfoError::with_source(InfoErrorKind::Other, err))?;
        let object_info =
            serde_json::from_slice::<ObjectInfo>(&decoded_payload).map_err(|err| {
                InfoError::with_source(
                    InfoErrorKind::Other,
                    format!("failed to decode info payload: {}", err),
                )
            })?;

        Ok(object_info)
    }

    /// Puts an [Object] into the [ObjectStore].
    /// This method implements `tokio::io::AsyncRead`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let bucket = jetstream.get_object_store("store").await?;
    /// let mut file = tokio::fs::File::open("foo.txt").await?;
    /// bucket.put("file", &mut file).await.unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put<T>(
        &self,
        meta: T,
        data: &mut (impl tokio::io::AsyncRead + std::marker::Unpin),
    ) -> Result<ObjectInfo, PutError>
    where
        ObjectMeta: From<T>,
    {
        let object_meta: ObjectMeta = meta.into();

        let encoded_object_name = encode_object_name(&object_meta.name);
        if !is_valid_object_name(&encoded_object_name) {
            return Err(PutError::new(PutErrorKind::InvalidName));
        }
        // Fetch any existing object info, if there is any for later use.
        let maybe_existing_object_info = match self.info(&encoded_object_name).await {
            Ok(object_info) => Some(object_info),
            Err(_) => None,
        };

        let object_nuid = nuid::next();
        let chunk_subject = format!("$O.{}.C.{}", &self.name, &object_nuid);

        let mut object_chunks = 0;
        let mut object_size = 0;

        let mut buffer = Box::new([0; DEFAULT_CHUNK_SIZE]);
        let mut context = ring::digest::Context::new(&SHA256);

        loop {
            let n = data
                .read(&mut *buffer)
                .await
                .map_err(|err| PutError::with_source(PutErrorKind::ReadChunks, err))?;

            if n == 0 {
                break;
            }
            context.update(&buffer[..n]);

            object_size += n;
            object_chunks += 1;

            // FIXME: this is ugly
            let payload = bytes::Bytes::from(buffer[..n].to_vec());

            self.stream
                .context
                .publish(chunk_subject.clone(), payload)
                .await
                .map_err(|err| {
                    PutError::with_source(
                        PutErrorKind::PublishChunks,
                        format!("failed chunk publish: {}", err),
                    )
                })?
                .await
                .map_err(|err| {
                    PutError::with_source(
                        PutErrorKind::PublishChunks,
                        format!("failed getting chunk ack: {}", err),
                    )
                })?;
        }
        let digest = context.finish();
        let subject = format!("$O.{}.M.{}", &self.name, &encoded_object_name);
        let object_info = ObjectInfo {
            name: object_meta.name,
            description: object_meta.description,
            link: object_meta.link,
            bucket: self.name.clone(),
            nuid: object_nuid,
            chunks: object_chunks,
            size: object_size,
            digest: Some(format!("SHA-256={}", URL_SAFE.encode(digest))),
            modified: OffsetDateTime::now_utc(),
            deleted: false,
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            NATS_ROLLUP,
            ROLLUP_SUBJECT.parse::<HeaderValue>().map_err(|err| {
                PutError::with_source(
                    PutErrorKind::Other,
                    format!("failed parsing header: {}", err),
                )
            })?,
        );
        let data = serde_json::to_vec(&object_info).map_err(|err| {
            PutError::with_source(
                PutErrorKind::Other,
                format!("failed serializing object info: {}", err),
            )
        })?;

        // publish meta.
        self.stream
            .context
            .publish_with_headers(subject, headers, data.into())
            .await
            .map_err(|err| {
                PutError::with_source(
                    PutErrorKind::PublishMetadata,
                    format!("failed publishing metadata: {}", err),
                )
            })?
            .await
            .map_err(|err| {
                PutError::with_source(
                    PutErrorKind::PublishMetadata,
                    format!("failed ack from metadata publish: {}", err),
                )
            })?;

        // Purge any old chunks.
        if let Some(existing_object_info) = maybe_existing_object_info {
            let chunk_subject = format!("$O.{}.C.{}", &self.name, &existing_object_info.nuid);

            self.stream
                .purge()
                .filter(&chunk_subject)
                .await
                .map_err(|err| PutError::with_source(PutErrorKind::PurgeOldChunks, err))?;
        }

        Ok(object_info)
    }

    /// Creates a [Watch] stream over changes in the [ObjectStore].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let bucket = jetstream.get_object_store("store").await?;
    /// let mut watcher = bucket.watch().await.unwrap();
    /// while let Some(object) = watcher.next().await {
    ///     println!("detected changes in {:?}", object?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch(&self) -> Result<Watch<'_>, WatchError> {
        let subject = format!("$O.{}.M.>", self.name);
        let ordered = self
            .stream
            .create_consumer(crate::jetstream::consumer::push::OrderedConfig {
                deliver_policy: super::consumer::DeliverPolicy::New,
                deliver_subject: self.stream.context.client.new_inbox(),
                description: Some("object store watcher".to_string()),
                filter_subject: subject,
                ..Default::default()
            })
            .await?;
        Ok(Watch {
            subscription: ordered.messages().await?,
        })
    }

    /// Returns a [List] stream with all not deleted [Objects][Object] in the [ObjectStore].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let bucket = jetstream.get_object_store("store").await?;
    /// let mut list = bucket.list().await.unwrap();
    /// while let Some(object) = list.next().await {
    ///     println!("object {:?}", object?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list(&self) -> Result<List<'_>, ListError> {
        trace!("starting Object List");
        let subject = format!("$O.{}.M.>", self.name);
        let ordered = self
            .stream
            .create_consumer(crate::jetstream::consumer::push::OrderedConfig {
                deliver_policy: super::consumer::DeliverPolicy::All,
                deliver_subject: self.stream.context.client.new_inbox(),
                description: Some("object store list".to_string()),
                filter_subject: subject,
                ..Default::default()
            })
            .await?;
        Ok(List {
            done: ordered.info.num_pending == 0,
            subscription: ordered.messages().await?,
        })
    }

    /// Seals a [ObjectStore], preventing any further changes to it or its [Objects][Object].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let mut bucket = jetstream.get_object_store("store").await?;
    /// bucket.seal().await.unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn seal(&mut self) -> Result<(), SealError> {
        let mut stream_config = self
            .stream
            .info()
            .await
            .map_err(|err| SealError::with_source(SealErrorKind::Info, err))?
            .to_owned();
        stream_config.config.sealed = true;

        self.stream
            .context
            .update_stream(&stream_config.config)
            .await?;
        Ok(())
    }
}

pub struct Watch<'a> {
    subscription: crate::jetstream::consumer::push::Ordered<'a>,
}

impl Stream for Watch<'_> {
    type Item = Result<ObjectInfo, WatcherError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.subscription.poll_next_unpin(cx) {
            Poll::Ready(message) => match message {
                Some(message) => Poll::Ready(
                    serde_json::from_slice::<ObjectInfo>(&message?.payload)
                        .map_err(|err| {
                            WatcherError::with_source(
                                WatcherErrorKind::Other,
                                format!("failed to deserialize object info: {}", err),
                            )
                        })
                        .map_or_else(|err| Some(Err(err)), |result| Some(Ok(result))),
                ),
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct List<'a> {
    subscription: crate::jetstream::consumer::push::Ordered<'a>,
    done: bool,
}

impl Stream for List<'_> {
    type Item = Result<ObjectInfo, ListerError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if self.done {
                debug!("Object Store list done");
                return Poll::Ready(None);
            }

            match self.subscription.poll_next_unpin(cx) {
                Poll::Ready(message) => match message {
                    None => return Poll::Ready(None),
                    Some(message) => {
                        let message = message?;
                        let info = message
                            .info()
                            .map_err(|err| ListerError::with_source(ListerErrorKind::Other, err))?;
                        trace!("num pending: {}", info.pending);
                        if info.pending == 0 {
                            self.done = true;
                        }
                        let response: ObjectInfo = serde_json::from_slice(&message.payload)
                            .map_err(|err| {
                                ListerError::with_source(
                                    ListerErrorKind::Other,
                                    format!("failed deserializing object info: {}", err),
                                )
                            })?;
                        if response.deleted {
                            continue;
                        }
                        return Poll::Ready(Some(Ok(response)));
                    }
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Represents an object stored in a bucket.
pub struct Object<'a> {
    pub info: ObjectInfo,
    remaining_bytes: VecDeque<u8>,
    has_pending_messages: bool,
    digest: Option<ring::digest::Context>,
    subscription: Option<crate::jetstream::consumer::push::Ordered<'a>>,
}

impl<'a> Object<'a> {
    pub(crate) fn new(subscription: Ordered<'a>, info: ObjectInfo) -> Self {
        Object {
            subscription: Some(subscription),
            info,
            remaining_bytes: VecDeque::new(),
            has_pending_messages: true,
            digest: Some(ring::digest::Context::new(&SHA256)),
        }
    }

    /// Returns information about the object.
    pub fn info(&self) -> &ObjectInfo {
        &self.info
    }
}

impl tokio::io::AsyncRead for Object<'_> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let (buf1, _buf2) = self.remaining_bytes.as_slices();
        if !buf1.is_empty() {
            let len = cmp::min(buf.remaining(), buf1.len());
            buf.put_slice(&buf1[..len]);
            self.remaining_bytes.drain(..len);
            return Poll::Ready(Ok(()));
        }

        if self.has_pending_messages {
            if let Some(subscription) = self.subscription.as_mut() {
                match subscription.poll_next_unpin(cx) {
                    Poll::Ready(message) => match message {
                        Some(message) => {
                            let message = message.map_err(|err| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("error from JetStream subscription: {err}"),
                                )
                            })?;
                            let len = cmp::min(buf.remaining(), message.payload.len());
                            buf.put_slice(&message.payload[..len]);
                            if let Some(context) = &mut self.digest {
                                context.update(&message.payload);
                            }
                            self.remaining_bytes.extend(&message.payload[len..]);

                            let info = message.info().map_err(|err| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("error from JetStream subscription: {err}"),
                                )
                            })?;
                            if info.pending == 0 {
                                let digest = self.digest.take().map(|context| context.finish());
                                if let Some(digest) = digest {
                                    if self
                                        .info
                                        .digest
                                        .as_ref()
                                        .map(|digest_self| {
                                            format!("SHA-256={}", URL_SAFE.encode(digest))
                                                != *digest_self
                                        })
                                        .unwrap_or(false)
                                    {
                                        return Poll::Ready(Err(std::io::Error::new(
                                            std::io::ErrorKind::InvalidData,
                                            "wrong digest",
                                        )));
                                    }
                                } else {
                                    return Poll::Ready(Err(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "digest should be Some",
                                    )));
                                }
                                self.has_pending_messages = false;
                                self.subscription = None;
                            }
                            Poll::Ready(Ok(()))
                        }
                        None => Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "subscription ended before reading whole object",
                        ))),
                    },
                    Poll::Pending => Poll::Pending,
                }
            } else {
                Poll::Ready(Ok(()))
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

/// Meta and instance information about an object.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObjectInfo {
    /// Name of the object
    pub name: String,
    /// A short human readable description of the object.
    pub description: Option<String>,
    /// Link this object points to, if any.
    pub link: Option<ObjectLink>,
    /// Name of the bucket the object is stored in.
    pub bucket: String,
    /// Unique identifier used to uniquely identify this version of the object.
    pub nuid: String,
    /// Size in bytes of the object.
    pub size: usize,
    /// Number of chunks the object is stored in.
    pub chunks: usize,
    /// Date and time the object was last modified.
    #[serde(with = "rfc3339")]
    #[serde(rename = "mtime")]
    pub modified: time::OffsetDateTime,
    /// Digest of the object stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Set to true if the object has been deleted.
    #[serde(default, skip_serializing_if = "is_default")]
    pub deleted: bool,
}

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}
/// A link to another object, potentially in another bucket.
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObjectLink {
    /// Name of the object
    pub name: String,
    /// Name of the bucket the object is stored in.
    pub bucket: Option<String>,
}

/// Meta information about an object.
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObjectMeta {
    /// Name of the object
    pub name: String,
    /// A short human readable description of the object.
    pub description: Option<String>,
    /// Link this object points to, if any.
    pub link: Option<ObjectLink>,
}

impl From<&str> for ObjectMeta {
    fn from(s: &str) -> ObjectMeta {
        ObjectMeta {
            name: s.to_string(),
            ..Default::default()
        }
    }
}

#[derive(Debug)]
pub struct InfoError {
    kind: InfoErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum InfoErrorKind {
    InvalidName,
    NotFound,
    Other,
    TimedOut,
}

impl Display for InfoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            InfoErrorKind::InvalidName => write!(f, "invalid object name"),
            InfoErrorKind::Other => write!(f, "getting info failed: {}", self.format_source()),
            InfoErrorKind::NotFound => write!(f, "not found"),
            InfoErrorKind::TimedOut => write!(f, "timed out"),
        }
    }
}

crate::error_impls!(InfoError, InfoErrorKind);

#[derive(Debug)]
pub struct GetError {
    kind: GetErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}
#[derive(Debug, PartialEq, Clone)]
pub enum GetErrorKind {
    InvalidName,
    ConsumerCreate,
    NotFound,
    Other,
    TimedOut,
}
crate::error_impls!(GetError, GetErrorKind);
crate::from_with_timeout!(GetError, GetErrorKind, ConsumerError, ConsumerErrorKind);
crate::from_with_timeout!(GetError, GetErrorKind, StreamError, StreamErrorKind);

impl From<InfoError> for GetError {
    fn from(err: InfoError) -> Self {
        match err.kind() {
            InfoErrorKind::InvalidName => GetError::new(GetErrorKind::InvalidName),
            InfoErrorKind::NotFound => GetError::new(GetErrorKind::NotFound),
            InfoErrorKind::Other => GetError::with_source(GetErrorKind::Other, err),
            InfoErrorKind::TimedOut => GetError::new(GetErrorKind::TimedOut),
        }
    }
}

impl Display for GetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind() {
            GetErrorKind::ConsumerCreate => {
                write!(
                    f,
                    "failed creating consumer for fetching object: {}",
                    self.format_source()
                )
            }
            GetErrorKind::Other => write!(f, "failed getting object: {}", self.format_source()),
            GetErrorKind::NotFound => write!(f, "object not found"),
            GetErrorKind::TimedOut => write!(f, "timed out"),
            GetErrorKind::InvalidName => write!(f, "invalid object name"),
        }
    }
}

#[derive(Debug)]
pub struct DeleteError {
    kind: DeleteErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeleteErrorKind {
    TimedOut,
    NotFound,
    Metadata,
    InvalidName,
    Chunks,
    Other,
}

impl Display for DeleteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind() {
            DeleteErrorKind::TimedOut => write!(f, "timed out"),
            DeleteErrorKind::Metadata => {
                write!(f, "failed rolling up metadata: {}", self.format_source())
            }
            DeleteErrorKind::Chunks => write!(f, "failed purging chunks: {}", self.format_source()),
            DeleteErrorKind::Other => write!(f, "delete failed: {}", self.format_source()),
            DeleteErrorKind::NotFound => write!(f, "object not found"),
            DeleteErrorKind::InvalidName => write!(f, "invalid object name"),
        }
    }
}

impl From<InfoError> for DeleteError {
    fn from(err: InfoError) -> Self {
        match err.kind() {
            InfoErrorKind::InvalidName => DeleteError::new(DeleteErrorKind::InvalidName),
            InfoErrorKind::NotFound => DeleteError::new(DeleteErrorKind::NotFound),
            InfoErrorKind::Other => DeleteError::with_source(DeleteErrorKind::Other, err),
            InfoErrorKind::TimedOut => DeleteError::new(DeleteErrorKind::TimedOut),
        }
    }
}

crate::error_impls!(DeleteError, DeleteErrorKind);
crate::from_with_timeout!(DeleteError, DeleteErrorKind, PublishError, PublishErrorKind);
crate::from_with_timeout!(DeleteError, DeleteErrorKind, PurgeError, PurgeErrorKind);

#[derive(Debug)]
pub struct PutError {
    kind: PutErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PutErrorKind {
    InvalidName,
    ReadChunks,
    PublishChunks,
    PublishMetadata,
    PurgeOldChunks,
    TimedOut,
    Other,
}

crate::error_impls!(PutError, PutErrorKind);

impl Display for PutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind() {
            PutErrorKind::PublishChunks => {
                write!(
                    f,
                    "failed publishing object chunks: {}",
                    self.format_source()
                )
            }
            PutErrorKind::PublishMetadata => {
                write!(f, "failed publishing metadata: {}", self.format_source())
            }
            PutErrorKind::PurgeOldChunks => {
                write!(f, "falied purging old chunks: {}", self.format_source())
            }
            PutErrorKind::TimedOut => write!(f, "timed out"),
            PutErrorKind::Other => write!(f, "error: {}", self.format_source()),
            PutErrorKind::InvalidName => write!(f, "invalid object name"),
            PutErrorKind::ReadChunks => write!(
                f,
                "error while reading the buffer: {}",
                self.format_source()
            ),
        }
    }
}

#[derive(Debug)]
pub struct WatchError {
    kind: WatchErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum WatchErrorKind {
    TimedOut,
    ConsumerCreate,
    Other,
}

crate::error_impls!(WatchError, WatchErrorKind);
crate::from_with_timeout!(WatchError, WatchErrorKind, ConsumerError, ConsumerErrorKind);
crate::from_with_timeout!(WatchError, WatchErrorKind, StreamError, StreamErrorKind);

impl Display for WatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            WatchErrorKind::ConsumerCreate => {
                write!(
                    f,
                    "watch consumer creation failed: {}",
                    self.format_source()
                )
            }
            WatchErrorKind::Other => write!(f, "watch failed: {}", self.format_source()),
            WatchErrorKind::TimedOut => write!(f, "timed out"),
        }
    }
}

pub type ListError = WatchError;
pub type ListErrorKind = WatchErrorKind;

#[derive(Debug)]
pub struct SealError {
    kind: SealErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SealErrorKind {
    TimedOut,
    Other,
    Info,
    Update,
}

crate::error_impls!(SealError, SealErrorKind);

impl Display for SealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            SealErrorKind::TimedOut => write!(f, "timed out"),
            SealErrorKind::Other => write!(f, "seal failed: {}", self.format_source()),
            SealErrorKind::Info => write!(
                f,
                "failed getting stream info before sealing bucket: {}",
                self.format_source()
            ),
            SealErrorKind::Update => {
                write!(f, "failed sealing the bucket: {}", self.format_source())
            }
        }
    }
}

impl From<super::context::UpdateStreamError> for SealError {
    fn from(err: super::context::UpdateStreamError) -> Self {
        match err.kind() {
            super::context::CreateStreamErrorKind::TimedOut => {
                SealError::new(SealErrorKind::TimedOut)
            }
            _ => SealError::with_source(SealErrorKind::Update, err),
        }
    }
}

#[derive(Debug)]
pub struct WatcherError {
    kind: WatcherErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WatcherErrorKind {
    ConsumerError,
    Other,
}

impl Display for WatcherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            WatcherErrorKind::ConsumerError => {
                write!(f, "watcher consumer error: {}", self.format_source())
            }
            WatcherErrorKind::Other => write!(f, "watcher error: {}", self.format_source()),
        }
    }
}

crate::error_impls!(WatcherError, WatcherErrorKind);

impl From<OrderedError> for WatcherError {
    fn from(err: OrderedError) -> Self {
        WatcherError::with_source(WatcherErrorKind::ConsumerError, err)
    }
}

pub type ListerError = WatcherError;
pub type ListerErrorKind = WatcherErrorKind;
