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
use std::{
    cmp,
    io::{self, ErrorKind},
    str::FromStr,
    task::Poll,
    time::Duration,
};

use crate::{HeaderMap, HeaderValue};
use base64::URL_SAFE;
use ring::digest::SHA256;
use tokio::io::AsyncReadExt;

use base64_url::base64;
use futures::{Stream, StreamExt};
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::Error;

use super::{consumer::push::Ordered, stream::StorageType};
use time::{serde::rfc3339, OffsetDateTime};

const DEFAULT_CHUNK_SIZE: usize = 128 * 1024;
const NATS_ROLLUP: &str = "Nats-Rollup";
const ROLLUP_SUBJECT: &str = "sub";

lazy_static! {
    static ref BUCKET_NAME_RE: Regex = Regex::new(r#"\A[a-zA-Z0-9_-]+\z"#).unwrap();
    static ref OBJECT_NAME_RE: Regex = Regex::new(r#"\A[-/_=\.a-zA-Z0-9]+\z"#).unwrap();
}

pub(crate) fn is_valid_bucket_name(bucket_name: &str) -> bool {
    BUCKET_NAME_RE.is_match(bucket_name)
}

pub(crate) fn is_valid_object_name(object_name: &str) -> bool {
    if object_name.is_empty() || object_name.starts_with('.') || object_name.ends_with('.') {
        return false;
    }

    OBJECT_NAME_RE.is_match(object_name)
}

pub(crate) fn enocde_object_name(object_name: &str) -> String {
    base64::encode_config(object_name, base64::URL_SAFE)
}

/// Configuration values for object store buckets.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Name of the storage bucket.
    pub bucket: String,
    /// A short description of the purpose of this storage bucket.
    pub description: Option<String>,
    /// Maximum age of any value in the bucket, expressed in nanoseconds
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
    /// Gets an [Object] from the [Store].
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
    pub async fn get<T: AsRef<str>>(&self, object_name: T) -> Result<Object<'_>, Error> {
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

    /// Gets an [Object] from the [Store].
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
    pub async fn delete<T: AsRef<str>>(&self, object_name: T) -> Result<(), Error> {
        let object_name = object_name.as_ref();
        let mut object_info = self.info(object_name).await?;
        object_info.chunks = 0;
        object_info.size = 0;
        object_info.deleted = true;

        let data = serde_json::to_vec(&object_info)?;

        let mut headers = HeaderMap::default();
        headers.insert(NATS_ROLLUP, HeaderValue::from_str(ROLLUP_SUBJECT)?);

        let subject = format!("$O.{}.M.{}", &self.name, enocde_object_name(object_name));

        self.stream
            .context
            .publish_with_headers(subject, headers, data.into())
            .await?;

        let chunk_subject = format!("$O.{}.C.{}", self.name, object_info.nuid);

        self.stream.purge_subject(&chunk_subject).await?;

        Ok(())
    }

    /// Retrieves [Object] [Info].
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
    pub async fn info<T: AsRef<str>>(&self, object_name: T) -> Result<ObjectInfo, Error> {
        let object_name = object_name.as_ref();
        let object_name = enocde_object_name(object_name);
        if !is_valid_object_name(&object_name) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid object name",
            )));
        }

        // Grab last meta value we have.
        let subject = format!("$O.{}.M.{}", &self.name, &object_name);

        let message = self
            .stream
            .get_last_raw_message_by_subject(subject.as_str())
            .await?;
        let decoded_payload = base64::decode(message.payload)
            .map_err(|err| Box::new(std::io::Error::new(ErrorKind::Other, err)))?;
        let object_info = serde_json::from_slice::<ObjectInfo>(&decoded_payload)?;

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
    ) -> Result<ObjectInfo, Error>
    where
        ObjectMeta: From<T>,
    {
        let object_meta: ObjectMeta = meta.into();

        let encoded_object_name = enocde_object_name(&object_meta.name);
        if !is_valid_object_name(&encoded_object_name) {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid object name",
            )));
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

        let mut buffer = [0; DEFAULT_CHUNK_SIZE];
        let mut context = ring::digest::Context::new(&SHA256);

        loop {
            let n = data.read(&mut buffer).await?;

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
                .await?;
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
            digest: format!(
                "SHA-256={}",
                base64::encode_config(digest, base64::URL_SAFE)
            ),
            modified: OffsetDateTime::now_utc(),
            deleted: false,
        };

        let mut headers = HeaderMap::new();
        headers.insert(NATS_ROLLUP, ROLLUP_SUBJECT.parse::<HeaderValue>()?);
        let data = serde_json::to_vec(&object_info)?;

        // publish meta.
        self.stream
            .context
            .publish_with_headers(subject, headers, data.into())
            .await?;

        // Purge any old chunks.
        if let Some(existing_object_info) = maybe_existing_object_info {
            let chunk_subject = format!("$O.{}.C.{}", &self.name, &existing_object_info.nuid);

            self.stream.purge_subject(&chunk_subject).await?;
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
    pub async fn watch(&self) -> Result<Watch<'_>, Error> {
        let subject = format!("$O.{}.M.>", self.name);
        let ordered = self
            .stream
            .create_consumer(crate::jetstream::consumer::push::OrderedConfig {
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
    pub async fn seal(&mut self) -> Result<(), Error> {
        let mut stream_config = self.stream.info().await?.to_owned();
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
    type Item = Result<ObjectInfo, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.subscription.poll_next_unpin(cx) {
            Poll::Ready(message) => match message {
                Some(message) => Poll::Ready(
                    serde_json::from_slice::<ObjectInfo>(&message?.payload)
                        .map_err(|err| {
                            Box::from(io::Error::new(
                                ErrorKind::Other,
                                format!("failed to deserialize the reponse: {:?}", err),
                            ))
                        })
                        .map_or_else(|err| Some(Err(err)), |result| Some(Ok(result))),
                ),
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Represents an object stored in a bucket.
pub struct Object<'a> {
    pub info: ObjectInfo,
    remaining_bytes: Vec<u8>,
    has_pending_messages: bool,
    digest: Option<ring::digest::Context>,
    subscription: crate::jetstream::consumer::push::Ordered<'a>,
}

impl<'a> Object<'a> {
    pub(crate) fn new(subscription: Ordered<'a>, info: ObjectInfo) -> Self {
        Object {
            subscription,
            info,
            remaining_bytes: Vec::new(),
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
        if !self.remaining_bytes.is_empty() {
            let len = cmp::min(buf.remaining(), self.remaining_bytes.len());
            buf.put_slice(&self.remaining_bytes[..len]);
            self.remaining_bytes = self.remaining_bytes[len..].to_vec();
            return Poll::Ready(Ok(()));
        }

        if self.has_pending_messages {
            match self.subscription.poll_next_unpin(cx) {
                Poll::Ready(message) => match message {
                    Some(message) => {
                        let message = message.map_err(|err| {
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("error from JetStream subscription: {}", err),
                            )
                        })?;
                        let len = cmp::min(buf.remaining(), message.payload.len());
                        buf.put_slice(&message.payload[..len]);
                        if let Some(context) = &mut self.digest {
                            context.update(&message.payload);
                        }
                        self.remaining_bytes
                            .extend_from_slice(&message.payload[len..]);

                        let info = message.info().map_err(|err| {
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("error from JetStream subscription: {}", err),
                            )
                        })?;
                        if info.pending == 0 {
                            let digest = self.digest.take().map(|context| context.finish());
                            if let Some(digest) = digest {
                                if format!("SHA-256={}", base64::encode_config(digest, URL_SAFE))
                                    != self.info.digest
                                {
                                    return Poll::Ready(Err(io::Error::new(
                                        ErrorKind::InvalidData,
                                        "wrong digest",
                                    )));
                                }
                            } else {
                                return Poll::Ready(Err(io::Error::new(
                                    ErrorKind::InvalidData,
                                    "digest should be Some",
                                )));
                            }
                            self.has_pending_messages = false;
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
    pub modified: time::OffsetDateTime,
    /// Digest of the object stream.
    pub digest: String,
    /// Set to true if the object has been deleted.
    pub deleted: bool,
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
