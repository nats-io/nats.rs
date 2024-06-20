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

use crate::crypto::Sha256;
use crate::subject::Subject;
use crate::{HeaderMap, HeaderValue};
use base64::engine::general_purpose::{STANDARD, URL_SAFE};
use base64::engine::Engine;
use bytes::BytesMut;
use futures::future::BoxFuture;
use once_cell::sync::Lazy;
use tokio::io::AsyncReadExt;

use futures::{Stream, StreamExt};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

use super::consumer::push::{OrderedConfig, OrderedError};
use super::consumer::{DeliverPolicy, StreamError, StreamErrorKind};
use super::context::{PublishError, PublishErrorKind};
use super::stream::{self, ConsumerError, ConsumerErrorKind, PurgeError, PurgeErrorKind};
use super::{consumer::push::Ordered, stream::StorageType};
use crate::error::Error;
use time::{serde::rfc3339, OffsetDateTime};

const DEFAULT_CHUNK_SIZE: usize = 128 * 1024;
const NATS_ROLLUP: &str = "Nats-Rollup";
const ROLLUP_SUBJECT: &str = "sub";

static BUCKET_NAME_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\A[a-zA-Z0-9_-]+\z").unwrap());
static OBJECT_NAME_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\A[-/_=\.a-zA-Z0-9]+\z").unwrap());

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
    #[serde(default, with = "serde_nanos")]
    pub max_age: Duration,
    /// How large the storage bucket may become in total bytes.
    pub max_bytes: i64,
    /// The type of storage backend, `File` (default) and `Memory`
    pub storage: StorageType,
    /// How many replicas to keep for each value in a cluster, maximum 5.
    pub num_replicas: usize,
    /// Sets compression of the underlying stream.
    pub compression: bool,
    // Cluster and tag placement.
    pub placement: Option<stream::Placement>,
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
    pub async fn get<T: AsRef<str> + Send>(&self, object_name: T) -> Result<Object, GetError> {
        self.get_impl(object_name).await
    }

    fn get_impl<'bucket, 'future, T>(
        &'bucket self,
        object_name: T,
    ) -> BoxFuture<'future, Result<Object, GetError>>
    where
        T: AsRef<str> + Send + 'future,
        'bucket: 'future,
    {
        Box::pin(async move {
            let object_info = self.info(object_name).await?;
            if let Some(ref options) = object_info.options {
                if let Some(link) = options.link.as_ref() {
                    if let Some(link_name) = link.name.as_ref() {
                        let link_name = link_name.clone();
                        debug!("getting object via link");
                        if link.bucket == self.name {
                            return self.get_impl(link_name).await;
                        } else {
                            let bucket = self
                                .stream
                                .context
                                .get_object_store(&link.bucket)
                                .await
                                .map_err(|err| {
                                GetError::with_source(GetErrorKind::Other, err)
                            })?;
                            let object = bucket.get_impl(&link_name).await?;
                            return Ok(object);
                        }
                    } else {
                        return Err(GetError::new(GetErrorKind::BucketLink));
                    }
                }
            }

            debug!("not a link. Getting the object");
            Ok(Object::new(object_info, self.stream.clone()))
        })
    }

    /// Deletes an [Object] from the [ObjectStore].
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
                stream::LastRawMessageErrorKind::NoMessageFound => {
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
        ObjectMetadata: From<T>,
    {
        let object_meta: ObjectMetadata = meta.into();

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
        let chunk_subject = Subject::from(format!("$O.{}.C.{}", &self.name, &object_nuid));

        let mut object_chunks = 0;
        let mut object_size = 0;

        let chunk_size = object_meta.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);
        let mut buffer = BytesMut::with_capacity(chunk_size);
        let mut sha256 = Sha256::new();

        loop {
            let n = data
                .read_buf(&mut buffer)
                .await
                .map_err(|err| PutError::with_source(PutErrorKind::ReadChunks, err))?;

            if n == 0 {
                break;
            }

            let payload = buffer.split().freeze();
            sha256.update(&payload);

            object_size += payload.len();
            object_chunks += 1;

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
        let digest = sha256.finish();
        let subject = format!("$O.{}.M.{}", &self.name, &encoded_object_name);
        let object_info = ObjectInfo {
            name: object_meta.name,
            description: object_meta.description,
            options: Some(ObjectOptions {
                max_chunk_size: Some(chunk_size),
                link: None,
            }),
            bucket: self.name.clone(),
            nuid: object_nuid.to_string(),
            chunks: object_chunks,
            size: object_size,
            digest: Some(format!("SHA-256={}", URL_SAFE.encode(digest))),
            modified: Some(OffsetDateTime::now_utc()),
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
    pub async fn watch(&self) -> Result<Watch, WatchError> {
        self.watch_with_deliver_policy(DeliverPolicy::New).await
    }

    /// Creates a [Watch] stream over changes in the [ObjectStore] which yields values whenever
    /// there are changes for that key with as well as last value.
    pub async fn watch_with_history(&self) -> Result<Watch, WatchError> {
        self.watch_with_deliver_policy(DeliverPolicy::LastPerSubject)
            .await
    }

    async fn watch_with_deliver_policy(
        &self,
        deliver_policy: DeliverPolicy,
    ) -> Result<Watch, WatchError> {
        let subject = format!("$O.{}.M.>", self.name);
        let ordered = self
            .stream
            .create_consumer(crate::jetstream::consumer::push::OrderedConfig {
                deliver_policy,
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
    pub async fn list(&self) -> Result<List, ListError> {
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
            subscription: Some(ordered.messages().await?),
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

    /// Updates [Object] [ObjectMetadata].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::object_store;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let mut bucket = jetstream.get_object_store("store").await?;
    /// bucket
    ///     .update_metadata(
    ///         "object",
    ///         object_store::UpdateMetadata {
    ///             name: "new_name".to_string(),
    ///             description: Some("a new description".to_string()),
    ///         },
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_metadata<A: AsRef<str>>(
        &self,
        object: A,
        metadata: UpdateMetadata,
    ) -> Result<ObjectInfo, UpdateMetadataError> {
        let mut info = self.info(object.as_ref()).await?;

        // If name is being update, we need to check if other metadata with it already exists.
        // If does, error. Otherwise, purge old name metadata.
        if metadata.name != info.name {
            tracing::info!("new metadata name is different than then old one");
            if !is_valid_object_name(&metadata.name) {
                return Err(UpdateMetadataError::new(
                    UpdateMetadataErrorKind::InvalidName,
                ));
            }
            match self.info(&metadata.name).await {
                Ok(_) => {
                    return Err(UpdateMetadataError::new(
                        UpdateMetadataErrorKind::NameAlreadyInUse,
                    ))
                }
                Err(err) => match err.kind() {
                    InfoErrorKind::NotFound => {
                        tracing::info!("purging old metadata: {}", info.name);
                        self.stream
                            .purge()
                            .filter(format!(
                                "$O.{}.M.{}",
                                self.name,
                                encode_object_name(&info.name)
                            ))
                            .await
                            .map_err(|err| {
                                UpdateMetadataError::with_source(
                                    UpdateMetadataErrorKind::Purge,
                                    err,
                                )
                            })?;
                    }
                    _ => {
                        return Err(UpdateMetadataError::with_source(
                            UpdateMetadataErrorKind::Other,
                            err,
                        ))
                    }
                },
            }
        }

        info.name = metadata.name;
        info.description = metadata.description;

        let name = encode_object_name(&info.name);
        let subject = format!("$O.{}.M.{}", &self.name, &name);

        let mut headers = HeaderMap::new();
        headers.insert(
            NATS_ROLLUP,
            ROLLUP_SUBJECT.parse::<HeaderValue>().map_err(|err| {
                UpdateMetadataError::with_source(
                    UpdateMetadataErrorKind::Other,
                    format!("failed parsing header: {}", err),
                )
            })?,
        );
        let data = serde_json::to_vec(&info).map_err(|err| {
            UpdateMetadataError::with_source(
                UpdateMetadataErrorKind::Other,
                format!("failed serializing object info: {}", err),
            )
        })?;

        // publish meta.
        self.stream
            .context
            .publish_with_headers(subject, headers, data.into())
            .await
            .map_err(|err| {
                UpdateMetadataError::with_source(
                    UpdateMetadataErrorKind::PublishMetadata,
                    format!("failed publishing metadata: {}", err),
                )
            })?
            .await
            .map_err(|err| {
                UpdateMetadataError::with_source(
                    UpdateMetadataErrorKind::PublishMetadata,
                    format!("failed ack from metadata publish: {}", err),
                )
            })?;

        Ok(info)
    }

    /// Adds a link to an [Object].
    /// It creates a new [Object] in the [ObjectStore] that points to another [Object]
    /// and does not have any contents on it's own.
    /// Links are automatically followed (one level deep) when calling [ObjectStore::get].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::object_store;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let bucket = jetstream.get_object_store("bucket").await?;
    /// let object = bucket.get("object").await?;
    /// bucket.add_link("link_to_object", &object).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_link<'a, T, O>(&self, name: T, object: O) -> Result<ObjectInfo, AddLinkError>
    where
        T: ToString,
        O: AsObjectInfo,
    {
        let object = object.as_info();
        let name = name.to_string();
        if name.is_empty() {
            return Err(AddLinkError::new(AddLinkErrorKind::EmptyName));
        }
        if object.name.is_empty() {
            return Err(AddLinkError::new(AddLinkErrorKind::ObjectRequired));
        }
        if object.deleted {
            return Err(AddLinkError::new(AddLinkErrorKind::Deleted));
        }
        if let Some(ref options) = object.options {
            if options.link.is_some() {
                return Err(AddLinkError::new(AddLinkErrorKind::LinkToLink));
            }
        }
        match self.info(&name).await {
            Ok(info) => {
                if let Some(options) = info.options {
                    if options.link.is_none() {
                        return Err(AddLinkError::new(AddLinkErrorKind::AlreadyExists));
                    }
                } else {
                    return Err(AddLinkError::new(AddLinkErrorKind::AlreadyExists));
                }
            }
            Err(err) if err.kind() != InfoErrorKind::NotFound => {
                return Err(AddLinkError::with_source(AddLinkErrorKind::Other, err))
            }
            _ => (),
        }

        let info = ObjectInfo {
            name,
            description: None,
            options: Some(ObjectOptions {
                link: Some(ObjectLink {
                    name: Some(object.name.clone()),
                    bucket: object.bucket.clone(),
                }),
                max_chunk_size: None,
            }),
            bucket: self.name.clone(),
            nuid: nuid::next().to_string(),
            size: 0,
            chunks: 0,
            modified: Some(OffsetDateTime::now_utc()),
            digest: None,
            deleted: false,
        };
        publish_meta(self, &info).await?;
        Ok(info)
    }

    /// Adds a link to another [ObjectStore] bucket by creating a new [Object]
    /// in the current [ObjectStore] that points to another [ObjectStore] and
    /// does not contain any data.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::object_store;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let bucket = jetstream.get_object_store("bucket").await?;
    /// bucket
    ///     .add_bucket_link("link_to_object", "another_bucket")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_bucket_link<T: ToString, U: ToString>(
        &self,
        name: T,
        bucket: U,
    ) -> Result<ObjectInfo, AddLinkError> {
        let name = name.to_string();
        let bucket = bucket.to_string();
        if name.is_empty() {
            return Err(AddLinkError::new(AddLinkErrorKind::EmptyName));
        }

        match self.info(&name).await {
            Ok(info) => {
                if let Some(options) = info.options {
                    if options.link.is_none() {
                        return Err(AddLinkError::new(AddLinkErrorKind::AlreadyExists));
                    }
                }
            }
            Err(err) if err.kind() != InfoErrorKind::NotFound => {
                return Err(AddLinkError::with_source(AddLinkErrorKind::Other, err))
            }
            _ => (),
        }

        let info = ObjectInfo {
            name: name.clone(),
            description: None,
            options: Some(ObjectOptions {
                link: Some(ObjectLink { name: None, bucket }),
                max_chunk_size: None,
            }),
            bucket: self.name.clone(),
            nuid: nuid::next().to_string(),
            size: 0,
            chunks: 0,
            modified: Some(OffsetDateTime::now_utc()),
            digest: None,
            deleted: false,
        };
        publish_meta(self, &info).await?;
        Ok(info)
    }
}

async fn publish_meta(store: &ObjectStore, info: &ObjectInfo) -> Result<(), PublishMetadataError> {
    let encoded_object_name = encode_object_name(&info.name);
    let subject = format!("$O.{}.M.{}", &store.name, &encoded_object_name);

    let mut headers = HeaderMap::new();
    headers.insert(
        NATS_ROLLUP,
        ROLLUP_SUBJECT.parse::<HeaderValue>().map_err(|err| {
            PublishMetadataError::with_source(
                PublishMetadataErrorKind::Other,
                format!("failed parsing header: {}", err),
            )
        })?,
    );
    let data = serde_json::to_vec(&info).map_err(|err| {
        PublishMetadataError::with_source(
            PublishMetadataErrorKind::Other,
            format!("failed serializing object info: {}", err),
        )
    })?;

    store
        .stream
        .context
        .publish_with_headers(subject, headers, data.into())
        .await
        .map_err(|err| {
            PublishMetadataError::with_source(
                PublishMetadataErrorKind::PublishMetadata,
                format!("failed publishing metadata: {}", err),
            )
        })?
        .await
        .map_err(|err| {
            PublishMetadataError::with_source(
                PublishMetadataErrorKind::PublishMetadata,
                format!("failed ack from metadata publish: {}", err),
            )
        })?;
    Ok(())
}

pub struct Watch {
    subscription: crate::jetstream::consumer::push::Ordered,
}

impl Stream for Watch {
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

pub struct List {
    subscription: Option<crate::jetstream::consumer::push::Ordered>,
    done: bool,
}

impl Stream for List {
    type Item = Result<ObjectInfo, ListerError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if self.done {
                debug!("Object Store list done");
                self.subscription = None;
                return Poll::Ready(None);
            }

            if let Some(subscription) = self.subscription.as_mut() {
                match subscription.poll_next_unpin(cx) {
                    Poll::Ready(message) => match message {
                        None => return Poll::Ready(None),
                        Some(message) => {
                            let message = message?;
                            let info = message.info().map_err(|err| {
                                ListerError::with_source(ListerErrorKind::Other, err)
                            })?;
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
            } else {
                return Poll::Ready(None);
            }
        }
    }
}

/// Represents an object stored in a bucket.
pub struct Object {
    pub info: ObjectInfo,
    remaining_bytes: VecDeque<u8>,
    has_pending_messages: bool,
    digest: Option<Sha256>,
    subscription: Option<crate::jetstream::consumer::push::Ordered>,
    subscription_future: Option<BoxFuture<'static, Result<Ordered, StreamError>>>,
    stream: crate::jetstream::stream::Stream,
}

impl Object {
    pub(crate) fn new(info: ObjectInfo, stream: stream::Stream) -> Self {
        Object {
            subscription: None,
            info,
            remaining_bytes: VecDeque::new(),
            has_pending_messages: true,
            digest: Some(Sha256::new()),
            subscription_future: None,
            stream,
        }
    }

    /// Returns information about the object.
    pub fn info(&self) -> &ObjectInfo {
        &self.info
    }
}

impl tokio::io::AsyncRead for Object {
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
            if self.subscription.is_none() {
                let future = match self.subscription_future.as_mut() {
                    Some(future) => future,
                    None => {
                        let stream = self.stream.clone();
                        let bucket = self.info.bucket.clone();
                        let nuid = self.info.nuid.clone();
                        self.subscription_future.insert(Box::pin(async move {
                            stream
                                .create_consumer(OrderedConfig {
                                    deliver_subject: stream.context.client.new_inbox(),
                                    filter_subject: format!("$O.{}.C.{}", bucket, nuid),
                                    ..Default::default()
                                })
                                .await
                                .unwrap()
                                .messages()
                                .await
                        }))
                    }
                };
                match future.as_mut().poll(cx) {
                    Poll::Ready(subscription) => {
                        self.subscription = Some(subscription.unwrap());
                    }
                    Poll::Pending => (),
                }
            }
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
                                let digest = self.digest.take().map(Sha256::finish);
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
                Poll::Pending
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObjectOptions {
    pub link: Option<ObjectLink>,
    pub max_chunk_size: Option<usize>,
}

/// Meta and instance information about an object.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObjectInfo {
    /// Name of the object
    pub name: String,
    /// A short human readable description of the object.
    #[serde(default)]
    pub description: Option<String>,
    /// Link this object points to, if any.
    #[serde(default)]
    pub options: Option<ObjectOptions>,
    /// Name of the bucket the object is stored in.
    pub bucket: String,
    /// Unique identifier used to uniquely identify this version of the object.
    #[serde(default)]
    pub nuid: String,
    /// Size in bytes of the object.
    #[serde(default)]
    pub size: usize,
    /// Number of chunks the object is stored in.
    #[serde(default)]
    pub chunks: usize,
    /// Date and time the object was last modified.
    #[serde(default, with = "rfc3339::option")]
    #[serde(rename = "mtime")]
    pub modified: Option<time::OffsetDateTime>,
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
    pub name: Option<String>,
    /// Name of the bucket the object is stored in.
    pub bucket: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct UpdateMetadata {
    /// Name of the object
    pub name: String,
    /// A short human readable description of the object.
    pub description: Option<String>,
}

/// Meta information about an object.
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObjectMetadata {
    /// Name of the object
    pub name: String,
    /// A short human readable description of the object.
    pub description: Option<String>,
    /// Max chunk size. Default is 128k.
    pub chunk_size: Option<usize>,
}

impl From<&str> for ObjectMetadata {
    fn from(s: &str) -> ObjectMetadata {
        ObjectMetadata {
            name: s.to_string(),
            ..Default::default()
        }
    }
}

pub trait AsObjectInfo {
    fn as_info(&self) -> &ObjectInfo;
}

impl AsObjectInfo for &Object {
    fn as_info(&self) -> &ObjectInfo {
        &self.info
    }
}
impl AsObjectInfo for &ObjectInfo {
    fn as_info(&self) -> &ObjectInfo {
        self
    }
}

impl From<ObjectInfo> for ObjectMetadata {
    fn from(info: ObjectInfo) -> Self {
        ObjectMetadata {
            name: info.name,
            description: info.description,
            chunk_size: None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum UpdateMetadataErrorKind {
    InvalidName,
    NotFound,
    TimedOut,
    Other,
    PublishMetadata,
    NameAlreadyInUse,
    Purge,
}

impl Display for UpdateMetadataErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName => write!(f, "invalid object name"),
            Self::NotFound => write!(f, "object not found"),
            Self::TimedOut => write!(f, "timed out"),
            Self::Other => write!(f, "error"),
            Self::PublishMetadata => {
                write!(f, "failed publishing metadata")
            }
            Self::NameAlreadyInUse => {
                write!(f, "object with updated name already exists")
            }
            Self::Purge => write!(f, "failed purging old name metadata"),
        }
    }
}

impl From<InfoError> for UpdateMetadataError {
    fn from(error: InfoError) -> Self {
        match error.kind() {
            InfoErrorKind::InvalidName => {
                UpdateMetadataError::new(UpdateMetadataErrorKind::InvalidName)
            }
            InfoErrorKind::NotFound => UpdateMetadataError::new(UpdateMetadataErrorKind::NotFound),
            InfoErrorKind::Other => {
                UpdateMetadataError::with_source(UpdateMetadataErrorKind::Other, error)
            }
            InfoErrorKind::TimedOut => UpdateMetadataError::new(UpdateMetadataErrorKind::TimedOut),
        }
    }
}

pub type UpdateMetadataError = Error<UpdateMetadataErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum InfoErrorKind {
    InvalidName,
    NotFound,
    Other,
    TimedOut,
}

impl Display for InfoErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName => write!(f, "invalid object name"),
            Self::Other => write!(f, "getting info failed"),
            Self::NotFound => write!(f, "not found"),
            Self::TimedOut => write!(f, "timed out"),
        }
    }
}

pub type InfoError = Error<InfoErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GetErrorKind {
    InvalidName,
    ConsumerCreate,
    NotFound,
    BucketLink,
    Other,
    TimedOut,
}

impl Display for GetErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConsumerCreate => write!(f, "failed creating consumer for fetching object"),
            Self::Other => write!(f, "failed getting object"),
            Self::NotFound => write!(f, "object not found"),
            Self::TimedOut => write!(f, "timed out"),
            Self::InvalidName => write!(f, "invalid object name"),
            Self::BucketLink => write!(f, "object is a link to a bucket"),
        }
    }
}

pub type GetError = Error<GetErrorKind>;

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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DeleteErrorKind {
    TimedOut,
    NotFound,
    Metadata,
    InvalidName,
    Chunks,
    Other,
}

impl Display for DeleteErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out"),
            Self::Metadata => write!(f, "failed rolling up metadata"),
            Self::Chunks => write!(f, "failed purging chunks"),
            Self::Other => write!(f, "delete failed"),
            Self::NotFound => write!(f, "object not found"),
            Self::InvalidName => write!(f, "invalid object name"),
        }
    }
}

pub type DeleteError = Error<DeleteErrorKind>;

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

crate::from_with_timeout!(DeleteError, DeleteErrorKind, PublishError, PublishErrorKind);
crate::from_with_timeout!(DeleteError, DeleteErrorKind, PurgeError, PurgeErrorKind);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PutErrorKind {
    InvalidName,
    ReadChunks,
    PublishChunks,
    PublishMetadata,
    PurgeOldChunks,
    TimedOut,
    Other,
}

impl Display for PutErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PublishChunks => write!(f, "failed publishing object chunks"),
            Self::PublishMetadata => write!(f, "failed publishing metadata"),
            Self::PurgeOldChunks => write!(f, "failed purging old chunks"),
            Self::TimedOut => write!(f, "timed out"),
            Self::Other => write!(f, "error"),
            Self::InvalidName => write!(f, "invalid object name"),
            Self::ReadChunks => write!(f, "error while reading the buffer"),
        }
    }
}

pub type PutError = Error<PutErrorKind>;

pub type AddLinkError = Error<AddLinkErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AddLinkErrorKind {
    EmptyName,
    ObjectRequired,
    Deleted,
    LinkToLink,
    PublishMetadata,
    AlreadyExists,
    Other,
}

impl Display for AddLinkErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AddLinkErrorKind::ObjectRequired => write!(f, "cannot link to empty Object"),
            AddLinkErrorKind::Deleted => write!(f, "cannot link a deleted Object"),
            AddLinkErrorKind::LinkToLink => write!(f, "cannot link to another link"),
            AddLinkErrorKind::EmptyName => write!(f, "link name cannot be empty"),
            AddLinkErrorKind::PublishMetadata => write!(f, "failed publishing link metadata"),
            AddLinkErrorKind::Other => write!(f, "error"),
            AddLinkErrorKind::AlreadyExists => write!(f, "object already exists"),
        }
    }
}

type PublishMetadataError = Error<PublishMetadataErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
enum PublishMetadataErrorKind {
    PublishMetadata,
    Other,
}

impl Display for PublishMetadataErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishMetadataErrorKind::PublishMetadata => write!(f, "failed to publish metadata"),
            PublishMetadataErrorKind::Other => write!(f, "error"),
        }
    }
}

impl From<PublishMetadataError> for AddLinkError {
    fn from(error: PublishMetadataError) -> Self {
        match error.kind {
            PublishMetadataErrorKind::PublishMetadata => {
                AddLinkError::new(AddLinkErrorKind::PublishMetadata)
            }
            PublishMetadataErrorKind::Other => {
                AddLinkError::with_source(AddLinkErrorKind::Other, error)
            }
        }
    }
}
impl From<PublishMetadataError> for PutError {
    fn from(error: PublishMetadataError) -> Self {
        match error.kind {
            PublishMetadataErrorKind::PublishMetadata => {
                PutError::new(PutErrorKind::PublishMetadata)
            }
            PublishMetadataErrorKind::Other => PutError::with_source(PutErrorKind::Other, error),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WatchErrorKind {
    TimedOut,
    ConsumerCreate,
    Other,
}

impl Display for WatchErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConsumerCreate => write!(f, "watch consumer creation failed"),
            Self::Other => write!(f, "watch failed"),
            Self::TimedOut => write!(f, "timed out"),
        }
    }
}

pub type WatchError = Error<WatchErrorKind>;

crate::from_with_timeout!(WatchError, WatchErrorKind, ConsumerError, ConsumerErrorKind);
crate::from_with_timeout!(WatchError, WatchErrorKind, StreamError, StreamErrorKind);

pub type ListError = WatchError;
pub type ListErrorKind = WatchErrorKind;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SealErrorKind {
    TimedOut,
    Other,
    Info,
    Update,
}

impl Display for SealErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out"),
            Self::Other => write!(f, "seal failed"),
            Self::Info => write!(f, "failed getting stream info before sealing bucket"),
            Self::Update => write!(f, "failed sealing the bucket"),
        }
    }
}

pub type SealError = Error<SealErrorKind>;

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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WatcherErrorKind {
    ConsumerError,
    Other,
}

impl Display for WatcherErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConsumerError => write!(f, "watcher consumer error"),
            Self::Other => write!(f, "watcher error"),
        }
    }
}

pub type WatcherError = Error<WatcherErrorKind>;

impl From<OrderedError> for WatcherError {
    fn from(err: OrderedError) -> Self {
        WatcherError::with_source(WatcherErrorKind::ConsumerError, err)
    }
}

pub type ListerError = WatcherError;
pub type ListerErrorKind = WatcherErrorKind;
