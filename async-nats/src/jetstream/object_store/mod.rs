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

use std::{cmp, task::Poll, time::Duration};

use futures::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};

use super::{consumer::push::Ordered, stream::StorageType};
use time::serde::rfc3339;

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

pub(crate) fn sanitize_object_name(object_name: &str) -> String {
    object_name.replace('.', "_").replace(' ', "_")
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
pub struct ObjectStore {
    pub(crate) name: String,
    pub(crate) context: crate::jetstream::Context,
}


impl ObjectStore {

}

/// Represents an object stored in a bucket.
pub struct Object<'a> {
    info: ObjectInfo,
    remaining_bytes: Vec<u8>,
    has_pending_messages: bool,
    subscription: crate::jetstream::consumer::push::Ordered<'a>,
}

impl<'a> Object<'a> {
    pub(crate) fn new(subscription: Ordered<'a>, info: ObjectInfo) -> Self {
        Object {
            subscription,
            info,
            remaining_bytes: Vec::new(),
            has_pending_messages: true,
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
            buf.put_slice(&self.remaining_bytes[len..]);
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
                        self.remaining_bytes = self.remaining_bytes[len..].to_vec();

                        let info = message.info().map_err(|err| {
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("error from JetStream subscription: {}", err),
                            )
                        })?;
                        if info.pending == 0 {
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
