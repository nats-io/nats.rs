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
//! Manage operations on a [Stream], create/delete/update [Consumer][crate::jetstream::consumer::Consumer].

use std::{
    io::{self, ErrorKind},
    time::Duration,
};

use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::serde::rfc3339;

use super::{
    consumer::{self, Consumer, FromConsumer, IntoConsumerConfig},
    response::Response,
    Context,
};

/// Handle to operations that can be performed on a `Stream`.
#[derive(Debug)]
pub struct Stream {
    pub info: Info,
    pub(crate) context: Context,
}

impl Stream {
    /// Get a raw message from the stream.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let context = async_nats::jetstream::new(client);
    ///
    /// let stream = context.get_or_create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let publish_ack = context.publish("events".to_string(), "data".into()).await?;
    /// let raw_message = stream.get_raw_message(publish_ack.sequence).await?;
    /// println!("Retreived raw message {:?}", raw_message);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_raw_message(&self, sequence: u64) -> Result<RawMessage, Error> {
        let subject = format!("STREAM.MSG.GET.{}", &self.info.config.name);
        let payload = json!({
            "seq": sequence,
        });

        let response: Response<GetRawMessage> = self.context.request(subject, &payload).await?;
        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting message: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(value) => Ok(value.message),
        }
    }

    /// Delete a message from the stream.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let context = async_nats::jetstream::new(client);
    ///
    /// let stream = context.get_or_create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let publish_ack = context.publish("events".to_string(), "data".into()).await?;
    /// stream.delete_message(publish_ack.sequence).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_message(&self, sequence: u64) -> Result<bool, Error> {
        let subject = format!("STREAM.MSG.DELETE.{}", &self.info.config.name);
        let payload = json!({
            "seq": sequence,
        });

        let response: Response<DeleteStatus> = self.context.request(subject, &payload).await?;

        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while deleting message: {}, {}",
                    error.code, error.status
                ),
            ))),
            Response::Ok(value) => Ok(value.success),
        }
    }

    /// Create a new `Durable` or `Ephemeral` Consumer (if `durable_name` was not provided) and
    /// returns the info from the server about created [Consumer][Consumer]
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
    /// let stream = jetstream.get_stream("events").await?;
    /// let info = stream.create_consumer(consumer::pull::Config {
    ///     durable_name: Some("pull".to_string()),
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_consumer<C: IntoConsumerConfig + FromConsumer>(
        &self,
        config: C,
    ) -> Result<Consumer<C>, Error> {
        let config = config.into_consumer_config();
        let subject = if let Some(ref durable_name) = config.durable_name {
            format!(
                "CONSUMER.DURABLE.CREATE.{}.{}",
                self.info.config.name, durable_name
            )
        } else {
            format!("CONSUMER.CREATE.{}", self.info.config.name)
        };

        match self
            .context
            .request(
                subject,
                &json!({"stream_name": self.info.config.name.clone(), "config": config}),
            )
            .await?
        {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while creating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok::<consumer::Info>(info) => Ok(Consumer::new(
                FromConsumer::try_from_consumer_config(info.clone().config)?,
                info,
                self.context.clone(),
            )),
        }
    }

    /// Retrieve [Info] about [Consumer] from the server.
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
    /// let stream = jetstream.get_stream("events").await?;
    /// let info = stream.consumer_info("pull").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn consumer_info<T: AsRef<str>>(&self, name: T) -> Result<consumer::Info, Error> {
        let name = name.as_ref();

        let subject = format!("CONSUMER.INFO.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok(info) => Ok(info),
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting consumer info: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
        }
    }

    /// Get [Consumer] from the the server. [Consumer] iterators can be used to retrieve
    /// [Messages][crate::jetstream::Message] for a given [Consumer].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// let consumer: consumer::PullConsumer = stream.get_consumer("pull").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_consumer<T: FromConsumer + IntoConsumerConfig>(
        &self,
        name: &str,
    ) -> Result<Consumer<T>, Error> {
        let info = self.consumer_info(name).await?;

        Ok(Consumer::new(
            T::try_from_consumer_config(info.config.clone())?,
            info,
            self.context.clone(),
        ))
    }

    /// Create a [Consumer] with the given configuration if it is not present on the server. Returns a handle to the [Consumer].
    ///
    /// Note: This does not validate if the [Consumer] on the server is compatible with the configuration passed in except Push/Pull compatibility.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// let consumer = stream.get_or_create_consumer("pull", consumer::pull::Config {
    ///     durable_name: Some("pull".to_string()),
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_or_create_consumer<T: FromConsumer + IntoConsumerConfig>(
        &self,
        name: &str,
        config: T,
    ) -> Result<Consumer<T>, Error> {
        let subject = format!("CONSUMER.INFO.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Err { error } if error.status == 404 => self.create_consumer(config).await,
            Response::Err { error } => Err(Box::new(io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting or creating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok::<consumer::Info>(info) => Ok(Consumer::new(
                T::try_from_consumer_config(info.config.clone())?,
                info,
                self.context.clone(),
            )),
        }
    }

    /// Delete a [Consumer] from the server.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// jetstream.get_stream("events").await?
    ///     .delete_consumer("pull").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_consumer(&self, name: &str) -> Result<DeleteStatus, Error> {
        let subject = format!("CONSUMER.DELETE.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok(delete_status) => Ok(delete_status),
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while deleting consumer: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
        }
    }
}

/// `StreamConfig` determines the properties for a stream.
/// There are sensible defaults for most. If no subjects are
/// given the name will be used as the only subject.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Config {
    /// A name for the Stream. Must not have spaces, tabs or period `.` characters
    pub name: String,
    /// How large the Stream may become in total bytes before the configured discard policy kicks in
    pub max_bytes: i64,
    /// How large the Stream may become in total messages before the configured discard policy kicks in
    #[serde(rename = "max_msgs")]
    pub max_messages: i64,
    /// Maximum amount of messages to keep per subject
    #[serde(rename = "max_msgs_per_subject")]
    pub max_messages_per_subject: i64,
    /// When a Stream has reached its configured `max_bytes` or `max_msgs`, this policy kicks in.
    /// `DiscardPolicy::New` refuses new messages or `DiscardPolicy::Old` (default) deletes old messages to make space
    pub discard: DiscardPolicy,
    /// Which NATS subjects to populate this stream with. Supports wildcards. Defaults to just the
    /// configured stream `name`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subjects: Vec<String>,
    /// How message retention is considered, `Limits` (default), `Interest` or `WorkQueue`
    pub retention: RetentionPolicy,
    /// How many Consumers can be defined for a given Stream, -1 for unlimited
    pub max_consumers: i32,
    /// Maximum age of any message in the stream, expressed in nanoseconds
    #[serde(with = "serde_nanos")]
    pub max_age: Duration,
    /// The largest message that will be accepted by the Stream
    #[serde(default, skip_serializing_if = "is_default", rename = "max_msg_size")]
    pub max_message_size: i32,
    /// The type of storage backend, `File` (default) and `Memory`
    pub storage: StorageType,
    /// How many replicas to keep for each message in a clustered JetStream, maximum 5
    pub num_replicas: usize,
    /// Disables acknowledging messages that are received by the Stream
    #[serde(default, skip_serializing_if = "is_default")]
    pub no_ack: bool,
    /// The window within which to track duplicate messages.
    #[serde(default, skip_serializing_if = "is_default")]
    pub duplicate_window: i64,
    /// The owner of the template associated with this stream.
    #[serde(default, skip_serializing_if = "is_default")]
    pub template_owner: String,
    /// Indicates the stream is sealed and cannot be modified in any way
    #[serde(default, skip_serializing_if = "is_default")]
    pub sealed: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    /// A short description of the purpose of this stream.
    pub description: Option<String>,
    #[serde(
        default,
        rename = "allow_rollup_hdrs",
        skip_serializing_if = "is_default"
    )]
    /// Indicates if rollups will be allowed or not.
    pub allow_rollup: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    /// Indicates deletes will be denied or not.
    pub deny_delete: bool,
    /// Indicates if purges will be denied or not.
    #[serde(default, skip_serializing_if = "is_default")]
    pub deny_purge: bool,
}

impl From<&Config> for Config {
    fn from(sc: &Config) -> Config {
        sc.clone()
    }
}

impl From<&str> for Config {
    fn from(s: &str) -> Config {
        Config {
            name: s.to_string(),
            ..Default::default()
        }
    }
}

/// `DiscardPolicy` determines how we proceed when limits of messages or bytes are hit. The default, `Old` will
/// remove older messages. `New` will fail to store the new message.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DiscardPolicy {
    /// will remove older messages when limits are hit.
    #[serde(rename = "old")]
    Old = 0,
    /// will error on a StoreMsg call when limits are hit
    #[serde(rename = "new")]
    New = 1,
}

impl Default for DiscardPolicy {
    fn default() -> DiscardPolicy {
        DiscardPolicy::Old
    }
}

/// `RetentionPolicy` determines how messages in a set are retained.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetentionPolicy {
    /// `Limits` (default) means that messages are retained until any given limit is reached.
    /// This could be one of mesages, bytes, or age.
    #[serde(rename = "limits")]
    Limits = 0,
    /// `Interest` specifies that when all known observables have acknowledged a message it can be removed.
    #[serde(rename = "interest")]
    Interest = 1,
    /// `WorkQueue` specifies that when the first worker or subscriber acknowledges the message it can be removed.
    #[serde(rename = "workqueue")]
    WorkQueue = 2,
}

impl Default for RetentionPolicy {
    fn default() -> RetentionPolicy {
        RetentionPolicy::Limits
    }
}

/// determines how messages are stored for retention.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageType {
    /// Stream data is kept in files. This is the default.
    #[serde(rename = "file")]
    File = 0,
    /// Stream data is kept only in memory.
    #[serde(rename = "memory")]
    Memory = 1,
}

impl Default for StorageType {
    fn default() -> StorageType {
        StorageType::File
    }
}

/// Shows config and current state for this stream.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Info {
    /// The configuration associated with this stream
    pub config: Config,
    /// The time that this stream was created
    #[serde(with = "rfc3339")]
    pub created: time::OffsetDateTime,
    /// Various metrics associated with this stream
    pub state: State,

    ///information about leader and replicas
    #[serde(default)]
    pub cluster: Option<ClusterInfo>,
}

#[derive(Deserialize)]
pub struct DeleteStatus {
    pub success: bool,
}

/// information about the given stream.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct State {
    /// The number of messages contained in this stream
    pub messages: u64,
    /// The number of bytes of all messages contained in this stream
    pub bytes: u64,
    /// The lowest sequence number still present in this stream
    #[serde(rename = "first_seq")]
    pub first_sequence: u64,
    /// The time associated with the oldest message still present in this stream
    #[serde(with = "rfc3339", rename = "first_ts")]
    pub first_timestamp: time::OffsetDateTime,
    /// The last sequence number assigned to a message in this stream
    #[serde(rename = "last_seq")]
    pub last_sequence: u64,
    /// The time that the last message was received by this stream
    #[serde(with = "rfc3339", rename = "last_ts")]
    pub last_timestamp: time::OffsetDateTime,
    /// The number of consumers configured to consume this stream
    pub consumer_count: usize,
}

/// A raw stream message in the representation it is stored.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RawMessage {
    /// Subject of the message.
    #[serde(rename = "subject")]
    pub subject: String,

    /// Sequence of the message.
    #[serde(rename = "seq")]
    pub sequence: u64,

    /// Raw payload of the message as a base64 encoded string.
    #[serde(default, rename = "data")]
    pub payload: String,

    /// Raw header string, if any.
    #[serde(default, rename = "hdrs")]
    pub headers: Option<String>,

    /// The time the message was published.
    #[serde(rename = "time", with = "rfc3339")]
    pub time: time::OffsetDateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GetRawMessage {
    pub message: RawMessage,
}

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}
/// Information about the stream's, consumer's associated `JetStream` cluster
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ClusterInfo {
    /// The cluster name.
    #[serde(default)]
    pub name: Option<String>,
    /// The server name of the RAFT leader.
    #[serde(default)]
    pub leader: Option<String>,
    /// The members of the RAFT cluster.
    #[serde(default)]
    pub replicas: Vec<PeerInfo>,
}

/// The members of the RAFT cluster
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    /// The server name of the peer.
    pub name: String,
    /// Indicates if the server is up to date and synchronised.
    pub current: bool,
    /// Nanoseconds since this peer was last seen.
    #[serde(with = "serde_nanos")]
    pub active: Duration,
    /// Indicates the node is considered offline by the group.
    #[serde(default)]
    pub offline: bool,
    /// How many uncommitted operations this peer is behind the leader.
    pub lag: Option<u64>,
}
