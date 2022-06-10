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

use std::{
    io::{self, ErrorKind},
    time::Duration,
};

use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::serde::rfc3339;

use super::{
    consumer::{Consumer, FromConsumer, Info, IntoConsumerConfig},
    response::Response,
    Context,
};

pub struct Stream {
    pub info: StreamInfo,
    pub(crate) context: Context,
}

impl Stream {
    /// Get a message from the stream.
    pub async fn get_message(&self, sequence: u64) -> Result<MessageStatus, Error> {
        let subject = format!("{}.STREAM.MSG.GET.{}", self.prefix, &self.info.config.name);
        let payload = json!({
            "seq": sequence,
        });

        match self.context.request(subject, &payload).await? {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while creating stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(status) => Ok(status),
        }
    }

    pub async fn create_consumer<C: IntoConsumerConfig>(&self, config: C) -> Result<Info, Error> {
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
                    "nats: error while creating stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(info) => Ok(info),
        }
    }

    pub async fn consumer_info<T: AsRef<str>>(&self, name: T) -> Result<Info, Error> {
        let name = name.as_ref();

        let subject = format!("CONSUMER.INFO.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok(info) => Ok(info),
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting consumer info: {}, {}",
                    error.code, error.description
                ),
            ))),
        }
    }

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

    pub async fn get_or_create_consumer<T: FromConsumer + IntoConsumerConfig>(
        &self,
        name: &str,
        config: T,
    ) -> Result<Consumer<T>, Error> {
        let subject = format!("CONSUMER.INFO.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Err { error } if error.code == 404 => self
                .create_consumer(config.into_consumer_config())
                .await
                .map(|info| {
                    Consumer::new(
                        T::try_from_consumer_config(info.config.clone()).unwrap(),
                        info,
                        self.context.clone(),
                    )
                }),
            Response::Err { error } => Err(Box::new(io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting or creating stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok::<Info>(info) => Ok(Consumer::new(
                T::try_from_consumer_config(info.config.clone())?,
                info,
                self.context.clone(),
            )),
        }
    }

    pub async fn delete_consumer(&self, name: &str) -> Result<DeleteStatus, Error> {
        let subject = format!("CONSUMER.DELETE.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok(delete_status) => Ok(delete_status),
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while deleting consumer: {}, {}",
                    error.code, error.description
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
pub struct StreamInfo {
    /// The configuration associated with this stream
    pub config: Config,
    /// The time that this stream was created
    #[serde(with = "rfc3339")]
    pub created: time::OffsetDateTime,
    /// Various metrics associated with this stream
    pub state: StreamState,
}

#[derive(Deserialize)]
pub struct DeleteStatus {
    pub success: bool,
}

/// information about the given stream.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct StreamState {
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
pub struct Message {
    /// Subject of the message.
    #[serde(rename = "subject")]
    pub subject: String,

    /// Sequence of the message.
    #[serde(rename = "seq")]
    pub sequence: u64,

    /// Data of the mssage.
    #[serde(default, rename = "data")]
    pub data: String,

    /// Raw header string, if any.
    #[serde(default, rename = "hdrs")]
    pub headers: Option<String>,

    /// The time the message was published.
    #[serde(rename = "time", with = "rfc3339")]
    pub time: time::OffsetDateTime,
}

/// Helper type to deserialize and flatten message requests
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageStatus {
    pub message: Message,
}

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}
