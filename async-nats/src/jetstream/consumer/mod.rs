// Copyright 2020-2023 The NATS Authors
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
//! Push and Pull [Consumer] API.

pub mod pull;
pub mod push;
#[cfg(feature = "server_2_10")]
use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use time::serde::rfc3339;

use super::context::RequestError;
use super::stream::ClusterInfo;
use super::Context;
use crate::error::Error;
use crate::jetstream::consumer;

pub trait IntoConsumerConfig {
    fn into_consumer_config(self) -> Config;
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Consumer<T: IntoConsumerConfig> {
    pub(crate) context: Context,
    pub(crate) config: T,
    pub(crate) info: Info,
}

impl<T: IntoConsumerConfig> Consumer<T> {
    pub fn new(config: T, info: consumer::Info, context: Context) -> Self {
        Self {
            config,
            info,
            context,
        }
    }
}
impl<T: IntoConsumerConfig> Consumer<T> {
    /// Retrieves `info` about [Consumer] from the server, updates the cached `info` inside
    /// [Consumer] and returns it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let mut consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let info = consumer.info().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn info(&mut self) -> Result<&consumer::Info, RequestError> {
        let subject = format!("CONSUMER.INFO.{}.{}", self.info.stream_name, self.info.name);

        let info = self.context.request(subject, &json!({})).await?;
        self.info = info;
        Ok(&self.info)
    }

    async fn fetch_info(&self) -> Result<consumer::Info, RequestError> {
        let subject = format!("CONSUMER.INFO.{}.{}", self.info.stream_name, self.info.name);
        self.context.request(subject, &json!({})).await
    }

    /// Returns cached [Info] for the [Consumer].
    /// Cache is either from initial creation/retrieval of the [Consumer] or last call to
    /// [Info].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let info = consumer.cached_info();
    /// # Ok(())
    /// # }
    /// ```
    pub fn cached_info(&self) -> &consumer::Info {
        &self.info
    }
}

/// Trait used to convert generic [Stream Config][crate::jetstream::consumer::Config] into either
/// [Pull][crate::jetstream::consumer::pull::Config] or
/// [Push][crate::jetstream::consumer::push::Config] config. It validates if given config is
/// a valid target one.
pub trait FromConsumer {
    fn try_from_consumer_config(
        config: crate::jetstream::consumer::Config,
    ) -> Result<Self, crate::Error>
    where
        Self: Sized;
}

pub type PullConsumer = Consumer<self::pull::Config>;
pub type PushConsumer = Consumer<self::push::Config>;
pub type OrderedPullConsumer = Consumer<self::pull::OrderedConfig>;
pub type OrderedPushConsumer = Consumer<self::push::OrderedConfig>;

/// Information about a consumer
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct Info {
    /// The stream being consumed
    pub stream_name: String,
    /// The consumer's unique name
    pub name: String,
    /// The time the consumer was created
    #[serde(with = "rfc3339")]
    pub created: time::OffsetDateTime,
    /// The consumer's configuration
    pub config: Config,
    /// Statistics for delivered messages
    pub delivered: SequenceInfo,
    /// Statistics for acknowledged messages
    pub ack_floor: SequenceInfo,
    /// The difference between delivered and acknowledged messages
    pub num_ack_pending: usize,
    /// The number of messages re-sent after acknowledgment was not received within the configured
    /// time threshold
    pub num_redelivered: usize,
    /// The number of waiting
    pub num_waiting: usize,
    /// The number of pending
    pub num_pending: u64,
    /// Information about the consumer's cluster
    #[serde(skip_serializing_if = "is_default")]
    pub cluster: Option<ClusterInfo>,
    /// Indicates if any client is connected and receiving messages from a push consumer
    #[serde(default, skip_serializing_if = "is_default")]
    pub push_bound: bool,
}

/// Information about a consumer and the stream it is consuming
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct SequenceInfo {
    /// How far along the consumer has progressed
    #[serde(rename = "consumer_seq")]
    pub consumer_sequence: u64,
    /// The aggregate for all stream consumers
    #[serde(rename = "stream_seq")]
    pub stream_sequence: u64,
    // Last activity for the sequence
    #[serde(
        default,
        with = "rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_active: Option<time::OffsetDateTime>,
}

/// Configuration for consumers. From a high level, the
/// `durable_name` and `deliver_subject` fields have a particularly
/// strong influence on the consumer's overall behavior.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Config {
    /// Setting `deliver_subject` to `Some(...)` will cause this consumer
    /// to be "push-based". This is analogous in some ways to a normal
    /// NATS subscription (rather than a queue subscriber) in that the
    /// consumer will receive all messages published to the stream that
    /// the consumer is interested in. Acknowledgment policies such as
    /// `AckPolicy::None` and `AckPolicy::All` may be enabled for such
    /// push-based consumers, which reduce the amount of effort spent
    /// tracking delivery. Combining `AckPolicy::All` with
    /// `Consumer::process_batch` enables particularly nice throughput
    /// optimizations.
    ///
    /// Setting `deliver_subject` to `None` will cause this consumer to
    /// be "pull-based", and will require explicit acknowledgment of
    /// each message. This is analogous in some ways to a normal NATS
    /// queue subscriber, where a message will be delivered to a single
    /// subscriber. Pull-based consumers are intended to be used for
    /// workloads where it is desirable to have a single process receive
    /// a message. The only valid `ack_policy` for pull-based consumers
    /// is the default of `AckPolicy::Explicit`, which acknowledges each
    /// processed message individually. Pull-based consumers may be a
    /// good choice for work queue-like workloads where you want messages
    /// to be handled by a single consumer process. Note that it is
    /// possible to deliver a message to multiple consumers if the
    /// consumer crashes or is slow to acknowledge the delivered message.
    /// This is a fundamental behavior present in all distributed systems
    /// that attempt redelivery when a consumer fails to acknowledge a message.
    /// This is known as "at least once" message processing. To achieve
    /// "exactly once" semantics, it is necessary to implement idempotent
    /// semantics in any system that is written to as a result of processing
    /// a message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliver_subject: Option<String>,

    /// Setting `durable_name` to `Some(...)` will cause this consumer
    /// to be "durable". This may be a good choice for workloads that
    /// benefit from the `JetStream` server or cluster remembering the
    /// progress of consumers for fault tolerance purposes. If a consumer
    /// crashes, the `JetStream` server or cluster will remember which
    /// messages the consumer acknowledged. When the consumer recovers,
    /// this information will allow the consumer to resume processing
    /// where it left off. If you're unsure, set this to `Some(...)`.
    ///
    /// Setting `durable_name` to `None` will cause this consumer to
    /// be "ephemeral". This may be a good choice for workloads where
    /// you don't need the `JetStream` server to remember the consumer's
    /// progress in the case of a crash, such as certain "high churn"
    /// workloads or workloads where a crashed instance is not required
    /// to recover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_name: Option<String>,
    /// A name of the consumer. Can be specified for both durable and ephemeral
    /// consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// A short description of the purpose of this consumer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Deliver group to use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliver_group: Option<String>,
    /// Allows for a variety of options that determine how this consumer will receive messages
    #[serde(flatten)]
    pub deliver_policy: DeliverPolicy,
    /// How messages should be acknowledged
    pub ack_policy: AckPolicy,
    /// How long to allow messages to remain un-acknowledged before attempting redelivery
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub ack_wait: Duration,
    /// Maximum number of times a specific message will be delivered. Use this to avoid poison pill messages that repeatedly crash your consumer processes forever.
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_deliver: i64,
    /// When consuming from a Stream with many subjects, or wildcards, this selects only specific incoming subjects. Supports wildcards.
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subject: String,
    #[cfg(feature = "server_2_10")]
    /// Fulfills the same role as [Config::filter_subject], but allows filtering by many subjects.
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subjects: Vec<String>,
    /// Whether messages are sent as quickly as possible or at the rate of receipt
    pub replay_policy: ReplayPolicy,
    /// The rate of message delivery in bits per second
    #[serde(default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    /// What percentage of acknowledgments should be samples for observability, 0-100
    #[serde(
        rename = "sample_freq",
        with = "sample_freq_deser",
        default,
        skip_serializing_if = "is_default"
    )]
    pub sample_frequency: u8,
    /// The maximum number of waiting consumers.
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_waiting: i64,
    /// The maximum number of unacknowledged messages that may be
    /// in-flight before pausing sending additional messages to
    /// this consumer.
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_ack_pending: i64,
    /// Only deliver headers without payloads.
    #[serde(default, skip_serializing_if = "is_default")]
    pub headers_only: bool,
    /// Enable flow control messages
    #[serde(default, skip_serializing_if = "is_default")]
    pub flow_control: bool,
    /// Enable idle heartbeat messages
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub idle_heartbeat: Duration,
    /// Maximum size of a request batch
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_batch: i64,
    /// Maximum size of a request max_bytes
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_bytes: i64,
    /// Maximum value for request expiration
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub max_expires: Duration,
    /// Threshold for ephemeral consumer inactivity
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub inactive_threshold: Duration,
    /// Number of consumer replicas
    #[serde(default, skip_serializing_if = "is_default")]
    pub num_replicas: usize,
    /// Force consumer to use memory storage.
    #[serde(default, skip_serializing_if = "is_default", rename = "mem_storage")]
    pub memory_storage: bool,

    #[cfg(feature = "server_2_10")]
    /// Additional consumer metadata.
    #[serde(default, skip_serializing_if = "is_default")]
    pub metadata: HashMap<String, String>,
    /// Custom backoff for missed acknowledgments.
    #[serde(default, skip_serializing_if = "is_default", with = "serde_nanos")]
    pub backoff: Vec<Duration>,
}

impl From<&Config> for Config {
    fn from(cc: &Config) -> Config {
        cc.clone()
    }
}

impl From<&str> for Config {
    fn from(s: &str) -> Config {
        Config {
            durable_name: Some(s.to_string()),
            ..Default::default()
        }
    }
}

impl IntoConsumerConfig for Config {
    fn into_consumer_config(self) -> Config {
        self
    }
}
impl IntoConsumerConfig for &Config {
    fn into_consumer_config(self) -> Config {
        self.clone()
    }
}

impl FromConsumer for Config {
    fn try_from_consumer_config(config: Config) -> Result<Self, crate::Error>
    where
        Self: Sized,
    {
        Ok(config)
    }
}

/// `DeliverPolicy` determines how the consumer should select the first message to deliver.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[serde(tag = "deliver_policy")]
pub enum DeliverPolicy {
    /// All causes the consumer to receive the oldest messages still present in the system.
    /// This is the default.
    #[default]
    #[serde(rename = "all")]
    All,
    /// Last will start the consumer with the last sequence received.
    #[serde(rename = "last")]
    Last,
    /// New will only deliver new messages that are received by the `JetStream` server
    /// after the consumer is created.
    #[serde(rename = "new")]
    New,
    /// `ByStartSeq` will look for a defined starting sequence to the consumer's configured `opt_start_seq`
    /// parameter.
    #[serde(rename = "by_start_sequence")]
    ByStartSequence {
        #[serde(rename = "opt_start_seq")]
        start_sequence: u64,
    },
    /// `ByStartTime` will select the first message with a timestamp >= to the consumer's
    /// configured `opt_start_time` parameter.
    #[serde(rename = "by_start_time")]
    ByStartTime {
        #[serde(rename = "opt_start_time", with = "rfc3339")]
        start_time: time::OffsetDateTime,
    },
    /// `LastPerSubject` will start the consumer with the last message
    /// for all subjects received.
    #[serde(rename = "last_per_subject")]
    LastPerSubject,
}

/// Determines whether messages will be acknowledged individually,
/// in batches, or never.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AckPolicy {
    /// All messages will be individually acknowledged. This is the default.
    #[default]
    #[serde(rename = "explicit")]
    Explicit = 2,
    /// No messages are acknowledged.
    #[serde(rename = "none")]
    None = 0,
    /// Acknowledges all messages with lower sequence numbers when a later
    /// message is acknowledged. Useful for "batching" acknowledgment.
    #[serde(rename = "all")]
    All = 1,
}

/// `ReplayPolicy` controls whether messages are sent to a consumer
/// as quickly as possible or at the rate that they were originally received at.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplayPolicy {
    /// Sends all messages in a stream to the consumer as quickly as possible. This is the default.
    #[default]
    #[serde(rename = "instant")]
    Instant = 0,
    /// Sends messages to a consumer in a rate-limited fashion based on the rate of receipt. This
    /// is useful for replaying traffic in a testing or staging environment based on production
    /// traffic patterns.
    #[serde(rename = "original")]
    Original = 1,
}

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}

pub(crate) mod sample_freq_deser {
    pub(crate) fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;

        let mut spliterator = s.split('%');
        match (spliterator.next(), spliterator.next()) {
            // No percentage occurred, parse as number
            (Some(number), None) => T::from_str(number).map_err(serde::de::Error::custom),
            // A percentage sign occurred right at the end
            (Some(number), Some("")) => T::from_str(number).map_err(serde::de::Error::custom),
            _ => Err(serde::de::Error::custom(format!(
                "Malformed sample frequency: {s}"
            ))),
        }
    }

    pub(crate) fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: std::fmt::Display,
        S: serde::Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StreamErrorKind {
    TimedOut,
    Other,
}

impl std::fmt::Display for StreamErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out"),
            Self::Other => write!(f, "failed"),
        }
    }
}

pub type StreamError = Error<StreamErrorKind>;
