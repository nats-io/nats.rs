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
//! Push and [Pull][PullConsumer] [Consumer] API.

pub mod pull;
pub mod push;
use std::io::ErrorKind;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use time::serde::rfc3339;

use super::response::Response;
use super::Context;
use crate::jetstream::consumer;
use crate::Error;

pub trait IntoConsumerConfig {
    fn into_consumer_config(self) -> Config;
}

#[allow(dead_code)]
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
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let info = consumer.info().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn info(&mut self) -> Result<&consumer::Info, Error> {
        let subject = format!("CONSUMER.INFO.{}.{}", self.info.stream_name, self.info.name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok::<Info>(info) => {
                self.info = info;
                Ok(&self.info)
            }
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting consumer info: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
        }
    }

    /// Returns cached [Info] for the [Consumer].
    /// Cache is either from initial creation/retrival of the [Consumer] or last call to
    /// [Consumer::info].
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
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
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
    fn try_from_consumer_config(config: crate::jetstream::consumer::Config) -> Result<Self, Error>
    where
        Self: Sized;
}

pub type PullConsumer = Consumer<self::pull::Config>;
pub type PushConsumer = Consumer<self::push::Config>;

/// Information about a consumer
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
    pub delivered: SequencePair,
    /// Statistics for acknowleged messages
    pub ack_floor: SequencePair,
    /// The difference between delivered and acknowledged messages
    pub num_ack_pending: usize,
    /// The number of messages re-sent after acknowledgement was not received within the configured
    /// time threshold
    pub num_redelivered: usize,
    /// The number of waiting
    pub num_waiting: usize,
    /// The number of pending
    pub num_pending: u64,
    /// Information about the consumer's cluster
    pub cluster: ClusterInfo,
    /// Indicates if any client is connected and receiving messages from a push consumer
    #[serde(default)]
    pub push_bound: bool,
}

/// Information about the consumer's associated `JetStream` cluster
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ClusterInfo {
    /// The leader of the cluster
    pub leader: String,
}

/// Information about a consumer and the stream it is consuming
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct SequencePair {
    /// How far along the consumer has progressed
    #[serde(rename = "consumer_seq")]
    pub consumer_sequence: u64,
    /// The aggregate for all stream consumers
    #[serde(rename = "stream_seq")]
    pub stream_sequence: u64,
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
    /// the consumer is interested in. Acknowledgement policies such as
    /// `AckPolicy::None` and `AckPolicy::All` may be enabled for such
    /// push-based consumers, which reduce the amount of effort spent
    /// tracking delivery. Combining `AckPolicy::All` with
    /// `Consumer::process_batch` enables particularly nice throughput
    /// optimizations.
    ///
    /// Setting `deliver_subject` to `None` will cause this consumer to
    /// be "pull-based", and will require explicit acknowledgement of
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
    /// A short description of the purpose of this consumer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Deliver group to use.
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
    /// Whether messages are sent as quickly as possible or at the rate of receipt
    pub replay_policy: ReplayPolicy,
    /// The rate of message delivery in bits per second
    #[serde(default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    /// What percentage of acknowledgements should be samples for observability, 0-100
    #[serde(default, skip_serializing_if = "is_default")]
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
    /// Maximum value for request exiration
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub max_expires: Duration,
    /// Threshold for ephemeral consumer intactivity
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub inactive_threshold: Duration,
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
    fn try_from_consumer_config(config: Config) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Ok(config)
    }
}

/// `DeliverPolicy` determines how the consumer should select the first message to deliver.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[serde(tag = "deliver_policy")]
pub enum DeliverPolicy {
    /// All causes the consumer to receive the oldest messages still present in the system.
    /// This is the default.
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
    /// `ByStartTime` will select the first messsage with a timestamp >= to the consumer's
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

impl Default for DeliverPolicy {
    fn default() -> DeliverPolicy {
        DeliverPolicy::All
    }
}

/// Determines whether messages will be acknowledged individually,
/// in batches, or never.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AckPolicy {
    /// All messages will be individually acknowledged. This is the default.
    #[serde(rename = "explicit")]
    Explicit = 2,
    /// No messages are acknowledged.
    #[serde(rename = "none")]
    None = 0,
    /// Acknowledges all messages with lower sequence numbers when a later
    /// message is acknowledged. Useful for "batching" acknowledgement.
    #[serde(rename = "all")]
    All = 1,
}

impl Default for AckPolicy {
    fn default() -> AckPolicy {
        AckPolicy::Explicit
    }
}

/// `ReplayPolicy` controls whether messages are sent to a consumer
/// as quickly as possible or at the rate that they were originally received at.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplayPolicy {
    /// Sends all messages in a stream to the consumer as quickly as possible. This is the default.
    #[serde(rename = "instant")]
    Instant = 0,
    /// Sends messages to a consumer in a rate-limited fashion based on the rate of receipt. This
    /// is useful for replaying traffic in a testing or staging environment based on production
    /// traffic patterns.
    #[serde(rename = "original")]
    Original = 1,
}

impl Default for ReplayPolicy {
    fn default() -> ReplayPolicy {
        ReplayPolicy::Instant
    }
}
fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}
