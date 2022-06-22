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

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{jetstream, Error};

use super::{AckPolicy, Consumer, DeliverPolicy, FromConsumer, IntoConsumerConfig, ReplayPolicy};

impl Consumer<Config> {
    pub fn push(&self) {
        println!("push");
    }
}
/// Configuration for consumers. From a high level, the
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
}

impl FromConsumer for Config {
    fn try_from_consumer_config(config: super::Config) -> Result<Self, Error> {
        if config.deliver_subject.is_none() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "push consumer must have delivery subject",
            )));
        }

        Ok(Config {
            deliver_subject: config.deliver_subject,
            durable_name: config.durable_name,
            description: config.description,
            deliver_group: config.deliver_group,
            deliver_policy: config.deliver_policy,
            ack_policy: config.ack_policy,
            ack_wait: config.ack_wait,
            max_deliver: config.max_deliver,
            filter_subject: config.filter_subject,
            replay_policy: config.replay_policy,
            rate_limit: config.rate_limit,
            sample_frequency: config.sample_frequency,
            max_waiting: config.max_waiting,
            max_ack_pending: config.max_ack_pending,
            headers_only: config.headers_only,
            flow_control: config.flow_control,
            idle_heartbeat: config.idle_heartbeat,
        })
    }
}

impl IntoConsumerConfig for Config {
    fn into_consumer_config(self) -> jetstream::consumer::Config {
        jetstream::consumer::Config {
            deliver_subject: self.deliver_subject,
            durable_name: self.durable_name,
            description: self.description,
            deliver_group: self.deliver_group,
            deliver_policy: self.deliver_policy,
            ack_policy: self.ack_policy,
            ack_wait: self.ack_wait,
            max_deliver: self.max_deliver,
            filter_subject: self.filter_subject,
            replay_policy: self.replay_policy,
            rate_limit: self.rate_limit,
            sample_frequency: self.sample_frequency,
            max_waiting: self.max_waiting,
            max_ack_pending: self.max_ack_pending,
            headers_only: self.headers_only,
            flow_control: self.flow_control,
            idle_heartbeat: self.idle_heartbeat,
            max_batch: 0,
            max_expires: Duration::default(),
            inactive_threshold: Duration::default(),
        }
    }
}
impl IntoConsumerConfig for &Config {
    fn into_consumer_config(self) -> jetstream::consumer::Config {
        self.clone().into_consumer_config()
    }
}
fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}
