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

use std::time::Duration;

use crate::header::HeaderMap;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::io::{self, ErrorKind};
use time::serde::rfc3339;

/// A UTC time
pub type DateTime = time::OffsetDateTime;

#[derive(Serialize)]
pub(crate) struct StreamMessageGetRequest {
    #[serde(default, skip_serializing_if = "is_default")]
    pub seq: Option<u64>,

    #[serde(default, rename = "last_by_subj", skip_serializing_if = "is_default")]
    pub last_by_subject: Option<String>,
}

/// A raw stream message in the representation it is stored.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RawStreamMessage {
    /// Subject of the message.
    #[serde(rename = "subject")]
    pub subject: String,

    /// Sequence of the message.
    #[serde(rename = "seq")]
    pub sequence: u64,

    /// Data of the message.
    #[serde(default, rename = "data")]
    pub data: String,

    /// Raw header string, if any.
    #[serde(default, rename = "hdrs")]
    pub headers: Option<String>,

    /// The time the message was published.
    #[serde(rename = "time", with = "rfc3339")]
    pub time: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct StreamMessageGetResponse {
    #[serde(rename = "type")]
    pub kind: String,

    #[serde(rename = "message")]
    pub message: RawStreamMessage,
}

/// A message stored in a stream.
#[derive(Debug, Clone)]
pub struct StreamMessage {
    /// Subject of the message.
    pub subject: String,
    /// Sequence of the message
    pub sequence: u64,
    /// HeaderMap that were sent with the message, if any.
    pub headers: Option<HeaderMap>,
    /// Payload of the message.
    pub data: Vec<u8>,
    /// Date and time the message was published.
    pub time: DateTime,
}

impl TryFrom<RawStreamMessage> for StreamMessage {
    type Error = std::io::Error;

    fn try_from(raw_message: RawStreamMessage) -> Result<StreamMessage, Self::Error> {
        let maybe_headers = if let Some(raw_headers) = raw_message.headers {
            let decoded_headers = match base64::decode(raw_headers) {
                Ok(data) => data,
                Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err)),
            };

            let headers = HeaderMap::try_from(decoded_headers.as_slice())?;

            Some(headers)
        } else {
            None
        };

        let decoded_data = match base64::decode(&raw_message.data) {
            Ok(data) => data,
            Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err)),
        };

        Ok(StreamMessage {
            subject: raw_message.subject,
            sequence: raw_message.sequence,
            headers: maybe_headers,
            data: decoded_data,
            time: raw_message.time,
        })
    }
}

#[derive(Serialize)]
pub(crate) struct DeleteRequest {
    pub seq: u64,
}

#[derive(Deserialize)]
pub(crate) struct DeleteResponse {
    pub success: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct CreateConsumerRequest {
    pub stream_name: String,
    pub config: ConsumerConfig,
}

/// Indicates if ownership of a consumer is local or not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConsumerOwnership {
    /// Local consumer handled by subscriptions.
    Yes,
    /// External consumer, lifetime is handled by the user.
    No,
}

/// Configuration for consumers. From a high level, the
/// `durable_name` and `deliver_subject` fields have a particularly
/// strong influence on the consumer's overall behavior.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ConsumerConfig {
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
    /// A short description of the purpose of this consumer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Deliver group to use.
    pub deliver_group: Option<String>,
    /// Allows for a variety of options that determine how this consumer will receive messages
    pub deliver_policy: DeliverPolicy,
    /// Used in combination with `DeliverPolicy::ByStartSeq` to only select messages arriving
    /// after this sequence number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opt_start_seq: Option<u64>,
    /// Used in combination with `DeliverPolicy::ByStartTime` to only select messages arriving
    /// after this time.
    #[serde(default, skip_serializing_if = "is_default", with = "rfc3339::option")]
    pub opt_start_time: Option<DateTime>,
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
    /// What percentage of acknowledgments should be samples for observability, 0-100
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
    /// Maximum value for request expiration
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub max_expires: Duration,
    /// Threshold for ephemeral consumer inactivity
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub inactive_threshold: Duration,
}

pub(crate) enum ConsumerKind {
    Pull,
}

// TODO: validate consumer
impl ConsumerConfig {
    pub(crate) fn validate_for(&self, kind: &ConsumerKind) -> io::Result<()> {
        match kind {
            ConsumerKind::Pull => {
                if self.deliver_subject.is_some() {
                    return Err(io::Error::new(
                        ErrorKind::Other,
                        "pull subscription cannot bind to Push Consumer",
                    ));
                }
                // check ack policies
                if let AckPolicy::None = self.ack_policy {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "pull subscription cannot have Ack Policy set to None",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl From<&ConsumerConfig> for ConsumerConfig {
    fn from(cc: &ConsumerConfig) -> ConsumerConfig {
        cc.clone()
    }
}

impl From<&str> for ConsumerConfig {
    fn from(s: &str) -> ConsumerConfig {
        ConsumerConfig {
            durable_name: Some(s.to_string()),
            ..Default::default()
        }
    }
}

/// `StreamConfig` determines the properties for a stream.
/// There are sensible defaults for most. If no subjects are
/// given the name will be used as the only subject.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct StreamConfig {
    /// A name for the Stream. Must not have spaces, tabs or period `.` characters
    pub name: String,
    /// How large the Stream may become in total bytes before the configured discard policy kicks in
    pub max_bytes: i64,
    /// How large the Stream may become in total messages before the configured discard policy kicks in
    pub max_msgs: i64,
    /// Maximum amount of messages to keep per subject
    pub max_msgs_per_subject: i64,
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
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_msg_size: i32,
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

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}

impl From<&StreamConfig> for StreamConfig {
    fn from(sc: &StreamConfig) -> StreamConfig {
        sc.clone()
    }
}

impl From<&str> for StreamConfig {
    fn from(s: &str) -> StreamConfig {
        StreamConfig {
            name: s.to_string(),
            ..Default::default()
        }
    }
}

/// Shows config and current state for this stream.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StreamInfo {
    /// The configuration associated with this stream
    pub config: StreamConfig,
    /// The time that this stream was created
    #[serde(with = "rfc3339")]
    pub created: DateTime,
    /// Various metrics associated with this stream
    pub state: StreamState,
    /// Information about the stream's cluster
    #[serde(default)]
    pub cluster: ClusterInfo,
}

/// Information about a received message
#[derive(Debug, Clone)]
pub struct JetStreamMessageInfo<'a> {
    /// Optional domain, present in servers post-ADR-15
    pub domain: Option<&'a str>,
    /// Optional account hash, present in servers post-ADR-15
    pub acc_hash: Option<&'a str>,
    /// The stream name
    pub stream: &'a str,
    /// The consumer name
    pub consumer: &'a str,
    /// The stream sequence number associated with this message
    pub stream_seq: u64,
    /// The consumer sequence number associated with this message
    pub consumer_seq: u64,
    /// The number of delivery attempts for this message
    pub delivered: i64,
    /// the number of messages known by the server to be pending to this consumer
    pub pending: u64,
    /// the time that this message was received by the server from its publisher
    pub published: DateTime,
    /// Optional token, present in servers post-ADR-15
    pub token: Option<&'a str>,
}

/// information about the given stream.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct StreamState {
    /// The number of messages contained in this stream
    pub messages: u64,
    /// The number of bytes of all messages contained in this stream
    pub bytes: u64,
    /// The lowest sequence number still present in this stream
    pub first_seq: u64,
    /// The time associated with the oldest message still present in this stream
    #[serde(with = "rfc3339")]
    pub first_ts: DateTime,
    /// The last sequence number assigned to a message in this stream
    pub last_seq: u64,
    /// The time that the last message was received by this stream
    #[serde(with = "rfc3339")]
    pub last_ts: DateTime,
    /// The number of consumers configured to consume this stream
    pub consumer_count: usize,
}

/// `DeliverPolicy` determines how the consumer should select the first message to deliver.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DeliverPolicy {
    /// All causes the consumer to receive the oldest messages still present in the system.
    /// This is the default.
    #[default]
    #[serde(rename = "all")]
    All = 0,
    /// Last will start the consumer with the last sequence received.
    #[serde(rename = "last")]
    Last = 1,
    /// New will only deliver new messages that are received by the `JetStream` server
    /// after the consumer is created.
    #[serde(rename = "new")]
    New = 2,
    /// `ByStartSeq` will look for a defined starting sequence to the consumer's configured `opt_start_seq`
    /// parameter.
    #[serde(rename = "by_start_sequence")]
    ByStartSeq = 3,
    /// `ByStartTime` will select the first message with a timestamp >= to the consumer's
    /// configured `opt_start_time` parameter.
    #[serde(rename = "by_start_time")]
    ByStartTime = 4,
    /// `LastPerSubject` will start the consumer with the last message
    /// for all subjects received.
    #[serde(rename = "last_per_subject")]
    LastPerSubject = 5,
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

/// The payload used to generate a purge request.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct PurgeRequest {
    /// Purge up to but not including sequence.
    #[serde(default, rename = "seq", skip_serializing_if = "is_default")]
    pub sequence: Option<u64>,

    /// Subject to match against messages for the purge command.
    #[serde(default, rename = "filter", skip_serializing_if = "is_default")]
    pub filter: Option<String>,

    /// Number of messages to keep.
    #[serde(default, rename = "filter", skip_serializing_if = "is_default")]
    pub keep: Option<u64>,
}

/// The response generated by trying to purge a stream.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct PurgeResponse {
    /// Whether the purge request was successful.
    pub success: bool,
    /// The number of purged messages in a stream.
    pub purged: u64,
}

/// `RetentionPolicy` determines how messages in a set are retained.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetentionPolicy {
    /// `Limits` (default) means that messages are retained until any given limit is reached.
    /// This could be one of messages, bytes, or age.
    #[default]
    #[serde(rename = "limits")]
    Limits = 0,
    /// `Interest` specifies that when all known observables have acknowledged a message it can be removed.
    #[serde(rename = "interest")]
    Interest = 1,
    /// `WorkQueue` specifies that when the first worker or subscriber acknowledges the message it can be removed.
    #[serde(rename = "workqueue")]
    WorkQueue = 2,
}

/// `DiscardPolicy` determines how we proceed when limits of messages or bytes are hit. The default, `Old` will
/// remove older messages. `New` will fail to store the new message.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DiscardPolicy {
    /// will remove older messages when limits are hit.
    #[default]
    #[serde(rename = "old")]
    Old = 0,
    /// will error on a StoreMsg call when limits are hit
    #[serde(rename = "new")]
    New = 1,
}

/// determines how messages are stored for retention.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageType {
    /// Stream data is kept in files. This is the default.
    #[default]
    #[serde(rename = "file")]
    File = 0,
    /// Stream data is kept only in memory.
    #[serde(rename = "memory")]
    Memory = 1,
}

/// Various limits imposed on a particular account.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct AccountLimits {
    /// Maximum memory for this account (-1 if no limit)
    pub max_memory: i64,
    /// Maximum storage for this account (-1 if no limit)
    pub max_storage: i64,
    /// Maximum streams for this account (-1 if no limit)
    pub max_streams: i64,
    /// Maximum consumers for this account (-1 if no limit)
    pub max_consumers: i64,
}

/// returns current statistics about the account's `JetStream` usage.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AccountStats {
    pub memory: u64,
    pub storage: u64,
    pub streams: usize,
    pub limits: AccountLimits,
}

/// `PublishAck` is an acknowledgment received after successfully publishing a message.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct PublishAck {
    /// Name of stream the message was published to.
    pub stream: String,
    /// Sequence number the message was published in.
    #[serde(rename = "seq")]
    pub sequence: u64,
    /// Domain the message was published to
    // TODO(caspervonb) using String::is_empty as default for String is still unstable.
    // Use `is_default` once that is no longer gated for strings.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    /// True if the published message was determined to be a duplicate, false otherwise.
    #[serde(default, skip_serializing_if = "is_default")]
    pub duplicate: bool,
}

/// The kinds of response used for acknowledging a processed message.
#[derive(Debug, Clone, Copy)]
pub enum AckKind {
    /// Acknowledges a message was completely handled.
    Ack,
    /// Signals that the message will not be processed now
    /// and processing can move onto the next message, NAK'd
    /// message will be retried.
    Nak,
    /// When sent before the AckWait period indicates that
    /// work is ongoing and the period should be extended by
    /// another equal to AckWait.
    Progress,
    /// Acknowledges the message was handled and requests
    /// delivery of the next message to the reply subject.
    /// Only applies to Pull-mode.
    Next,
    /// Instructs the server to stop redelivery of a message
    /// without acknowledging it as successfully processed.
    Term,
}

impl AsRef<[u8]> for AckKind {
    fn as_ref(&self) -> &[u8] {
        use AckKind::*;
        match self {
            Ack => b"+ACK",
            Nak => b"-NAK",
            Progress => b"+WPI",
            Next => b"+NXT",
            Term => b"+TERM",
        }
    }
}

/// Information about a consumer
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ConsumerInfo {
    /// The stream being consumed
    pub stream_name: String,
    /// The consumer's unique name
    pub name: String,
    /// The time the consumer was created
    #[serde(with = "rfc3339")]
    pub created: DateTime,
    /// The consumer's configuration
    pub config: ConsumerConfig,
    /// Statistics for delivered messages
    pub delivered: SequencePair,
    /// Statistics for acknowledged messages
    pub ack_floor: SequencePair,
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
    #[serde(default)]
    pub cluster: ClusterInfo,
    /// Indicates if any client is connected and receiving messages from a push consumer
    #[serde(default)]
    pub push_bound: bool,
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
    /// Indicates if the server is up to date and synchronized.
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

/// Information about a consumer and the stream it is consuming
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct SequencePair {
    /// How far along the consumer has progressed
    pub consumer_seq: u64,
    /// The aggregate for all stream consumers
    pub stream_seq: u64,
}

/// Used for next Pull Request for Pull Consumer
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct BatchOptions {
    /// The number of messages that are being requested to be delivered.
    pub batch: usize,
    /// The optional number of nanoseconds that the server will store this next request for
    /// before forgetting about the pending batch size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<usize>,
    /// This optionally causes the server not to store this pending request at all, but when there are no
    /// messages to deliver will send a nil bytes message with a Status header of 404, this way you
    /// can know when you reached the end of the stream for example. A 409 is returned if the
    /// Consumer has reached MaxAckPending limits.
    #[serde(default, skip_serializing_if = "is_default")]
    pub no_wait: bool,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct StreamNamesRequest {
    #[serde(default, skip_serializing_if = "is_default")]
    pub subject: String,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct StreamNamesResponse {
    #[serde(default, skip_serializing_if = "is_default")]
    pub streams: Option<Vec<String>>,
}

/// Options to configure Pull Subscription
#[derive(Debug, Default, Clone)]
pub struct PullSubscribeOptions {
    pub(crate) stream_name: Option<String>,
    pub(crate) durable_name: Option<String>,
    pub(crate) bind_only: bool,
    pub(crate) consumer_config: Option<ConsumerConfig>,
}

impl PullSubscribeOptions {
    /// creates new options
    pub fn new() -> PullSubscribeOptions {
        Default::default()
    }

    /// Binds subscription explicitly to a stream.
    /// If not specified, stream will be looked up based on the subject provided.
    pub fn bind_stream(mut self, stream_name: String) -> Self {
        self.stream_name = Some(stream_name);
        self.bind_only = true;
        self
    }

    /// when creating Pull Subscription to not existing consumer
    /// Consumer Configuration can be specified
    /// will apply only if `bind_stream` is not called and consumer doesn't exist.
    pub fn consumer_config(mut self, consumer_config: ConsumerConfig) -> Self {
        self.consumer_config = Some(consumer_config);
        self
    }

    /// define consumer name
    pub fn durable_name(mut self, consumer_name: String) -> Self {
        self.durable_name = Some(consumer_name);
        self
    }
}

/// Options for subscription
#[derive(Debug, Default, Clone)]
pub struct SubscribeOptions {
    // For consumer binding:
    pub(crate) bind_only: bool,
    pub(crate) stream_name: Option<String>,
    pub(crate) consumer_name: Option<String>,

    // For consumer configuration:
    pub(crate) ordered: bool,
    pub(crate) ack_policy: Option<AckPolicy>,
    pub(crate) ack_wait: Option<Duration>,
    pub(crate) replay_policy: Option<ReplayPolicy>,
    pub(crate) deliver_policy: Option<DeliverPolicy>,
    pub(crate) deliver_subject: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) durable_name: Option<String>,
    pub(crate) sample_frequency: Option<u8>,
    pub(crate) idle_heartbeat: Option<Duration>,
    pub(crate) max_ack_pending: Option<i64>,
    pub(crate) max_deliver: Option<i64>,
    pub(crate) max_waiting: Option<i64>,
    pub(crate) opt_start_seq: Option<u64>,
    pub(crate) opt_start_time: Option<DateTime>,
    pub(crate) flow_control: Option<bool>,
    pub(crate) rate_limit: Option<u64>,
    pub(crate) headers_only: Option<bool>,
}

impl SubscribeOptions {
    /// Creates a new set of default subscription options
    pub fn new() -> Self {
        Self::default()
    }

    /// Binds to an existing consumer from a stream without attempting to create one.
    pub fn bind(stream_name: String, consumer_name: String) -> Self {
        Self {
            stream_name: Some(stream_name),
            consumer_name: Some(consumer_name),
            bind_only: true,
            ..Default::default()
        }
    }

    /// Creates an ordered fifo (first-in-first-out) subscription.
    pub fn ordered() -> Self {
        Self {
            ordered: true,
            ..Self::default()
        }
    }

    /// Binds the consumer to a stream explicitly based on a name.
    ///
    /// When a stream name is not specified, the subject is used as a way to find the stream name.
    /// This is done by making a request to the server to get list of stream names that have a filter the subject.
    /// To avoid the stream lookup, provide the stream name with this function.
    pub fn bind_stream(stream_name: String) -> Self {
        Self {
            stream_name: Some(stream_name),
            ..Default::default()
        }
    }

    /// Sets the description used for the created consumer.
    pub fn description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Sets the durable name for the created consumer.
    pub fn durable_name(mut self, consumer: String) -> Self {
        self.durable_name = Some(consumer);
        self
    }

    /// Configures the consumer to receive all the messages from a Stream.
    pub fn deliver_all(mut self) -> Self {
        self.deliver_policy = Some(DeliverPolicy::All);
        self
    }

    /// Configures the consumer to receive messages
    /// starting with the latest one.
    pub fn deliver_last(mut self) -> Self {
        self.deliver_policy = Some(DeliverPolicy::Last);
        self
    }

    /// Configures the consumer to receive messages
    /// starting with the latest one for each filtered subject.
    pub fn deliver_last_per_subject(mut self) -> Self {
        self.deliver_policy = Some(DeliverPolicy::LastPerSubject);
        self
    }

    /// Configures the consumer to receive messages
    /// published after the subscription.
    pub fn deliver_new(mut self) -> Self {
        self.deliver_policy = Some(DeliverPolicy::New);
        self
    }

    /// Configures a Consumer to receive
    /// messages from a start sequence.
    pub fn deliver_by_start_sequence(mut self, seq: u64) -> Self {
        self.deliver_policy = Some(DeliverPolicy::ByStartSeq);
        self.opt_start_seq = Some(seq);
        self
    }

    /// Configures the consumer to receive
    /// messages from a start time.
    pub fn deliver_by_start_time(mut self, time: DateTime) -> Self {
        self.deliver_policy = Some(DeliverPolicy::ByStartTime);
        self.opt_start_time = Some(time);

        self
    }

    /// Require no acks for delivered messages.
    pub fn ack_none(mut self) -> Self {
        self.ack_policy = Some(AckPolicy::None);
        self
    }

    /// When acking a sequence number, this implicitly acks all sequences
    /// below this one as well.
    pub fn ack_all(mut self) -> Self {
        self.ack_policy = Some(AckPolicy::All);
        self
    }

    /// Requires ack or nack for all messages.
    pub fn ack_explicit(mut self) -> Self {
        self.ack_policy = Some(AckPolicy::Explicit);
        self
    }

    /// Sets the number of redeliveries for a message.
    pub fn max_deliver(mut self, n: i64) -> Self {
        self.max_deliver = Some(n);
        self
    }

    /// Sets the number of outstanding acks that are allowed before
    /// message delivery is halted.
    pub fn max_ack_pending(mut self, n: i64) -> Self {
        self.max_ack_pending = Some(n);
        self
    }

    /// Replays the messages at the original speed.
    pub fn replay_original(mut self) -> Self {
        self.replay_policy = Some(ReplayPolicy::Original);
        self
    }

    /// Replays the messages as fast as possible.
    pub fn replay_instant(mut self) -> Self {
        self.replay_policy = Some(ReplayPolicy::Instant);
        self
    }

    /// The bits per second rate limit applied to the push consumer.
    pub fn rate_limit(mut self, n: u64) -> Self {
        self.rate_limit = Some(n);
        self
    }
    /// Specifies the consumer deliver subject.
    ///
    /// This option is used only in situations where the consumer does not exist
    /// and a creation request is sent to the server.
    ///
    /// If not provided, an inbox will be selected.
    pub fn deliver_subject(mut self, subject: String) -> Self {
        self.deliver_subject = Some(subject);
        self
    }

    /// Instruct the consumer to only deliver headers and no payloads.
    pub fn headers_only(mut self) -> Self {
        self.headers_only = Some(true);
        self
    }

    /// Enables flow control
    pub fn enable_flow_control(mut self) -> Self {
        self.flow_control = Some(true);
        self
    }

    /// Enables heartbeat messages to be sent.
    #[allow(clippy::cast_possible_truncation)]
    pub fn idle_heartbeat(mut self, interval: Duration) -> Self {
        self.idle_heartbeat = Some(interval);
        self
    }
}

/// Options for publishing
#[derive(Debug, Default, Clone)]
pub struct PublishOptions {
    /// Duration to wait before timing out
    pub timeout: Option<Duration>,
    /// Message id
    pub id: Option<String>,
    /// Expected last message id
    pub expected_last_msg_id: Option<String>,
    /// Expected stream name
    pub expected_stream: Option<String>,
    /// Expected last sequence
    pub expected_last_sequence: Option<u64>,
    /// Expected last subject sequence
    pub expected_last_subject_sequence: Option<u64>,
}

/// contains info about the `JetStream` usage from the current account.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct AccountInfo {
    pub(crate) r#type: String,
    /// How much memory is used
    pub memory: i64,
    /// How much storage is used
    pub storage: i64,
    /// How many streams exist
    pub streams: i64,
    /// How many consumers exist
    pub consumers: i64,
    /// Aggregated API statistics
    pub api: ApiStats,
    /// Limits placed on the account
    pub limits: AccountLimits,
}

/// reports on API calls to `JetStream` for this account.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct ApiStats {
    /// The total number of API requests
    pub total: u64,
    /// The total number of API requests resulting in errors
    pub errors: u64,
}
