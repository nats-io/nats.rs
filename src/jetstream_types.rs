use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};

use chrono::{DateTime as ChronoDateTime, Utc};

/// A UTC time
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct DateTime(pub ChronoDateTime<Utc>);

impl Default for DateTime {
    fn default() -> DateTime {
        DateTime(UNIX_EPOCH.into())
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

/// Configuration for consumers. From a high level, the
/// `durable_name` and `deliver_subject` fields have a particularly
/// strong influence on the consumer's overall behavior.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ConsumerConfig {
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
    /// Allows for a variety of options that determine how this consumer will receive messages
    pub deliver_policy: DeliverPolicy,
    /// Used in combination with `DeliverPolicy::ByStartSeq` to only select messages arriving
    /// after this sequence number.
    #[serde(default, skip_serializing_if = "is_default")]
    pub opt_start_seq: i64,
    /// Used in combination with `DeliverPolicy::ByStartTime` to only select messages arriving
    /// after this time.
    #[serde(default, skip_serializing_if = "is_default")]
    pub opt_start_time: Option<DateTime>,
    /// How messages should be acknowledged
    pub ack_policy: AckPolicy,
    /// How long to allow messages to remain un-acknowledged before attempting redelivery
    #[serde(default, skip_serializing_if = "is_default")]
    pub ack_wait: i64,
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
    pub rate_limit: i64,
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
    #[serde(default, skip_serializing_if = "is_default")]
    pub subjects: Option<Vec<String>>,
    /// How message retention is considered, `Limits` (default), `Interest` or `WorkQueue`
    pub retention: RetentionPolicy,
    /// How many Consumers can be defined for a given Stream, -1 for unlimited
    pub max_consumers: i32,
    /// Maximum age of any message in the stream, expressed in nanoseconds
    pub max_age: i64,
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
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct StreamInfo {
    /// The configuration associated with this stream
    pub config: StreamConfig,
    /// The time that this stream was created
    pub created: DateTime,
    /// Various metrics associated with this stream
    pub state: StreamState,
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
    /// the number of messages known by the server to be delivered to this consumer
    pub delivered: i64,
    /// the number of messages known by the server to be pending to this consumer
    pub pending: u64,
    /// the time that this message was received by the server from its publisher
    pub published: std::time::SystemTime,
    /// Optional token, present in servers post-ADR-15
    pub token: Option<&'a str>,
}

/// information about the given stream.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct StreamState {
    /// The number of messages contained in this stream
    pub messages: u64,
    /// The number of bytes of all messages contained in this stream
    pub bytes: u64,
    /// The lowest sequence number still present in this stream
    pub first_seq: u64,
    /// The time associated with the oldest message still present in this stream
    pub first_ts: DateTime,
    /// The last sequence number assigned to a message in this stream
    pub last_seq: u64,
    /// The time that the last message was received by this stream
    pub last_ts: DateTime,
    /// The number of consumers configured to consume this stream
    pub consumer_count: usize,
}

/// `DeliverPolicy` determines how the consumer should select the first message to deliver.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DeliverPolicy {
    /// All causes the consumer to receive the oldest messages still present in the system.
    /// This is the default.
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
    /// `ByStartTime` will select the first messsage with a timestamp >= to the consumer's
    /// configured `opt_start_time` parameter.
    #[serde(rename = "by_start_time")]
    ByStartTime = 4,
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

/// The response generated by trying ot purge a stream.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct PurgeResponse {
    /// Whether the purge request was successful.
    pub success: bool,
    /// The number of purged messages in a stream.
    pub purged: u64,
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

/// Various limits imposed on a particular account.
#[derive(
    Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq,
)]
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
#[derive(
    Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq,
)]
pub(crate) struct AccountStats {
    pub memory: u64,
    pub storage: u64,
    pub streams: usize,
    pub limits: AccountLimits,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct PubAck {
    pub stream: String,
    pub seq: u64,
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
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ConsumerInfo {
    /// The stream being consumed
    pub stream_name: String,
    /// The consumer's unique name
    pub name: String,
    /// The time the consumer was created
    pub created: DateTime,
    /// The consumer's configuration
    pub config: ConsumerConfig,
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
}

/// Information about the consumer's associated `JetStream` cluster
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ClusterInfo {
    /// The leader of the cluster
    pub leader: String,
}

/// Information about a consumer and the stream it is consuming
#[derive(
    Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq,
)]
pub struct SequencePair {
    /// How far along the consumer has progressed
    pub consumer_seq: u64,
    /// The aggregate for all stream consumers
    pub stream_seq: u64,
}

/// for getting next messages for pull based consumers.
#[derive(
    Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq,
)]
pub struct NextRequest {
    /// The number of messages that are being requested to be delivered.
    pub batch: usize,
    /// The optional number of nanoseconds that the server will store this next request for
    /// before forgetting about the pending batch size.
    #[serde(default, skip_serializing_if = "is_default")]
    pub expires: usize,
    /// This optionally causes the server not to store this pending request at all, but when there are no
    /// messages to deliver will send a nil bytes message with a Status header of 404, this way you
    /// can know when you reached the end of the stream for example. A 409 is returned if the
    /// Consumer has reached MaxAckPending limits.
    #[serde(default, skip_serializing_if = "is_default")]
    pub no_wait: bool,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct StreamRequest {
    #[serde(default, skip_serializing_if = "is_default")]
    pub subject: String,
}

/// options for subscription
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct SubOpts {
    // For attaching.
    pub stream: String,
    pub consumer: String,
    // For pull based consumers, batch size for pull
    pub pull: usize,
    // For manual ack
    pub mack: bool,
    // For creating or updating.
    pub cfg: ConsumerConfig,
}

/// Options for publishing
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct PubOpts {
    pub ttl: i64,
    pub id: String,
    // Expected last msgId
    pub lid: String,
    // Expected stream name
    pub str: String,
    // Expected last sequence
    pub seq: u64,
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
    /// Limits placed on the accuont
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
