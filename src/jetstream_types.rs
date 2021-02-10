#![allow(missing_docs)]

use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};

use chrono::{DateTime as ChronoDateTime, Utc};

/// A UTC time
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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
pub struct CreateConsumerRequest {
    pub stream_name: String,
    pub config: ConsumerConfig,
}

/// Configuration for consumers
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct ConsumerConfig {
    pub durable_name: Option<String>,
    pub deliver_subject: Option<String>,
    pub deliver_policy: DeliverPolicy,
    pub opt_start_seq: Option<i64>,
    pub opt_start_time: Option<DateTime>,
    pub ack_policy: AckPolicy,
    pub ack_wait: Option<isize>,
    pub max_deliver: Option<i64>,
    pub filter_subject: Option<String>,
    pub replay_policy: ReplayPolicy,
    pub rate_limit: Option<i64>,
    pub sample_frequency: Option<String>,
    pub max_waiting: Option<i64>,
    pub max_ack_pending: Option<i64>,
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
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct StreamConfig {
    /// A name for the Stream. Must not have spaces, tabs or period `.` characters
    pub name: String,
    /// How large the Stream may become in total bytes before the configured discard policy kicks in
    pub max_bytes: i64,
    /// How large the Stream may become in total messages before the configured discard policy kicks in
    pub max_msgs: i64,
    /// When a Stream has reached its configured `max_bytes` or `max_msgs`, this policy kicks in.
    /// `DiscardPolicy::New` refuses new messages or `DiscardPolicy::Old` (default) deletes old messages to make space
    pub discard: DiscardPolicy,
    /// Which NATS subjects to populate this stream with. Supports wildcards. Defaults to just the
    /// configured stream `name`.
    pub subjects: Option<Vec<String>>,
    /// How message retention is considered, `Limits` (default), `Interest` or `WorkQueue`
    pub retention: RetentionPolicy,
    // How many Consumers can be defined for a given Stream, -1 for unlimited
    pub max_consumers: isize,
    /// Maximum age of any message in the stream, expressed in microseconds
    pub max_age: isize,
    /// The largest message that will be accepted by the Stream
    pub max_msg_size: Option<i32>,
    /// The type of storage backend, `File` (default) and `Memory`
    pub storage: StorageType,
    /// How many replicas to keep for each message in a clustered JetStream, maximum 5
    pub num_replicas: usize,
    /// Disables acknowledging messages that are received by the Stream
    pub no_ack: Option<bool>,
    /// The window within which to track duplicate messages.
    pub duplicate_window: Option<isize>,
    pub template_owner: Option<String>,
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
    pub config: StreamConfig,
    pub created: DateTime,
    pub state: StreamState,
}

/// information about the given stream.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct StreamState {
    pub messages: u64,
    pub bytes: u64,
    pub first_seq: u64,
    pub first_ts: String,
    pub last_seq: u64,
    pub last_ts: DateTime,
    pub consumer_count: usize,
}

// DeliverPolicy determines how the consumer should select the first message to deliver.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum DeliverPolicy {
    // All will be the default so can be omitted from the request.
    #[serde(rename = "all")]
    All = 0,
    // Last will start the consumer with the last sequence received.
    #[serde(rename = "last")]
    Last = 1,
    // New will only deliver new messages that are sent
    // after the consumer is created.
    #[serde(rename = "new")]
    New = 2,
    // ByStartSequence will look for a defined starting sequence to start.
    #[serde(rename = "by_start_sequence")]
    ByStartSequence = 3,
    // StartTime will select the first messsage with a timestamp >= to StartTime.
    #[serde(rename = "by_start_time")]
    ByStartTime = 4,
}

impl Default for DeliverPolicy {
    fn default() -> DeliverPolicy {
        DeliverPolicy::All
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum AckPolicy {
    #[serde(rename = "none")]
    None = 0,
    #[serde(rename = "all")]
    All = 1,
    #[serde(rename = "explicit")]
    Explicit = 2,
    // For setting
    NotSet = 99,
}

impl Default for AckPolicy {
    fn default() -> AckPolicy {
        AckPolicy::Explicit
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum ReplayPolicy {
    #[serde(rename = "instant")]
    Instant = 0,
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
    pub success: bool,
    pub purged: u64,
}

/// `RetentionPolicy` determines how messages in a set are retained.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum RetentionPolicy {
    // Limits (default) means that messages are retained until any given limit is reached.
    // This could be one of MaxMsgs, MaxBytes, or MaxAge.
    #[serde(rename = "limits")]
    Limits = 0,
    // Interest specifies that when all known observables have acknowledged a message it can be removed.
    #[serde(rename = "interest")]
    Interest = 1,
    // WorkQueue specifies that when the first worker or subscriber acknowledges the message it can be removed.
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
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum StorageType {
    // File specifies on disk storage. It's the default.
    #[serde(rename = "file")]
    File = 0,
    // MemoryStorage specifies in memory only.
    #[serde(rename = "memory")]
    Memory = 1,
}

impl Default for StorageType {
    fn default() -> StorageType {
        StorageType::File
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct AccountLimits {
    pub max_memory: i64,
    pub max_storage: i64,
    pub max_streams: i64,
    pub max_consumers: i64,
}

/// returns current statistics about the account's `JetStream` usage.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct AccountStats {
    pub memory: u64,
    pub storage: u64,
    pub streams: usize,
    pub limits: AccountLimits,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct PubAck {
    pub stream: String,
    pub seq: u64,
    pub duplicate: Option<bool>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct ConsumerInfo {
    pub stream_name: String,
    pub name: String,
    pub created: DateTime,
    pub config: ConsumerConfig,
    pub delivered: SequencePair,
    pub ack_floor: SequencePair,
    pub num_ack_pending: usize,
    pub num_redelivered: usize,
    pub num_waiting: usize,
    pub num_pending: u64,
    pub cluster: ClusterInfo,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct ClusterInfo {
    pub leader: String,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct SequencePair {
    pub consumer_seq: u64,
    pub stream_seq: u64,
}

/// for getting next messages for pull based consumers.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct NextRequest {
    pub expires: DateTime,
    pub batch: Option<usize>,
    pub no_wait: Option<bool>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct StreamRequest {
    pub subject: Option<String>,
}

/// options for subscription
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct SubOpts {
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
pub struct PubOpts {
    pub ttl: isize,
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
    pub r#type: String,
    pub memory: i64,
    pub storage: i64,
    pub streams: i64,
    pub consumers: i64,
    pub api: ApiStats,
    pub limits: AccountLimits,
}

/// reports on API calls to `JetStream` for this account.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct ApiStats {
    pub total: u64,
    pub errors: u64,
}
