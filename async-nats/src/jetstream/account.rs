use serde::{de::Deserializer, Deserialize, Serialize};
use std::collections::HashMap;

fn negative_as_none<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let n = i64::deserialize(deserializer)?;
    if n.is_negative() {
        Ok(None)
    } else {
        Ok(Some(n))
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum memory for this account (-1 if no limit)
    #[serde(deserialize_with = "negative_as_none")]
    pub max_memory: Option<i64>,
    /// Maximum storage for this account (-1 if no limit)
    #[serde(deserialize_with = "negative_as_none")]
    pub max_storage: Option<i64>,
    /// Maximum streams for this account (-1 if no limit)
    #[serde(deserialize_with = "negative_as_none")]
    pub max_streams: Option<i64>,
    /// Maximum consumers for this account (-1 if no limit)
    #[serde(deserialize_with = "negative_as_none")]
    pub max_consumers: Option<i64>,
    /// Indicates if Streams created in this account requires the max_bytes property set
    pub max_bytes_required: bool,
    /// The maximum number of outstanding ACKs any consumer may configure
    pub max_ack_pending: i64,
    /// The maximum size any single memory stream may be
    pub memory_max_stream_bytes: i64,
    /// The maximum size any single storage based stream may be
    pub storage_max_stream_bytes: i64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Requests {
    /// Total number of requests received for this account.
    pub total: u64,
    /// Total number of requests that resulted in an error response.
    pub errors: u64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Tier {
    /// Memory Storage being used for Stream Message storage
    pub memory: u64,
    /// File Storage being used for Stream Message storage
    pub storage: u64,
    /// Number of active Streams
    pub streams: usize,
    /// Number of active Consumers
    pub consumers: usize,
    /// Limits imposed on this tier.
    pub limits: Limits,
    /// Number of requests received.
    #[serde(rename = "api")]
    pub requests: Requests,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Account {
    /// Memory stoage being used for Stream Message storage
    pub memory: u64,
    /// File Storage being used for Stream Message storage
    pub storage: u64,
    /// Number of active Streams
    pub streams: usize,
    /// Number of active Consumers
    pub consumers: usize,
    /// The JetStream domain this account is in
    pub domain: Option<String>,
    /// Limits imposed on this account.
    pub limits: Limits,
    /// Number of requests received.
    #[serde(rename = "api")]
    pub requests: Requests,
    /// Tiers associated with this account.
    #[serde(default)]
    pub tiers: HashMap<String, Tier>,
}
