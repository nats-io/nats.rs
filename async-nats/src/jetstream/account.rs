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

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The maximum amount of Memory storage Stream Messages may consume
    #[serde(deserialize_with = "negative_as_none")]
    pub max_memory: Option<i64>,
    /// The maximum amount of File storage Stream Messages may consume
    #[serde(deserialize_with = "negative_as_none")]
    pub max_storage: Option<i64>,
    /// The maximum number of Streams an account can create
    #[serde(deserialize_with = "negative_as_none")]
    pub max_streams: Option<i64>,
    /// The maximum number of Consumer an account can create
    #[serde(deserialize_with = "negative_as_none")]
    pub max_consumers: Option<i64>,
    /// Indicates if Streams created in this account requires the max_bytes property set
    pub max_bytes_required: bool,
    /// The maximum number of outstanding ACKs any consumer may configure
    pub max_ack_pending: i64,
    /// The maximum size any single memory stream may be
    #[serde(deserialize_with = "negative_as_none")]
    pub memory_max_stream_bytes: Option<i64>,
    /// The maximum size any single storage based stream may be
    pub storage_max_stream_bytes: Option<i64>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Requests {
    /// Total number of requests received for this account.
    pub total: u64,
    /// Total number of requests that resulted in an error response.
    pub errors: u64,
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Default, Deserialize, Clone, PartialEq, Eq)]
pub struct Account {
    /// Memory storage being used for Stream Message storage
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
