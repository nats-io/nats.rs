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
//! Publish `JetStream` messages.
use serde::Deserialize;

/// `PublishAck` is an acknowledgment received after successfully publishing a message.
#[derive(Debug, Default, Deserialize, Clone, PartialEq, Eq)]
pub struct PublishAck {
    /// Name of stream the message was published to.
    pub stream: String,
    /// Sequence number the message was published in.
    #[serde(rename = "seq")]
    pub sequence: u64,
    /// Domain the message was published to
    #[serde(default)]
    pub domain: String,
    /// True if the published message was determined to be a duplicate, false otherwise.
    #[serde(default)]
    pub duplicate: bool,
    /// Used only when published against stream with counters enabled.
    #[serde(default, rename = "val")]
    pub value: Option<String>,
    /// Set on the final ack of an atomic batch publish (ADR-50): id of the
    /// committed batch.
    #[cfg(feature = "server_2_14")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server_2_14")))]
    #[serde(default, rename = "batch", skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    /// Set on the final ack of an atomic batch publish (ADR-50): number of
    /// messages persisted in the committed batch (excludes any EOB-only
    /// commit message).
    #[cfg(feature = "server_2_14")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server_2_14")))]
    #[serde(default, rename = "count", skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u64>,
}
