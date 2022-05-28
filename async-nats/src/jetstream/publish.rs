use serde::{Deserialize, Serialize};
use std::ops::Not;

/// `PublishAck` is an acknowledgement received after successfully publishing a message.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct PublishAck {
    /// Name of stream the message was published to.
    pub stream: String,
    /// Sequence number the message was published in.
    #[serde(rename = "seq")]
    pub sequence: u64,
    /// Domain the message was published to
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    /// True if the published message was determined to be a duplicate, false otherwise.
    #[serde(default, skip_serializing_if = "Not::not")]
    pub duplicate: bool,
}
