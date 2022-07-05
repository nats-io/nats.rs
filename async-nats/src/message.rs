use crate::header::HeaderMap;
use crate::status::StatusCode;
use bytes::Bytes;

/// A Core NATS message.
#[derive(Debug)]
pub struct Message {
    /// Subject to which message is published to.
    pub subject: String,
    /// Optional reply subject to which response can be published by [crate::Subscriber].
    /// Used for request-response pattern with [crate::Client::request].
    pub reply: Option<String>,
    /// Payload of the message. Can be any arbitrary data format.
    pub payload: Bytes,
    /// Optional headers. Rust client uses [http::header].
    pub headers: Option<HeaderMap>,
    /// Optional Status of the message. Used mostly for internal handling.
    pub status: Option<StatusCode>,
    /// Optional [status][crate::Message::status] description.
    pub description: Option<String>,
}
