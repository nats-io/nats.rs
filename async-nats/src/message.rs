use crate::header::HeaderMap;
use crate::status::StatusCode;
use bytes::Bytes;
use std::num::NonZeroU16;

#[derive(Debug)]
pub struct Message {
    pub subject: String,
    pub reply: Option<String>,
    pub payload: Bytes,
    pub headers: Option<HeaderMap>,
    pub status: Option<StatusCode>,
    pub(crate) description: Option<String>,
}
