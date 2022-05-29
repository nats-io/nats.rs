use crate::header::HeaderMap;
use bytes::Bytes;
use std::num::NonZeroU16;

#[derive(Debug)]
pub struct Message {
    pub subject: String,
    pub reply: Option<String>,
    pub payload: Bytes,
    pub headers: Option<HeaderMap>,
    pub(crate) status: Option<NonZeroU16>,
    pub(crate) description: Option<String>,
}

impl Message {
    pub(crate) fn is_no_responders(&self) -> bool {
        if !self.payload.is_empty() {
            return false;
        }
        if self.status == NonZeroU16::new(NO_RESPONDERS) {
            return true;
        }
        false
    }
}

const NO_RESPONDERS: u16 = 503;
