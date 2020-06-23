use std::fmt;

use crate::asynk::client::Client;

/// A message received on a subject.
pub struct AsyncMessage {
    /// The subject this message came from.
    pub subject: String,

    /// Optional reply subject that may be used for sending a response to this message.
    pub reply: Option<String>,

    /// The message contents.
    pub data: Vec<u8>,

    /// Client for publishing on the reply subject.
    pub(crate) client: Client,
}

impl fmt::Debug for AsyncMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("AsyncMessage")
            .field("subject", &self.subject)
            .field("reply", &self.reply)
            .field("length", &self.data.len())
            .finish()
    }
}
