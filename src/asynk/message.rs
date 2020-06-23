use std::fmt;
use std::io::{self, Error, ErrorKind};

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

impl AsyncMessage {
    /// Responds to a request.
    ///
    /// The response will be published as a message on the `reply` subject.
    ///
    /// If `reply` is [`None`], an error will be returned.
    pub async fn respond(self, msg: impl AsRef<[u8]>) -> io::Result<()> {
        match self.reply.as_ref() {
            None => Err(Error::new(
                ErrorKind::InvalidInput,
                "no reply subject available",
            )),
            Some(reply) => self.client.publish(reply, None, msg.as_ref()).await,
        }
    }
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
