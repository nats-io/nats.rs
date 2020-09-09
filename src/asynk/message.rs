use std::{
    fmt,
    io,
};

use crate::{asynk::client::Client, Headers};

/// A message received on a subject.
#[derive(Clone)]
pub struct Message {
    /// The subject this message came from.
    pub subject: String,

    /// Optional reply subject that may be used for sending a response to this message.
    pub reply: Option<String>,

    /// The message contents.
    pub data: Vec<u8>,

    /// Optional headers associated with this `Message`.
    pub headers: Option<Headers>,

    /// Client for publishing on the reply subject.
    pub(crate) client: Client,
}

impl Message {
    /// Respond to a request message.
    pub async fn respond(&self, msg: impl AsRef<[u8]>) -> io::Result<()> {
        match self.reply.as_ref() {
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no reply subject available",
            )),
            Some(reply) => self.client.publish(reply, None, None, msg.as_ref()).await,
        }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("Message")
            .field("subject", &self.subject)
            .field("headers", &self.headers)
            .field("reply", &self.reply)
            .field("length", &self.data.len())
            .finish()
    }
}
