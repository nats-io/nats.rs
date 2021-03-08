use std::{fmt, io};

use crate::{client::Client, Headers};

/// A message received on a subject.
#[derive(Clone)]
pub struct Message {
    /// The subject this message came from.
    pub subject: String,

    /// Optional reply subject that may be used for sending a response to this
    /// message.
    pub reply: Option<String>,

    /// The message contents.
    pub data: Vec<u8>,

    /// Optional headers associated with this `Message`.
    pub headers: Option<Headers>,

    /// Client for publishing on the reply subject.
    #[doc(hidden)]
    pub client: Client,
}

impl Message {
    /// Respond to a request message.
    pub fn respond(&self, msg: impl AsRef<[u8]>) -> io::Result<()> {
        match self.reply.as_ref() {
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no reply subject available",
            )),
            Some(reply) => self.client.publish(reply, None, None, msg.as_ref()),
        }
    }

    /// Acknowledge a `JetStream` message with a default acknowledgement.
    /// See `AckKind` documentation for details of what other types of
    /// acks are available. If you need to send a non-default ack, use
    /// the `ack_kind` method below. If you need to block until the
    /// server acks your ack, use the `double_ack` method instead.
    ///
    /// Requires the `jetstream` feature.
    #[cfg(feature = "jetstream")]
    pub fn ack(&self) -> io::Result<()> {
        self.respond(b"")
    }

    /// Acknowledge a `JetStream` message. See `AckKind` documentation for
    /// details of what each variant means. If you need to block until the
    /// server acks your ack, use the `double_ack` method instead.
    ///
    /// Requires the `jetstream` feature.
    #[cfg(feature = "jetstream")]
    pub fn ack_kind(
        &self,
        ack_kind: crate::jetstream::AckKind,
    ) -> io::Result<()> {
        self.respond(ack_kind)
    }

    /// Acknowledge a `JetStream` message and wait for acknowledgement from the server
    /// that it has received our ack. Retry acknowledgement until we receive a response.
    /// See `AckKind` documentation for details of what each variant means.
    ///
    /// Requires the `jetstream` feature.
    #[cfg(feature = "jetstream")]
    pub fn double_ack(
        &self,
        ack_kind: crate::jetstream::AckKind,
    ) -> io::Result<()> {
        let original_reply = match self.reply.as_ref() {
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "No reply subject available (not a JetStream message)",
                ))
            }
            Some(original_reply) => original_reply,
        };
        let mut retries = 0;
        loop {
            retries += 1;
            if retries == 2 {
                log::warn!("double_ack is retrying until the server connection is reestablished");
            }
            let ack_reply = format!("_INBOX.{}", nuid::next());
            let sub_ret = self.client.subscribe(&ack_reply, None);
            if sub_ret.is_err() {
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            let (sid, receiver) = sub_ret?;
            let sub = crate::Subscription::new(
                sid,
                ack_reply.to_string(),
                receiver,
                self.client.clone(),
            );

            let pub_ret = self.client.publish(
                original_reply,
                Some(&ack_reply),
                None,
                ack_kind.as_ref(),
            );
            if pub_ret.is_err() {
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            if sub
                .next_timeout(std::time::Duration::from_millis(100))
                .is_ok()
            {
                return Ok(());
            }
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

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut body = format!("[{} bytes]", self.data.len());
        if let Ok(str) = std::str::from_utf8(&self.data) {
            body = str.to_string();
        }
        if let Some(reply) = &self.reply {
            write!(
                f,
                "Message {{\n  subject: \"{}\",\n  reply: \"{}\",\n  data: \
                 \"{}\"\n}}",
                self.subject, reply, body
            )
        } else {
            write!(
                f,
                "Message {{\n  subject: \"{}\",\n  data: \"{}\"\n}}",
                self.subject, body
            )
        }
    }
}
