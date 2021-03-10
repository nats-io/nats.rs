use std::{
    fmt, io,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::{client::Client, Headers};

/// A message received on a subject.
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

    /// Whether this message has already been successfully double-acked
    /// using `JetStream`.
    #[doc(hidden)]
    pub double_acked: AtomicBool,
}

impl Clone for Message {
    fn clone(&self) -> Message {
        Message {
            subject: self.subject.clone(),
            reply: self.reply.clone(),
            data: self.data.clone(),
            headers: self.headers.clone(),
            client: self.client.clone(),
            double_acked: AtomicBool::new(
                self.double_acked.load(Ordering::Acquire),
            ),
        }
    }
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
    /// Returns immediately if this message has already been
    /// double-acked.
    ///
    /// Requires the `jetstream` feature.
    #[cfg(feature = "jetstream")]
    pub fn ack(&self) -> io::Result<()> {
        if self.double_acked.load(Ordering::Acquire) {
            return Ok(());
        }
        self.respond(b"")
    }

    /// Acknowledge a `JetStream` message. See `AckKind` documentation for
    /// details of what each variant means. If you need to block until the
    /// server acks your ack, use the `double_ack` method instead.
    ///
    /// Does not check whether this message has already been double-acked.
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
    /// Returns immediately if this message has already been double-acked.
    ///
    /// Requires the `jetstream` feature.
    #[cfg(feature = "jetstream")]
    pub fn double_ack(
        &self,
        ack_kind: crate::jetstream::AckKind,
    ) -> io::Result<()> {
        if self.double_acked.load(Ordering::Acquire) {
            return Ok(());
        }
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
                self.double_acked.store(true, Ordering::Release);
                return Ok(());
            }
        }
    }

    /// Returns the `JetStream` message ID
    /// if this is a `JetStream` message.
    /// Returns `None` if this is not
    /// a `JetStream` message with headers
    /// set.
    ///
    /// Requires the `jetstream` feature.
    #[cfg(feature = "jetstream")]
    pub fn jetstream_message_info(
        &self,
    ) -> Option<crate::jetstream::JetStreamMessageInfo<'_>> {
        let reply = self.reply.as_ref()?;
        let mut split = reply.split('.');
        if split.next()? != "$JS" || split.next()? != "ACK" {
            return None;
        }

        macro_rules! try_parse {
            () => {
                match str::parse(try_parse!(str)) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        log::error!(
                            "failed to parse jetstream reply \
                            subject: {}, error: {:?}. Is your \
                            nats-server up to date?",
                            reply,
                            e
                        );
                        return None;
                    }
                }
            };
            (str) => {
                if let Some(next) = split.next() {
                    next
                } else {
                    log::error!(
                        "unexpectedly few tokens while parsing \
                        jetstream reply subject: {}. Is your \
                        nats-server up to date?",
                        reply
                    );
                    return None;
                }
            };
        }

        Some(crate::jetstream::JetStreamMessageInfo {
            stream: try_parse!(str),
            consumer: try_parse!(str),
            delivered: try_parse!(),
            stream_seq: try_parse!(),
            consumer_seq: try_parse!(),
            published: {
                let nanos: u64 = try_parse!();
                let offset = std::time::Duration::from_nanos(nanos);
                std::time::UNIX_EPOCH + offset
            },
            pending: try_parse!(),
        })
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
