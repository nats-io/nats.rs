use std::{
    fmt, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

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

    /// Whether this message has already been successfully double-acked
    /// using `JetStream`.
    #[doc(hidden)]
    pub double_acked: Arc<AtomicBool>,
}

impl From<crate::asynk::Message> for Message {
    fn from(asynk: crate::asynk::Message) -> Message {
        Message {
            subject: asynk.subject,
            reply: asynk.reply,
            data: asynk.data,
            headers: asynk.headers,
            client: asynk.client,
            double_acked: asynk.double_acked,
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
    pub fn jetstream_message_info(
        &self,
    ) -> Option<crate::jetstream::JetStreamMessageInfo<'_>> {
        const PREFIX: &'static str = "$JS.ACK.";
        const SKIP: usize = PREFIX.len();

        let mut reply: &str = self.reply.as_ref()?;

        if !reply.starts_with(PREFIX) {
            return None;
        }

        reply = &reply[SKIP..];

        let mut split = reply.split('.');

        // we should avoid allocating to prevent
        // large performance degradations in
        // parsing this.
        let mut tokens: [Option<&str>; 10] = [None; 10];
        let mut n_tokens = 0;
        for index in 0..10 {
            if let Some(token) = split.next() {
                tokens[index] = Some(token);
                n_tokens += 1;
            }
        }

        let mut token_index = 0;

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
                if let Some(next) = tokens[token_index].take() {
                    #[allow(unused)]
                    {
                        // this isn't actually unused, but it's
                        // difficult for the compiler to infer this.
                        token_index += 1;
                    }
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

        // now we can try to parse the tokens to
        // individual types. We use an if-else
        // chain instead of a match because it
        // produces more optimal code usually,
        // and we want to try the 9 (11 - the first 2)
        // case first because we expect it to
        // be the most common. We use >= to be
        // future-proof.
        if n_tokens >= 9 {
            Some(crate::jetstream::JetStreamMessageInfo {
                domain: {
                    let domain: &str = try_parse!(str);
                    if domain == "_" {
                        None
                    } else {
                        Some(domain)
                    }
                },
                acc_hash: Some(try_parse!(str)),
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
                token: if n_tokens >= 9 {
                    Some(try_parse!(str))
                } else {
                    None
                },
            })
        } else if n_tokens == 7 {
            // we expect this to be increasingly rare, as older
            // servers are phased out.
            Some(crate::jetstream::JetStreamMessageInfo {
                domain: None,
                acc_hash: None,
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
                token: None,
            })
        } else {
            None
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
