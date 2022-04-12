// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fmt, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::{
    client::Client,
    header::{self, HeaderMap},
};

use time::OffsetDateTime;

pub(crate) const MESSAGE_NOT_BOUND: &str = "message not bound to a connection";

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
    pub headers: Option<HeaderMap>,

    /// Client for publishing on the reply subject.
    #[doc(hidden)]
    pub client: Option<Client>,

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
    /// Creates new empty `Message`, without a Client.
    /// Useful for passing `Message` data or creating `Message` instance without caring about `Client`,
    /// but cannot be used on it's own for associated methods as those require `Client` injected into `Message`
    /// and will error without it.
    pub fn new(
        subject: &str,
        reply: Option<&str>,
        data: impl AsRef<[u8]>,
        headers: Option<HeaderMap>,
    ) -> Message {
        Message {
            subject: subject.to_string(),
            reply: reply.map(String::from),
            data: data.as_ref().to_vec(),
            headers,
            ..Default::default()
        }
    }

    /// Respond to a request message.
    pub fn respond(&self, msg: impl AsRef<[u8]>) -> io::Result<()> {
        let reply = self.reply.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "No reply subject to reply to")
        })?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, MESSAGE_NOT_BOUND))?;
        client.publish(reply.as_str(), None, None, msg.as_ref())?;
        Ok(())
    }

    /// Determine if the message is a no responders response from the server.
    pub fn is_no_responders(&self) -> bool {
        if !self.data.is_empty() {
            return false;
        }

        if let Some(headers) = &self.headers {
            if headers.get(header::STATUS) == Some(&"503".to_string()) {
                return true;
            }
        }

        false
    }

    /// Determine if a message is `404 No Messages`.
    pub(crate) fn is_no_messages(&self) -> bool {
        if let Some(headers) = &self.headers {
            if headers.get(header::STATUS) == Some(&"404".to_string()) {
                return true;
            }
        }

        false
    }

    // Determine if a message is `408 Request Timeout`.
    pub(crate) fn is_request_timeout(&self) -> bool {
        if let Some(headers) = &self.headers {
            if headers.get(header::STATUS) == Some(&"408".to_string()) {
                return true;
            }
        }
        false
    }

    // Helper for detecting flow control messages.
    pub(crate) fn is_flow_control(&self) -> bool {
        if !self.data.is_empty() {
            return false;
        }

        if let Some(headers) = &self.headers {
            if headers.get(header::STATUS) != Some(&"100".to_string()) {
                return false;
            }

            if headers.get(header::DESCRIPTION) == Some(&"Flow Control".to_string()) {
                return true;
            }

            if headers.get(header::DESCRIPTION) == Some(&"FlowControl Request".to_string()) {
                return true;
            }
        }

        false
    }

    // Helper for detecting idle heartbeat messages.
    pub(crate) fn is_idle_heartbeat(&self) -> bool {
        if !self.data.is_empty() {
            return false;
        }

        if let Some(headers) = &self.headers {
            if headers.get(header::STATUS) != Some(&"100".to_string()) {
                return false;
            }

            if headers.get(header::DESCRIPTION) == Some(&"Idle Heartbeat".to_string()) {
                return true;
            }
        }

        false
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
    pub fn ack_kind(&self, ack_kind: crate::jetstream::AckKind) -> io::Result<()> {
        self.respond(ack_kind)
    }

    /// Acknowledge a `JetStream` message and wait for acknowledgement from the server
    /// that it has received our ack. Retry acknowledgement until we receive a response.
    /// See `AckKind` documentation for details of what each variant means.
    ///
    /// Returns immediately if this message has already been double-acked.
    pub fn double_ack(&self, ack_kind: crate::jetstream::AckKind) -> io::Result<()> {
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
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, MESSAGE_NOT_BOUND))?;

        loop {
            retries += 1;
            if retries == 2 {
                log::warn!("double_ack is retrying until the server connection is reestablished");
            }
            let ack_reply = format!("_INBOX.{}", nuid::next());
            let sub_ret = client.subscribe(&ack_reply, None);
            if sub_ret.is_err() {
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            let (sid, receiver) = sub_ret?;
            let sub =
                crate::Subscription::new(sid, ack_reply.to_string(), receiver, client.clone());

            let pub_ret = client.publish(original_reply, Some(&ack_reply), None, ack_kind.as_ref());
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
    #[allow(clippy::eval_order_dependence)]
    pub fn jetstream_message_info(&self) -> Option<crate::jetstream::JetStreamMessageInfo<'_>> {
        const PREFIX: &str = "$JS.ACK.";
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
        for each_token in &mut tokens {
            if let Some(token) = split.next() {
                *each_token = Some(token);
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
                    let nanos: i128 = try_parse!();
                    OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()?
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
                    let nanos: i128 = try_parse!();
                    OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()?
                },
                pending: try_parse!(),
                token: None,
            })
        } else {
            None
        }
    }
}

impl Default for Message {
    fn default() -> Message {
        Message {
            subject: String::from(""),
            reply: None,
            data: Vec::new(),
            headers: None,
            client: None,
            double_acked: Arc::new(AtomicBool::new(false)),
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
