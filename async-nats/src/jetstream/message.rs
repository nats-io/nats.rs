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

//! A wrapped `crate::Message` with `JetStream` related methods.
use super::context::Context;
use crate::Error;

use bytes::Bytes;
use futures::future::TryFutureExt;
use futures::StreamExt;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Message {
    pub message: crate::Message,
    pub context: Context,
}

impl std::ops::Deref for Message {
    type Target = crate::Message;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl From<Message> for crate::Message {
    fn from(source: Message) -> crate::Message {
        source.message
    }
}

impl Message {
    /// Acknowledges a message delivery by sending `+ACK` to the server.
    ///
    /// If [AckPolicy][crate::jetstream::consumer::AckPolicy] is set to `All` or `Explicit`, messages has to be acked.
    /// Otherwise redeliveries will occur and [Consumer][crate::jetstream::consumer::Consumer] will not be able to advance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.fetch().max_messages(100).messages().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     message?.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ack(&self) -> Result<(), Error> {
        if let Some(ref reply) = self.reply {
            self.context
                .client
                .publish(reply.to_string(), "".into())
                .map_err(Error::from)
                .await
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No reply subject, not a JetStream message",
            )))
        }
    }

    /// Acknowledges a message delivery by sending a chosen [AckKind] variant to the server.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use async_nats::jetstream::AckKind;
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.fetch().max_messages(100).messages().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     message?.ack_with(AckKind::Nak).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ack_with(&self, kind: AckKind) -> Result<(), Error> {
        if let Some(ref reply) = self.reply {
            self.context
                .client
                .publish(reply.to_string(), kind.into())
                .map_err(Error::from)
                .await
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No reply subject, not a JetStream message",
            )))
        }
    }

    /// Acknowledges a message delivery by sending `+ACK` to the server
    /// and awaits for confirmation for the server that it received the message.
    /// Useful if user wants to ensure `exactly once` semantics.
    ///
    /// If [AckPolicy][crate::jetstream::consumer::AckPolicy] is set to `All` or `Explicit`, messages has to be acked.
    /// Otherwise redeliveries will occur and [Consumer][crate::jetstream::consumer::Consumer] will not be able to advance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.fetch().max_messages(100).messages().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     message?.double_ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn double_ack(&self) -> Result<(), Error> {
        if let Some(ref reply) = self.reply {
            let inbox = self.context.client.new_inbox();
            let mut subscription = self.context.client.subscribe(inbox.clone()).await?;
            self.context
                .client
                .publish_with_reply(reply.to_string(), inbox, AckKind::Ack.into())
                .await?;
            match tokio::time::timeout(self.context.timeout, subscription.next())
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "double ack response timed out",
                    )
                })? {
                Some(_) => Ok(()),
                None => Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "subscription dropped",
                ))),
            }
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No reply subject, not a JetStream message",
            )))
        }
    }

    /// Returns the `JetStream` message ID
    /// if this is a `JetStream` message.
    #[allow(clippy::mixed_read_write_in_expression)]
    pub fn info(&self) -> Result<Info<'_>, Error> {
        const PREFIX: &str = "$JS.ACK.";
        const SKIP: usize = PREFIX.len();

        let mut reply: &str = self.reply.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "did not found reply subject")
        })?;

        if !reply.starts_with(PREFIX) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "did not found proper prefix",
            )));
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
                        return Err(Box::new(e));
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
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "too few tokens",
                    )));
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
            Ok(Info {
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
                stream_sequence: try_parse!(),
                consumer_sequence: try_parse!(),
                published: {
                    let nanos: i128 = try_parse!();
                    OffsetDateTime::from_unix_timestamp_nanos(nanos)?
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
            Ok(Info {
                domain: None,
                acc_hash: None,
                stream: try_parse!(str),
                consumer: try_parse!(str),
                delivered: try_parse!(),
                stream_sequence: try_parse!(),
                consumer_sequence: try_parse!(),
                published: {
                    let nanos: i128 = try_parse!();
                    OffsetDateTime::from_unix_timestamp_nanos(nanos)?
                },
                pending: try_parse!(),
                token: None,
            })
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "bad token number",
            )))
        }
    }
}

/// The kinds of response used for acknowledging a processed message.
#[derive(Debug, Clone, Copy)]
pub enum AckKind {
    /// Acknowledges a message was completely handled.
    Ack,
    /// Signals that the message will not be processed now
    /// and processing can move onto the next message, NAK'd
    /// message will be retried.
    Nak,
    /// When sent before the AckWait period indicates that
    /// work is ongoing and the period should be extended by
    /// another equal to AckWait.
    Progress,
    /// Acknowledges the message was handled and requests
    /// delivery of the next message to the reply subject.
    /// Only applies to Pull-mode.
    Next,
    /// Instructs the server to stop redelivery of a message
    /// without acknowledging it as successfully processed.
    Term,
}

impl From<AckKind> for Bytes {
    fn from(kind: AckKind) -> Self {
        use AckKind::*;
        match kind {
            Ack => Bytes::from_static(b"+ACK"),
            Nak => Bytes::from_static(b"-NAK"),
            Progress => Bytes::from_static(b"+WPI"),
            Next => Bytes::from_static(b"+NXT"),
            Term => Bytes::from_static(b"+TERM"),
        }
    }
}

/// Information about a received message
#[derive(Debug, Clone)]
pub struct Info<'a> {
    /// Optional domain, present in servers post-ADR-15
    pub domain: Option<&'a str>,
    /// Optional account hash, present in servers post-ADR-15
    pub acc_hash: Option<&'a str>,
    /// The stream name
    pub stream: &'a str,
    /// The consumer name
    pub consumer: &'a str,
    /// The stream sequence number associated with this message
    pub stream_sequence: u64,
    /// The consumer sequence number associated with this message
    pub consumer_sequence: u64,
    /// the number of messages known by the server to be delivered to this consumer
    pub delivered: i64,
    /// the number of messages known by the server to be pending to this consumer
    pub pending: u64,
    /// the time that this message was received by the server from its publisher
    pub published: time::OffsetDateTime,
    /// Optional token, present in servers post-ADR-15
    pub token: Option<&'a str>,
}
