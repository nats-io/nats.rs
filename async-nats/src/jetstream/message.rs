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
    /// Examples
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
    /// let mut messages = consumer.fetch(100).await?;
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

    /// Acknowledges a message delivery by sending a choosen [AckKind] variant to the server.
    ///
    /// Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use async_nats::jetstream::AckKind;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.fetch(100).await?;
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
    /// Examples
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
    /// let mut messages = consumer.fetch(100).await?;
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
            match subscription.next().await {
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
