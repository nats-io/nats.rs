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

use super::{header::HeaderMap, status::StatusCode, Command, Error, Message, Subscriber};
use bytes::Bytes;
use futures::future::TryFutureExt;
use futures::stream::StreamExt;
use std::error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{self, ErrorKind};
use tokio::sync::mpsc;

/// An error returned from the [`Client::publish`], [`Client::publish_with_headers`],
/// [`Client::publish_with_reply`] or [`Client::publish_with_reply_and_headers`] functions.
pub struct PublishError(mpsc::error::SendError<Command>);

impl fmt::Debug for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishError").finish_non_exhaustive()
    }
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "publishing on a closed client".fmt(f)
    }
}

impl error::Error for PublishError {}

/// Client is a `Clonable` handle to NATS connection.
/// Client should not be created directly. Instead, one of two methods can be used:
/// [connect] and [ConnectOptions::connect]
#[derive(Clone, Debug)]
pub struct Client {
    sender: mpsc::Sender<Command>,
    next_subscription_id: Arc<AtomicU64>,
    subscription_capacity: usize,
}

impl Client {
    pub(crate) fn new(sender: mpsc::Sender<Command>, capacity: usize) -> Client {
        Client {
            sender,
            next_subscription_id: Arc::new(AtomicU64::new(0)),
            subscription_capacity: capacity,
        }
    }

    pub async fn publish(&self, subject: String, payload: Bytes) -> Result<(), PublishError> {
        self.sender
            .send(Command::Publish {
                subject,
                payload,
                respond: None,
                headers: None,
            })
            .map_err(PublishError)
            .await?;
        Ok(())
    }

    pub async fn publish_with_headers(
        &self,
        subject: String,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<(), Error> {
        self.sender
            .send(Command::Publish {
                subject,
                payload,
                respond: None,
                headers: Some(headers),
            })
            .map_err(PublishError)
            .await?;
        Ok(())
    }

    pub async fn publish_with_reply(
        &self,
        subject: String,
        reply: String,
        payload: Bytes,
    ) -> Result<(), Error> {
        self.sender
            .send(Command::Publish {
                subject,
                payload,
                respond: Some(reply),
                headers: None,
            })
            .map_err(PublishError)
            .await?;
        Ok(())
    }

    pub async fn publish_with_reply_and_headers(
        &self,
        subject: String,
        reply: String,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<(), PublishError> {
        self.sender
            .send(Command::Publish {
                subject,
                payload,
                respond: Some(reply),
                headers: Some(headers),
            })
            .map_err(PublishError)
            .await?;
        Ok(())
    }

    pub async fn request(&self, subject: String, payload: Bytes) -> Result<Message, Error> {
        let inbox = self.new_inbox();
        let mut sub = self.subscribe(inbox.clone()).await?;
        self.publish_with_reply(subject, inbox, payload).await?;
        self.flush().await?;
        match sub.next().await {
            Some(message) => {
                if message.status == Some(StatusCode::NO_RESPONDERS) {
                    return Err(Box::new(std::io::Error::new(
                        ErrorKind::NotFound,
                        "nats: no responders",
                    )));
                }
                Ok(message)
            }
            None => Err(Box::new(io::Error::new(
                ErrorKind::BrokenPipe,
                "did not receive any message",
            ))),
        }
    }

    pub async fn request_with_headers(
        &self,
        subject: String,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<Message, Error> {
        let inbox = self.new_inbox();
        let mut sub = self.subscribe(inbox.clone()).await?;
        self.publish_with_reply_and_headers(subject, inbox, headers, payload)
            .await?;
        self.flush().await?;
        match sub.next().await {
            Some(message) => {
                if message.status == Some(StatusCode::NO_RESPONDERS) {
                    return Err(Box::new(std::io::Error::new(
                        ErrorKind::NotFound,
                        "nats: no responders",
                    )));
                }
                Ok(message)
            }
            None => Err(Box::new(io::Error::new(
                ErrorKind::BrokenPipe,
                "did not receive any message",
            ))),
        }
    }

    /// Create a new globally unique inbox which can be used for replies.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// # let mut nc = async_nats::connect("demo.nats.io").await?;
    /// let reply = nc.new_inbox();
    /// let rsub = nc.subscribe(reply).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_inbox(&self) -> String {
        format!("_INBOX.{}", nuid::next())
    }

    pub async fn subscribe(&self, subject: String) -> Result<Subscriber, Error> {
        let sid = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(self.subscription_capacity);

        self.sender
            .send(Command::Subscribe {
                sid,
                subject,
                queue_group: None,
                sender,
            })
            .await?;

        Ok(Subscriber::new(sid, self.sender.clone(), receiver))
    }

    pub async fn queue_subscribe(
        &self,
        subject: String,
        queue_group: String,
    ) -> Result<Subscriber, Error> {
        let sid = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(self.subscription_capacity);

        self.sender
            .send(Command::Subscribe {
                sid,
                subject,
                queue_group: Some(queue_group),
                sender,
            })
            .await?;

        Ok(Subscriber::new(sid, self.sender.clone(), receiver))
    }

    pub async fn flush(&self) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(Command::Flush { result: tx }).await?;
        // first question mark is an error from rx itself, second for error from flush.
        rx.await??;
        Ok(())
    }
}
