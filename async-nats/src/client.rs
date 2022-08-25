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

use crate::connection::State;
use crate::ServerInfo;

use super::{header::HeaderMap, status::StatusCode, Command, Error, Message, Subscriber};
use bytes::Bytes;
use futures::future::TryFutureExt;
use futures::stream::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;
use std::error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{self, ErrorKind};
use tokio::sync::mpsc;

lazy_static! {
    static ref VERSION_RE: Regex = Regex::new(r#"\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?"#).unwrap();
}

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
/// [crate::connect] and [crate::ConnectOptions::connect]
#[derive(Clone, Debug)]
pub struct Client {
    info: tokio::sync::watch::Receiver<ServerInfo>,
    pub(crate) state: tokio::sync::watch::Receiver<State>,
    sender: mpsc::Sender<Command>,
    next_subscription_id: Arc<AtomicU64>,
    subscription_capacity: usize,
    inbox_prefix: String,
    request_timeout: Option<Duration>,
}

impl Client {
    pub(crate) fn new(
        info: tokio::sync::watch::Receiver<ServerInfo>,
        state: tokio::sync::watch::Receiver<State>,
        sender: mpsc::Sender<Command>,
        capacity: usize,
        inbox_prefix: String,
        request_timeout: Option<Duration>,
    ) -> Client {
        Client {
            info,
            state,
            sender,
            next_subscription_id: Arc::new(AtomicU64::new(0)),
            subscription_capacity: capacity,
            inbox_prefix,
            request_timeout,
        }
    }

    /// Returns last received info from the server.
    ///
    /// Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main () -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// println!("info: {:?}", client.server_info());
    /// # Ok(())
    /// # }
    /// ```
    pub fn server_info(&self) -> ServerInfo {
        // We ignore notifying the watcher, as that requires mutable client reference.
        self.info.borrow().to_owned()
    }

    /// Returns true if the server version is compatible with the version components.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// assert!(client.is_server_compatible(2, 8, 4));
    /// # Ok(())
    /// # }
    /// ```
    pub fn is_server_compatible(&self, major: i64, minor: i64, patch: i64) -> bool {
        let info = self.server_info();

        let server_version_captures = VERSION_RE.captures(&info.version).unwrap();

        let server_major = server_version_captures
            .get(1)
            .map(|m| m.as_str().parse::<i64>().unwrap())
            .unwrap();

        let server_minor = server_version_captures
            .get(2)
            .map(|m| m.as_str().parse::<i64>().unwrap())
            .unwrap();

        let server_patch = server_version_captures
            .get(3)
            .map(|m| m.as_str().parse::<i64>().unwrap())
            .unwrap();

        if server_major < major
            || (server_major == major && server_minor < minor)
            || (server_major == major && server_minor == minor && server_patch < patch)
        {
            return false;
        }
        true
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
        let request = match self.request_timeout {
            Some(timeout) => {
                tokio::time::timeout(timeout, sub.next())
                    .map_err(|_| std::io::Error::new(ErrorKind::TimedOut, "request timed out"))
                    .await?
            }
            None => sub.next().await,
        };
        match request {
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
        let request = match self.request_timeout {
            Some(timeout) => {
                tokio::time::timeout(timeout, sub.next())
                    .map_err(|_| std::io::Error::new(ErrorKind::TimedOut, "request timed out"))
                    .await?
            }
            None => sub.next().await,
        };
        match request {
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
        format!("{}.{}", self.inbox_prefix, nuid::next())
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

    /// Returns the current state of the connection.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// println!("connection state: {}", client.connection_state());
    /// # Ok(())
    /// # }
    /// ```
    pub fn connection_state(&self) -> State {
        self.state.borrow().to_owned()
    }
}
