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

use super::{header::HeaderMap, status::StatusCode, Command, Message, Subscriber};
use bytes::Bytes;
use futures::future::TryFutureExt;
use futures::stream::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;
use std::fmt::Display;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::trace;

lazy_static! {
    static ref VERSION_RE: Regex = Regex::new(r#"\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?"#).unwrap();
}

/// An error returned from the [`Client::publish`], [`Client::publish_with_headers`],
/// [`Client::publish_with_reply`] or [`Client::publish_with_reply_and_headers`] functions.
#[derive(Debug, Error)]
#[error("failed to publish message: {0}")]
pub struct PublishError(#[source] Box<dyn std::error::Error + Send + Sync>);

impl From<tokio::sync::mpsc::error::SendError<Command>> for PublishError {
    fn from(err: tokio::sync::mpsc::error::SendError<Command>) -> Self {
        PublishError(Box::new(err))
    }
}

/// Client is a `Cloneable` handle to NATS connection.
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
    /// # Examples
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

    /// Publish a [Message] to a given subject.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// client.publish("events.data".into(), "payload".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish(&self, subject: String, payload: Bytes) -> Result<(), PublishError> {
        self.sender
            .send(Command::Publish {
                subject,
                payload,
                respond: None,
                headers: None,
            })
            .await?;
        Ok(())
    }

    /// Publish a [Message] with headers to a given subject.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use std::str::FromStr;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut headers = async_nats::HeaderMap::new();
    /// headers.insert("X-Header", async_nats::HeaderValue::from_str("Value").unwrap());
    /// client.publish_with_headers("events.data".into(), headers, "payload".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_with_headers(
        &self,
        subject: String,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<(), PublishError> {
        self.sender
            .send(Command::Publish {
                subject,
                payload,
                respond: None,
                headers: Some(headers),
            })
            .await?;
        Ok(())
    }

    /// Publish a [Message] to a given subject, with specified response subject
    /// to which the subscriber can respond.
    /// This method does not await for the response.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// client.publish_with_reply("events.data".into(), "reply_subject".into(), "payload".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_with_reply(
        &self,
        subject: String,
        reply: String,
        payload: Bytes,
    ) -> Result<(), PublishError> {
        self.sender
            .send(Command::Publish {
                subject,
                payload,
                respond: Some(reply),
                headers: None,
            })
            .await?;
        Ok(())
    }

    /// Publish a [Message] to a given subject with headers and specified response subject
    /// to which the subscriber can respond.
    /// This method does not await for the response.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use std::str::FromStr;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut headers = async_nats::HeaderMap::new();
    /// client.publish_with_reply_and_headers("events.data".into(), "reply_subject".into(), headers, "payload".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
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
            .await?;
        Ok(())
    }

    /// Sends the request with headers.
    ///
    /// # Examples
    /// ```no_run
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let response = client.request("service".into(), "data".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request(&self, subject: String, payload: Bytes) -> Result<Message, RequestError> {
        trace!("request sent to subject: {} ({})", subject, payload.len());
        let request = Request::new().payload(payload);
        self.send_request(subject, request).await
    }

    /// Sends the request with headers.
    ///
    /// # Examples
    /// ```no_run
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut headers = async_nats::HeaderMap::new();
    /// headers.insert("Key", "Value");
    /// let response = client.request_with_headers("service".into(), headers, "data".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request_with_headers(
        &self,
        subject: String,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<Message, RequestError> {
        let request = Request::new().headers(headers).payload(payload);
        self.send_request(subject, request).await
    }

    /// Sends the request created by the [Request].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let request = async_nats::Request::new().payload("data".into());
    /// let response = client.send_request("service".into(), request).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_request(
        &self,
        subject: String,
        request: Request,
    ) -> Result<Message, RequestError> {
        let inbox = request.inbox.unwrap_or_else(|| self.new_inbox());
        let timeout = request.timeout.unwrap_or(self.request_timeout);
        let mut sub = self.subscribe(inbox.clone()).await?;
        let payload: Bytes = request.payload.unwrap_or_else(Bytes::new);
        match request.headers {
            Some(headers) => {
                self.publish_with_reply_and_headers(subject, inbox, headers, payload)
                    .await?
            }
            None => self.publish_with_reply(subject, inbox, payload).await?,
        }
        self.flush()
            .await
            .map_err(|err| RequestError::with_source(RequestErrorKind::Other, err))?;
        let request = match timeout {
            Some(timeout) => {
                tokio::time::timeout(timeout, sub.next())
                    .map_err(|err| RequestError::with_source(RequestErrorKind::TimedOut, err))
                    .await?
            }
            None => sub.next().await,
        };
        match request {
            Some(message) => {
                if message.status == Some(StatusCode::NO_RESPONDERS) {
                    return Err(RequestError::with_source(
                        RequestErrorKind::NoResponders,
                        "no responders",
                    ));
                }
                Ok(message)
            }
            None => Err(RequestError::with_source(
                RequestErrorKind::Other,
                "broken pipe",
            )),
        }
    }

    /// Create a new globally unique inbox which can be used for replies.
    ///
    /// # Examples
    ///
    /// ```no_run
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

    /// Subscribes to a subject to receive [messages][Message].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut subscription = client.subscribe("events.>".into()).await?;
    /// while let Some(message) = subscription.next().await {
    ///     println!("received message: {:?}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe(&self, subject: String) -> Result<Subscriber, SubscribeError> {
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

    /// Subscribes to a subject with a queue group to receive [messages][Message].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut subscription = client.queue_subscribe("events.>".into(), "queue".into()).await?;
    /// while let Some(message) = subscription.next().await {
    ///     println!("received message: {:?}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn queue_subscribe(
        &self,
        subject: String,
        queue_group: String,
    ) -> Result<Subscriber, SubscribeError> {
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

    /// Flushes the internal buffer ensuring that all messages are sent.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// client.flush().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn flush(&self) -> Result<(), FlushError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(Command::Flush { result: tx })
            .await
            .map_err(|err| FlushError::with_source(FlushErrorKind::SendError, err))?;
        // first question mark is an error from rx itself, second for error from flush.
        rx.await
            .map_err(|err| FlushError::with_source(FlushErrorKind::FlushError, err))?
            .map_err(|err| FlushError::with_source(FlushErrorKind::FlushError, err))?;
        Ok(())
    }

    /// Returns the current state of the connection.
    ///
    /// # Examples
    /// ```no_run
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

/// Used for building customized requests.
#[derive(Default)]
pub struct Request {
    payload: Option<Bytes>,
    headers: Option<HeaderMap>,
    timeout: Option<Option<Duration>>,
    inbox: Option<String>,
}

impl Request {
    pub fn new() -> Request {
        Default::default()
    }

    /// Sets the payload of the request. If not used, empty payload will be sent.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let request = async_nats::Request::new().payload("data".into());
    /// client.send_request("service".into(), request).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn payload(mut self, payload: Bytes) -> Request {
        self.payload = Some(payload);
        self
    }

    /// Sets the headers of the requests.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use std::str::FromStr;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut headers = async_nats::HeaderMap::new();
    /// headers.insert("X-Example", async_nats::HeaderValue::from_str("Value").unwrap());
    /// let request = async_nats::Request::new()
    ///     .headers(headers)
    ///     .payload("data".into());
    /// client.send_request("service".into(), request).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn headers(mut self, headers: HeaderMap) -> Request {
        self.headers = Some(headers);
        self
    }

    /// Sets the custom timeout of the request. Overrides default [Client] timeout.
    /// Setting it to [Option::None] disables the timeout entirely which might result in deadlock.
    /// To use default timeout, simply do not call this function.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let request = async_nats::Request::new()
    ///     .timeout(Some(std::time::Duration::from_secs(15)))
    ///     .payload("data".into());
    /// client.send_request("service".into(), request).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn timeout(mut self, timeout: Option<Duration>) -> Request {
        self.timeout = Some(timeout);
        self
    }

    /// Sets custom inbox for this request. Overrides both customized and default [Client] Inbox.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use std::str::FromStr;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let request = async_nats::Request::new()
    ///     .inbox("custom_inbox".into())
    ///     .payload("data".into());
    /// client.send_request("service".into(), request).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn inbox(mut self, inbox: String) -> Request {
        self.inbox = Some(inbox);
        self
    }
}

#[derive(Error, Debug)]
#[error("failed to send subscribe: {0}")]
pub struct SubscribeError(#[source] Box<dyn std::error::Error + Sync + Send>);

impl From<tokio::sync::mpsc::error::SendError<Command>> for SubscribeError {
    fn from(err: tokio::sync::mpsc::error::SendError<Command>) -> Self {
        SubscribeError(Box::new(err))
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum RequestErrorKind {
    /// There are services listening on requested subject, but they didn't respond
    /// in time.
    TimedOut,
    /// No one is listening on request subject.
    NoResponders,
    /// Other errors, client/io related.
    Other,
}

/// Error returned when a core NATS request fails.
/// To be enumerate over the variants, call [RequestError::kind].
#[derive(Debug, Error)]
pub struct RequestError {
    kind: RequestErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            RequestErrorKind::TimedOut => write!(f, "request timed out"),
            RequestErrorKind::NoResponders => write!(f, "no responders"),
            RequestErrorKind::Other => write!(f, "request failed: {:?}", self.source),
        }
    }
}

impl RequestError {
    fn with_source<E>(kind: RequestErrorKind, source: E) -> RequestError
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        RequestError {
            kind,
            source: Some(source.into()),
        }
    }

    /// Returns the [RequestErrorKind] enum, allowing iterating over
    /// all error variants.
    pub fn kind(&self) -> RequestErrorKind {
        self.kind
    }
}

/// Error returned when flushing the messages buffered on the client fails.
/// To be enumerate over the variants, call [FlushError::kind].
#[derive(Debug, Error)]
pub struct FlushError {
    kind: FlushErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Display for FlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let source_info = self
            .source
            .as_ref()
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no details".into());
        match self.kind {
            FlushErrorKind::SendError => write!(f, "failed to send flush request: {}", source_info),
            FlushErrorKind::FlushError => write!(f, "flush failed: {}", source_info),
        }
    }
}

impl FlushError {
    fn with_source<E>(kind: FlushErrorKind, source: E) -> FlushError
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        FlushError {
            kind,
            source: Some(source.into()),
        }
    }
    pub fn kind(&self) -> FlushErrorKind {
        self.kind
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FlushErrorKind {
    /// Sending the flush failed client side.
    SendError,
    /// Flush failed.
    /// This can happen mostly in case of connection issues
    /// that cannot be resolved quickly.
    FlushError,
}

impl From<PublishError> for RequestError {
    fn from(e: PublishError) -> Self {
        RequestError::with_source(RequestErrorKind::Other, e)
    }
}
impl From<SubscribeError> for RequestError {
    fn from(e: SubscribeError) -> Self {
        RequestError::with_source(RequestErrorKind::Other, e)
    }
}
