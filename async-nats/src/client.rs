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
use crate::subject::ToSubject;
use crate::ServerInfo;

use super::{header::HeaderMap, status::StatusCode, Command, Message, Subscriber};
use crate::error::Error;
use bytes::Bytes;
use futures::future::TryFutureExt;
use futures::StreamExt;
use once_cell::sync::Lazy;
use portable_atomic::AtomicU64;
use regex::Regex;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::trace;

static VERSION_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?").unwrap());

/// An error returned from the [`Client::publish`], [`Client::publish_with_headers`],
/// [`Client::publish_with_reply`] or [`Client::publish_with_reply_and_headers`] functions.
pub type PublishError = Error<PublishErrorKind>;

impl From<tokio::sync::mpsc::error::SendError<Command>> for PublishError {
    fn from(err: tokio::sync::mpsc::error::SendError<Command>) -> Self {
        PublishError::with_source(PublishErrorKind::Send, err)
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PublishErrorKind {
    MaxPayloadExceeded,
    Send,
}

impl Display for PublishErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishErrorKind::MaxPayloadExceeded => write!(f, "max payload size exceeded"),
            PublishErrorKind::Send => write!(f, "failed to send message"),
        }
    }
}

/// Client is a `Cloneable` handle to NATS connection.
/// Client should not be created directly. Instead, one of two methods can be used:
/// [crate::connect] and [crate::ConnectOptions::connect]
#[derive(Clone, Debug)]
pub struct Client {
    info: tokio::sync::watch::Receiver<ServerInfo>,
    pub(crate) state: tokio::sync::watch::Receiver<State>,
    pub(crate) sender: mpsc::Sender<Command>,
    next_subscription_id: Arc<AtomicU64>,
    subscription_capacity: usize,
    inbox_prefix: Arc<str>,
    request_timeout: Option<Duration>,
    max_payload: Arc<AtomicUsize>,
}

impl Client {
    pub(crate) fn new(
        info: tokio::sync::watch::Receiver<ServerInfo>,
        state: tokio::sync::watch::Receiver<State>,
        sender: mpsc::Sender<Command>,
        capacity: usize,
        inbox_prefix: String,
        request_timeout: Option<Duration>,
        max_payload: Arc<AtomicUsize>,
    ) -> Client {
        Client {
            info,
            state,
            sender,
            next_subscription_id: Arc::new(AtomicU64::new(1)),
            subscription_capacity: capacity,
            inbox_prefix: inbox_prefix.into(),
            request_timeout,
            max_payload,
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
    /// This has to be used with caution, as it is not guaranteed that the server
    /// that client is connected to is the same version that the one that is
    /// a JetStream meta/stream/consumer leader, especially across leafnodes.
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

        let server_version_captures = match VERSION_RE.captures(&info.version) {
            Some(captures) => captures,
            None => return false,
        };

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
    /// client.publish("events.data", "payload".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish<S: ToSubject>(
        &self,
        subject: S,
        payload: Bytes,
    ) -> Result<(), PublishError> {
        let subject = subject.to_subject();
        let max_payload = self.max_payload.load(Ordering::Relaxed);
        if payload.len() > max_payload {
            return Err(PublishError::with_source(
                PublishErrorKind::MaxPayloadExceeded,
                format!(
                    "Payload size limit of {} exceeded by message size of {}",
                    payload.len(),
                    max_payload
                ),
            ));
        }

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
    /// headers.insert(
    ///     "X-Header",
    ///     async_nats::HeaderValue::from_str("Value").unwrap(),
    /// );
    /// client
    ///     .publish_with_headers("events.data", headers, "payload".into())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_with_headers<S: ToSubject>(
        &self,
        subject: S,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<(), PublishError> {
        let subject = subject.to_subject();

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
    /// client
    ///     .publish_with_reply("events.data", "reply_subject", "payload".into())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_with_reply<S: ToSubject, R: ToSubject>(
        &self,
        subject: S,
        reply: R,
        payload: Bytes,
    ) -> Result<(), PublishError> {
        let subject = subject.to_subject();
        let reply = reply.to_subject();

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
    /// client
    ///     .publish_with_reply_and_headers("events.data", "reply_subject", headers, "payload".into())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_with_reply_and_headers<S: ToSubject, R: ToSubject>(
        &self,
        subject: S,
        reply: R,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<(), PublishError> {
        let subject = subject.to_subject();
        let reply = reply.to_subject();

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
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let response = client.request("service", "data".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request<S: ToSubject>(
        &self,
        subject: S,
        payload: Bytes,
    ) -> Result<Message, RequestError> {
        let subject = subject.to_subject();

        trace!(
            "request sent to subject: {} ({})",
            subject.as_ref(),
            payload.len()
        );
        let request = Request::new().payload(payload);
        self.send_request(subject, request).await
    }

    /// Sends the request with headers.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut headers = async_nats::HeaderMap::new();
    /// headers.insert("Key", "Value");
    /// let response = client
    ///     .request_with_headers("service", headers, "data".into())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request_with_headers<S: ToSubject>(
        &self,
        subject: S,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<Message, RequestError> {
        let subject = subject.to_subject();

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
    /// let response = client.send_request("service", request).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_request<S: ToSubject>(
        &self,
        subject: S,
        request: Request,
    ) -> Result<Message, RequestError> {
        let subject = subject.to_subject();

        if let Some(inbox) = request.inbox {
            let timeout = request.timeout.unwrap_or(self.request_timeout);
            let mut subscriber = self.subscribe(inbox.clone()).await?;
            let payload: Bytes = request.payload.unwrap_or_default();
            match request.headers {
                Some(headers) => {
                    self.publish_with_reply_and_headers(subject, inbox, headers, payload)
                        .await?
                }
                None => self.publish_with_reply(subject, inbox, payload).await?,
            }
            let request = match timeout {
                Some(timeout) => {
                    tokio::time::timeout(timeout, subscriber.next())
                        .map_err(|err| RequestError::with_source(RequestErrorKind::TimedOut, err))
                        .await?
                }
                None => subscriber.next().await,
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
        } else {
            let (sender, receiver) = oneshot::channel();

            let payload = request.payload.unwrap_or_default();
            let respond = self.new_inbox().into();
            let headers = request.headers;

            self.sender
                .send(Command::Request {
                    subject,
                    payload,
                    respond,
                    headers,
                    sender,
                })
                .map_err(|err| RequestError::with_source(RequestErrorKind::Other, err))
                .await?;

            let timeout = request.timeout.unwrap_or(self.request_timeout);
            let request = match timeout {
                Some(timeout) => {
                    tokio::time::timeout(timeout, receiver)
                        .map_err(|err| RequestError::with_source(RequestErrorKind::TimedOut, err))
                        .await?
                }
                None => receiver.await,
            };

            match request {
                Ok(message) => {
                    if message.status == Some(StatusCode::NO_RESPONDERS) {
                        return Err(RequestError::with_source(
                            RequestErrorKind::NoResponders,
                            "no responders",
                        ));
                    }
                    Ok(message)
                }
                Err(err) => Err(RequestError::with_source(RequestErrorKind::Other, err)),
            }
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
    /// let mut subscription = client.subscribe("events.>").await?;
    /// while let Some(message) = subscription.next().await {
    ///     println!("received message: {:?}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe<S: ToSubject>(&self, subject: S) -> Result<Subscriber, SubscribeError> {
        let subject = subject.to_subject();
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
    /// let mut subscription = client.queue_subscribe("events.>", "queue".into()).await?;
    /// while let Some(message) = subscription.next().await {
    ///     println!("received message: {:?}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn queue_subscribe<S: ToSubject>(
        &self,
        subject: S,
        queue_group: String,
    ) -> Result<Subscriber, SubscribeError> {
        let subject = subject.to_subject();

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
            .send(Command::Flush { observer: tx })
            .await
            .map_err(|err| FlushError::with_source(FlushErrorKind::SendError, err))?;

        rx.await
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

    /// Forces the client to reconnect.
    /// Keep in mind that client will reconnect automatically if the connection is lost and this
    /// method does not have to be used in normal circumstances.
    /// However, if you want to force the client to reconnect, for example to re-trigger
    /// the `auth-callback`, or manually rebalance connections, this method can be useful.
    /// This method does not wait for connection to be re-established.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// client.force_reconnect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn force_reconnect(&self) -> Result<(), ReconnectError> {
        self.sender
            .send(Command::Reconnect)
            .await
            .map_err(Into::into)
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
    /// client.send_request("service", request).await?;
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
    /// headers.insert(
    ///     "X-Example",
    ///     async_nats::HeaderValue::from_str("Value").unwrap(),
    /// );
    /// let request = async_nats::Request::new()
    ///     .headers(headers)
    ///     .payload("data".into());
    /// client.send_request("service", request).await?;
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
    /// client.send_request("service", request).await?;
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
    /// client.send_request("service", request).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn inbox(mut self, inbox: String) -> Request {
        self.inbox = Some(inbox);
        self
    }
}

#[derive(Error, Debug)]
#[error("failed to send reconnect: {0}")]
pub struct ReconnectError(#[source] crate::Error);

impl From<tokio::sync::mpsc::error::SendError<Command>> for ReconnectError {
    fn from(err: tokio::sync::mpsc::error::SendError<Command>) -> Self {
        ReconnectError(Box::new(err))
    }
}

#[derive(Error, Debug)]
#[error("failed to send subscribe: {0}")]
pub struct SubscribeError(#[source] crate::Error);

impl From<tokio::sync::mpsc::error::SendError<Command>> for SubscribeError {
    fn from(err: tokio::sync::mpsc::error::SendError<Command>) -> Self {
        SubscribeError(Box::new(err))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RequestErrorKind {
    /// There are services listening on requested subject, but they didn't respond
    /// in time.
    TimedOut,
    /// No one is listening on request subject.
    NoResponders,
    /// Other errors, client/io related.
    Other,
}

impl Display for RequestErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => write!(f, "request timed out"),
            Self::NoResponders => write!(f, "no responders"),
            Self::Other => write!(f, "request failed"),
        }
    }
}

/// Error returned when a core NATS request fails.
/// To be enumerate over the variants, call [RequestError::kind].
pub type RequestError = Error<RequestErrorKind>;

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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FlushErrorKind {
    /// Sending the flush failed client side.
    SendError,
    /// Flush failed.
    /// This can happen mostly in case of connection issues
    /// that cannot be resolved quickly.
    FlushError,
}

impl Display for FlushErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SendError => write!(f, "failed to send flush request"),
            Self::FlushError => write!(f, "flush failed"),
        }
    }
}

pub type FlushError = Error<FlushErrorKind>;
