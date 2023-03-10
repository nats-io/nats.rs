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
use futures::FutureExt;
use lazy_static::lazy_static;
use regex::Regex;
use std::error;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
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

pub struct Publish<'a> {
    client: &'a Client,
    subject: String,
    payload: Bytes,
    headers: Option<HeaderMap>,
    respond: Option<String>,
}

impl<'a> Publish<'a> {
    pub fn new(client: &Client, subject: String, payload: Bytes) -> Publish {
        Publish {
            client,
            subject,
            payload,
            headers: None,
            respond: None,
        }
    }

    pub fn headers(mut self, headers: HeaderMap) -> Publish<'a> {
        self.headers = Some(headers);
        self
    }

    pub fn reply(mut self, subject: String) -> Publish<'a> {
        self.respond = Some(subject);
        self
    }
}

impl<'a> IntoFuture for Publish<'a> {
    type Output = Result<(), PublishError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<(), PublishError>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        self.client
            .sender
            .send(Command::Publish {
                subject: self.subject,
                payload: self.payload,
                respond: self.respond,
                headers: self.headers,
            })
            .map_err(PublishError)
            .boxed()
    }
}

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
    pub fn publish(&self, subject: String, payload: Bytes) -> Publish {
        Publish::new(self, subject, payload)
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
        self.publish(subject, payload).headers(headers).await?;

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
        self.publish(subject, payload).reply(reply).await?;

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
        self.publish(subject, payload)
            .headers(headers)
            .reply(reply)
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
    pub fn request(&self, subject: String, payload: Bytes) -> Request {
        Request::new(self, subject, payload)
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
    ) -> Result<Message, Error> {
        let message = Request::new(self, subject, payload)
            .headers(headers)
            .await?;

        Ok(message)
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

/// Used for building and sending requests.
#[derive(Debug)]
pub struct Request<'a> {
    client: &'a Client,
    subject: String,
    payload: Option<Bytes>,
    headers: Option<HeaderMap>,
    timeout: Option<Option<Duration>>,
    inbox: Option<String>,
}

impl<'a> Request<'a> {
    pub fn new(client: &'a Client, subject: String, payload: Bytes) -> Request<'a> {
        Request {
            client,
            subject,
            payload: Some(payload),
            headers: None,
            timeout: None,
            inbox: None,
        }
    }

    /// Sets the payload of the request. If not used, empty payload will be sent.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// client.request("subject".into(), "data".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn payload(mut self, payload: Bytes) -> Request<'a> {
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
    ///
    /// client.request("subject".into(), "payload".into())
    ///     .headers(headers)
    ///     .await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn headers(mut self, headers: HeaderMap) -> Request<'a> {
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
    /// client.request("service".into(), "data".into())
    ///     .timeout(Some(std::time::Duration::from_secs(15)))
    ///     .await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn timeout(mut self, timeout: Option<Duration>) -> Request<'a> {
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
    /// client.request("subject".into(), "payload".into())
    ///     .inbox("custom_inbox".into())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn inbox(mut self, inbox: String) -> Request<'a> {
        self.inbox = Some(inbox);
        self
    }

    async fn send(self) -> Result<Message, Error> {
        let inbox = self.inbox.unwrap_or_else(|| self.client.new_inbox());
        let mut subscriber = self.client.subscribe(inbox.clone()).await?;
        let mut publish = self
            .client
            .publish(self.subject, self.payload.unwrap_or_else(Bytes::new));

        if let Some(headers) = self.headers {
            publish = publish.headers(headers);
        }

        publish = publish.reply(inbox);
        publish.into_future().await?;

        self.client.flush().await?;

        let period = self.timeout.unwrap_or(self.client.request_timeout);
        let message = match period {
            Some(period) => {
                tokio::time::timeout(period, subscriber.next())
                    .map_err(|_| std::io::Error::new(ErrorKind::TimedOut, "request timed out"))
                    .await?
            }
            None => subscriber.next().await,
        };

        match message {
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
}

impl<'a> IntoFuture for Request<'a> {
    type Output = Result<Message, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<Message, Error>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        self.send().boxed()
    }
}
