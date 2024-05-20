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

//! A Rust asynchronous client for the NATS.io ecosystem.
//!
//! To access the repository, you can clone it by running:
//!
//! ```bash
//! git clone https://github.com/nats-io/nats.rs
//! ````
//! NATS.io is a simple, secure, and high-performance open-source messaging
//! system designed for cloud-native applications, IoT messaging, and microservices
//! architectures.
//!
//! **Note**: The synchronous NATS API is deprecated and no longer actively maintained. If you need to use the deprecated synchronous API, you can refer to:
//! <https://crates.io/crates/nats>
//!
//! For more information on NATS.io visit: <https://nats.io>
//!
//! ## Examples
//!
//! Below, you can find some basic examples on how to use this library.
//!
//! For more details, please refer to the specific methods and structures documentation.
//!
//! ### Complete example
//!
//! Connect to the NATS server, publish messages and subscribe to receive messages.
//!
//! ```no_run
//! use bytes::Bytes;
//! use futures::StreamExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), async_nats::Error> {
//!     // Connect to the NATS server
//!     let client = async_nats::connect("demo.nats.io").await?;
//!
//!     // Subscribe to the "messages" subject
//!     let mut subscriber = client.subscribe("messages").await?;
//!
//!     // Publish messages to the "messages" subject
//!     for _ in 0..10 {
//!         client.publish("messages", "data".into()).await?;
//!     }
//!
//!     // Receive and process messages
//!     while let Some(message) = subscriber.next().await {
//!         println!("Received message {:?}", message);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Publish
//!
//! Connect to the NATS server and publish messages to a subject.
//!
//! ```
//! # use bytes::Bytes;
//! # use std::error::Error;
//! # use std::time::Instant;
//! # #[tokio::main]
//! # async fn main() -> Result<(), async_nats::Error> {
//! // Connect to the NATS server
//! let client = async_nats::connect("demo.nats.io").await?;
//!
//! // Prepare the subject and data
//! let subject = "foo";
//! let data = Bytes::from("bar");
//!
//! // Publish messages to the NATS server
//! for _ in 0..10 {
//!     client.publish(subject, data.clone()).await?;
//! }
//!
//! // Flush internal buffer before exiting to make sure all messages are sent
//! client.flush().await?;
//!
//! #    Ok(())
//! # }
//! ```
//!
//! ### Subscribe
//!
//! Connect to the NATS server, subscribe to a subject and receive messages.
//!
//! ```no_run
//! # use bytes::Bytes;
//! # use futures::StreamExt;
//! # use std::error::Error;
//! # use std::time::Instant;
//! # #[tokio::main]
//! # async fn main() -> Result<(), async_nats::Error> {
//! // Connect to the NATS server
//! let client = async_nats::connect("demo.nats.io").await?;
//!
//! // Subscribe to the "foo" subject
//! let mut subscriber = client.subscribe("foo").await.unwrap();
//!
//! // Receive and process messages
//! while let Some(message) = subscriber.next().await {
//!     println!("Received message {:?}", message);
//! }
//! #     Ok(())
//! # }
//! ```
//!
//! ### JetStream
//!
//! To access JetStream API, create a JetStream [jetstream::Context].
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), async_nats::Error> {
//! // Connect to the NATS server
//! let client = async_nats::connect("demo.nats.io").await?;
//! // Create a JetStream context.
//! let jetstream = async_nats::jetstream::new(client);
//!
//! // Publish JetStream messages, manage streams, consumers, etc.
//! jetstream.publish("foo", "bar".into()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Key-value Store
//!
//! Key-value [Store][jetstream::kv::Store] is accessed through [jetstream::Context].
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), async_nats::Error> {
//! // Connect to the NATS server
//! let client = async_nats::connect("demo.nats.io").await?;
//! // Create a JetStream context.
//! let jetstream = async_nats::jetstream::new(client);
//! // Access an existing key-value.
//! let kv = jetstream.get_key_value("store").await?;
//! # Ok(())
//! # }
//! ```
//! ### Object Store store
//!
//! Object [Store][jetstream::object_store::ObjectStore] is accessed through [jetstream::Context].
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), async_nats::Error> {
//! // Connect to the NATS server
//! let client = async_nats::connect("demo.nats.io").await?;
//! // Create a JetStream context.
//! let jetstream = async_nats::jetstream::new(client);
//! // Access an existing key-value.
//! let kv = jetstream.get_object_store("store").await?;
//! # Ok(())
//! # }
//! ```
//! ### Service API
//!
//! [Service API][service::Service] is accessible through [Client] after importing its trait.
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), async_nats::Error> {
//! use async_nats::service::ServiceExt;
//! // Connect to the NATS server
//! let client = async_nats::connect("demo.nats.io").await?;
//! let mut service = client
//!     .service_builder()
//!     .description("some service")
//!     .stats_handler(|endpoint, stats| serde_json::json!({ "endpoint": endpoint }))
//!     .start("products", "1.0.0")
//!     .await?;
//! # Ok(())
//! # }
//! ```

#![deny(unreachable_pub)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]
#![deny(rustdoc::invalid_codeblock_attributes)]
#![deny(rustdoc::invalid_rust_codeblocks)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

use thiserror::Error;

use futures::stream::Stream;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tracing::{debug, error};

use core::fmt;
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::iter;
use std::mem;
use std::net::SocketAddr;
use std::option;
use std::pin::Pin;
use std::slice;
use std::str::{self, FromStr};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::ErrorKind;
use tokio::time::{interval, Duration, Interval, MissedTickBehavior};
use url::{Host, Url};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use tokio::io;
use tokio::sync::mpsc;
use tokio::task;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LANG: &str = "rust";
const MAX_PENDING_PINGS: usize = 2;
const MULTIPLEXER_SID: u64 = 0;

/// A re-export of the `rustls` crate used in this crate,
/// for use in cases where manual client configurations
/// must be provided using `Options::tls_client_config`.
pub use tokio_rustls::rustls;

use connection::{Connection, State};
use connector::{Connector, ConnectorOptions};
pub use header::{HeaderMap, HeaderName, HeaderValue};
pub use subject::Subject;

mod auth;
pub(crate) mod auth_utils;
pub mod client;
pub mod connection;
mod connector;
mod options;

pub use auth::Auth;
pub use client::{Client, PublishError, Request, RequestError, RequestErrorKind, SubscribeError};
pub use options::{AuthError, ConnectOptions};

mod crypto;
pub mod error;
pub mod header;
pub mod jetstream;
pub mod message;
#[cfg(feature = "service")]
pub mod service;
pub mod status;
pub mod subject;
mod tls;

pub use message::Message;
pub use status::StatusCode;

/// Information sent by the server back to this client
/// during initial connection, and possibly again later.
#[derive(Debug, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct ServerInfo {
    /// The unique identifier of the NATS server.
    #[serde(default)]
    pub server_id: String,
    /// Generated Server Name.
    #[serde(default)]
    pub server_name: String,
    /// The host specified in the cluster parameter/options.
    #[serde(default)]
    pub host: String,
    /// The port number specified in the cluster parameter/options.
    #[serde(default)]
    pub port: u16,
    /// The version of the NATS server.
    #[serde(default)]
    pub version: String,
    /// If this is set, then the server should try to authenticate upon
    /// connect.
    #[serde(default)]
    pub auth_required: bool,
    /// If this is set, then the server must authenticate using TLS.
    #[serde(default)]
    pub tls_required: bool,
    /// Maximum payload size that the server will accept.
    #[serde(default)]
    pub max_payload: usize,
    /// The protocol version in use.
    #[serde(default)]
    pub proto: i8,
    /// The server-assigned client ID. This may change during reconnection.
    #[serde(default)]
    pub client_id: u64,
    /// The version of golang the NATS server was built with.
    #[serde(default)]
    pub go: String,
    /// The nonce used for nkeys.
    #[serde(default)]
    pub nonce: String,
    /// A list of server urls that a client can connect to.
    #[serde(default)]
    pub connect_urls: Vec<String>,
    /// The client IP as known by the server.
    #[serde(default)]
    pub client_ip: String,
    /// Whether the server supports headers.
    #[serde(default)]
    pub headers: bool,
    /// Whether server goes into lame duck mode.
    #[serde(default, rename = "ldm")]
    pub lame_duck_mode: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ServerOp {
    Ok,
    Info(Box<ServerInfo>),
    Ping,
    Pong,
    Error(ServerError),
    Message {
        sid: u64,
        subject: Subject,
        reply: Option<Subject>,
        payload: Bytes,
        headers: Option<HeaderMap>,
        status: Option<StatusCode>,
        description: Option<String>,
        length: usize,
    },
}

#[derive(Debug)]
pub(crate) enum Command {
    Publish {
        subject: Subject,
        payload: Bytes,
        respond: Option<Subject>,
        headers: Option<HeaderMap>,
    },
    Request {
        subject: Subject,
        payload: Bytes,
        respond: Subject,
        headers: Option<HeaderMap>,
        sender: oneshot::Sender<Message>,
    },
    Subscribe {
        sid: u64,
        subject: Subject,
        queue_group: Option<String>,
        sender: mpsc::Sender<Message>,
    },
    Unsubscribe {
        sid: u64,
        max: Option<u64>,
    },
    Flush {
        observer: oneshot::Sender<()>,
    },
    Reconnect,
}

/// `ClientOp` represents all actions of `Client`.
#[derive(Debug)]
pub(crate) enum ClientOp {
    Publish {
        subject: Subject,
        payload: Bytes,
        respond: Option<Subject>,
        headers: Option<HeaderMap>,
    },
    Subscribe {
        sid: u64,
        subject: Subject,
        queue_group: Option<String>,
    },
    Unsubscribe {
        sid: u64,
        max: Option<u64>,
    },
    Ping,
    Pong,
    Connect(ConnectInfo),
}

#[derive(Debug)]
struct Subscription {
    subject: Subject,
    sender: mpsc::Sender<Message>,
    queue_group: Option<String>,
    delivered: u64,
    max: Option<u64>,
}

#[derive(Debug)]
struct Multiplexer {
    subject: Subject,
    prefix: Subject,
    senders: HashMap<String, oneshot::Sender<Message>>,
}

/// A connection handler which facilitates communication from channels to a single shared connection.
pub(crate) struct ConnectionHandler {
    connection: Connection,
    connector: Connector,
    subscriptions: HashMap<u64, Subscription>,
    multiplexer: Option<Multiplexer>,
    pending_pings: usize,
    info_sender: tokio::sync::watch::Sender<ServerInfo>,
    ping_interval: Interval,
    should_reconnect: bool,
    flush_observers: Vec<oneshot::Sender<()>>,
}

impl ConnectionHandler {
    pub(crate) fn new(
        connection: Connection,
        connector: Connector,
        info_sender: tokio::sync::watch::Sender<ServerInfo>,
        ping_period: Duration,
    ) -> ConnectionHandler {
        let mut ping_interval = interval(ping_period);
        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        ConnectionHandler {
            connection,
            connector,
            subscriptions: HashMap::new(),
            multiplexer: None,
            pending_pings: 0,
            info_sender,
            ping_interval,
            should_reconnect: false,
            flush_observers: Vec::new(),
        }
    }

    pub(crate) async fn process<'a>(&'a mut self, receiver: &'a mut mpsc::Receiver<Command>) {
        struct ProcessFut<'a> {
            handler: &'a mut ConnectionHandler,
            receiver: &'a mut mpsc::Receiver<Command>,
            recv_buf: &'a mut Vec<Command>,
        }

        enum ExitReason {
            Disconnected(Option<io::Error>),
            ReconnectRequested,
            Closed,
        }

        impl<'a> ProcessFut<'a> {
            const RECV_CHUNK_SIZE: usize = 16;

            #[cold]
            fn ping(&mut self) -> Poll<ExitReason> {
                self.handler.pending_pings += 1;

                if self.handler.pending_pings > MAX_PENDING_PINGS {
                    debug!(
                        "pending pings {}, max pings {}. disconnecting",
                        self.handler.pending_pings, MAX_PENDING_PINGS
                    );

                    Poll::Ready(ExitReason::Disconnected(None))
                } else {
                    self.handler.connection.enqueue_write_op(&ClientOp::Ping);

                    Poll::Pending
                }
            }
        }

        impl<'a> Future for ProcessFut<'a> {
            type Output = ExitReason;

            /// Drives the connection forward.
            ///
            /// Returns one of the following:
            ///
            /// * `Poll::Pending` means that the connection
            ///   is blocked on all fronts or there are
            ///   no commands to send or receive
            /// * `Poll::Ready(ExitReason::Disconnected(_))` means
            ///   that an I/O operation failed and the connection
            ///   is considered dead.
            /// * `Poll::Ready(ExitReason::Closed)` means that
            ///   [`Self::receiver`] was closed, so there's nothing
            ///   more for us to do than to exit the client.
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                // We need to be sure the waker is registered, therefore we need to poll until we
                // get a `Poll::Pending`. With a sane interval delay, this means that the loop
                // breaks at the second iteration.
                while self.handler.ping_interval.poll_tick(cx).is_ready() {
                    if let Poll::Ready(exit) = self.ping() {
                        return Poll::Ready(exit);
                    }
                }

                loop {
                    match self.handler.connection.poll_read_op(cx) {
                        Poll::Pending => break,
                        Poll::Ready(Ok(Some(server_op))) => {
                            self.handler.handle_server_op(server_op);
                        }
                        Poll::Ready(Ok(None)) => {
                            return Poll::Ready(ExitReason::Disconnected(None))
                        }
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(ExitReason::Disconnected(Some(err)))
                        }
                    }
                }

                // WARNING: after the following loop `handle_command`,
                // or other functions which call `enqueue_write_op`,
                // cannot be called anymore. Runtime wakeups won't
                // trigger a call to `poll_write`

                let mut made_progress = true;
                loop {
                    while !self.handler.connection.is_write_buf_full() {
                        debug_assert!(self.recv_buf.is_empty());

                        let Self {
                            recv_buf,
                            handler,
                            receiver,
                        } = &mut *self;
                        match receiver.poll_recv_many(cx, recv_buf, Self::RECV_CHUNK_SIZE) {
                            Poll::Pending => break,
                            Poll::Ready(1..) => {
                                made_progress = true;

                                for cmd in recv_buf.drain(..) {
                                    handler.handle_command(cmd);
                                }
                            }
                            // TODO: replace `_` with `0` after bumping MSRV to 1.75
                            Poll::Ready(_) => return Poll::Ready(ExitReason::Closed),
                        }
                    }

                    // The first round will poll both from
                    // the `receiver` and the writer, giving
                    // them both a chance to make progress
                    // and register `Waker`s.
                    //
                    // If writing is `Poll::Pending` we exit.
                    //
                    // If writing is completed we can repeat the entire
                    // cycle as long as the `receiver` doesn't end-up
                    // `Poll::Pending` immediately.
                    if !mem::take(&mut made_progress) {
                        break;
                    }

                    match self.handler.connection.poll_write(cx) {
                        Poll::Pending => {
                            // Write buffer couldn't be fully emptied
                            break;
                        }
                        Poll::Ready(Ok(())) => {
                            // Write buffer is empty
                            continue;
                        }
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(ExitReason::Disconnected(Some(err)))
                        }
                    }
                }

                if let (ShouldFlush::Yes, _) | (ShouldFlush::No, false) = (
                    self.handler.connection.should_flush(),
                    self.handler.flush_observers.is_empty(),
                ) {
                    match self.handler.connection.poll_flush(cx) {
                        Poll::Pending => {}
                        Poll::Ready(Ok(())) => {
                            for observer in self.handler.flush_observers.drain(..) {
                                let _ = observer.send(());
                            }
                        }
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(ExitReason::Disconnected(Some(err)))
                        }
                    }
                }

                if mem::take(&mut self.handler.should_reconnect) {
                    return Poll::Ready(ExitReason::ReconnectRequested);
                }

                Poll::Pending
            }
        }

        let mut recv_buf = Vec::with_capacity(ProcessFut::RECV_CHUNK_SIZE);
        loop {
            let process = ProcessFut {
                handler: self,
                receiver,
                recv_buf: &mut recv_buf,
            };
            match process.await {
                ExitReason::Disconnected(err) => {
                    debug!(?err, "disconnected");
                    if self.handle_disconnect().await.is_err() {
                        break;
                    };
                    debug!("reconnected");
                }
                ExitReason::Closed => break,
                ExitReason::ReconnectRequested => {
                    debug!("reconnect requested");
                    // Should be ok to ingore error, as that means we are not in connected state.
                    self.connection.stream.shutdown().await.ok();
                    if self.handle_disconnect().await.is_err() {
                        break;
                    };
                }
            }
        }
    }

    fn handle_server_op(&mut self, server_op: ServerOp) {
        self.ping_interval.reset();

        match server_op {
            ServerOp::Ping => {
                self.connection.enqueue_write_op(&ClientOp::Pong);
            }
            ServerOp::Pong => {
                debug!("received PONG");
                self.pending_pings = self.pending_pings.saturating_sub(1);
            }
            ServerOp::Error(error) => {
                self.connector
                    .events_tx
                    .try_send(Event::ServerError(error))
                    .ok();
            }
            ServerOp::Message {
                sid,
                subject,
                reply,
                payload,
                headers,
                status,
                description,
                length,
            } => {
                if let Some(subscription) = self.subscriptions.get_mut(&sid) {
                    let message: Message = Message {
                        subject,
                        reply,
                        payload,
                        headers,
                        status,
                        description,
                        length,
                    };

                    // if the channel for subscription was dropped, remove the
                    // subscription from the map and unsubscribe.
                    match subscription.sender.try_send(message) {
                        Ok(_) => {
                            subscription.delivered += 1;
                            // if this `Subscription` has set `max` value, check if it
                            // was reached. If yes, remove the `Subscription` and in
                            // the result, `drop` the `sender` channel.
                            if let Some(max) = subscription.max {
                                if subscription.delivered.ge(&max) {
                                    self.subscriptions.remove(&sid);
                                }
                            }
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            self.connector
                                .events_tx
                                .try_send(Event::SlowConsumer(sid))
                                .ok();
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            self.subscriptions.remove(&sid);
                            self.connection
                                .enqueue_write_op(&ClientOp::Unsubscribe { sid, max: None });
                        }
                    }
                } else if sid == MULTIPLEXER_SID {
                    if let Some(multiplexer) = self.multiplexer.as_mut() {
                        let maybe_token =
                            subject.strip_prefix(multiplexer.prefix.as_ref()).to_owned();

                        if let Some(token) = maybe_token {
                            if let Some(sender) = multiplexer.senders.remove(token) {
                                let message = Message {
                                    subject,
                                    reply,
                                    payload,
                                    headers,
                                    status,
                                    description,
                                    length,
                                };

                                let _ = sender.send(message);
                            }
                        }
                    }
                }
            }
            // TODO: we should probably update advertised server list here too.
            ServerOp::Info(info) => {
                if info.lame_duck_mode {
                    self.connector.events_tx.try_send(Event::LameDuckMode).ok();
                }
            }

            _ => {
                // TODO: don't ignore.
            }
        }
    }

    fn handle_command(&mut self, command: Command) {
        self.ping_interval.reset();

        match command {
            Command::Unsubscribe { sid, max } => {
                if let Some(subscription) = self.subscriptions.get_mut(&sid) {
                    subscription.max = max;
                    match subscription.max {
                        Some(n) => {
                            if subscription.delivered >= n {
                                self.subscriptions.remove(&sid);
                            }
                        }
                        None => {
                            self.subscriptions.remove(&sid);
                        }
                    }

                    self.connection
                        .enqueue_write_op(&ClientOp::Unsubscribe { sid, max });
                }
            }
            Command::Flush { observer } => {
                self.flush_observers.push(observer);
            }
            Command::Subscribe {
                sid,
                subject,
                queue_group,
                sender,
            } => {
                let subscription = Subscription {
                    sender,
                    delivered: 0,
                    max: None,
                    subject: subject.to_owned(),
                    queue_group: queue_group.to_owned(),
                };

                self.subscriptions.insert(sid, subscription);

                self.connection.enqueue_write_op(&ClientOp::Subscribe {
                    sid,
                    subject,
                    queue_group,
                });
            }
            Command::Request {
                subject,
                payload,
                respond,
                headers,
                sender,
            } => {
                let (prefix, token) = respond.rsplit_once('.').expect("malformed request subject");

                let multiplexer = if let Some(multiplexer) = self.multiplexer.as_mut() {
                    multiplexer
                } else {
                    let prefix = Subject::from(format!("{}.{}.", prefix, nuid::next()));
                    let subject = Subject::from(format!("{}*", prefix));

                    self.connection.enqueue_write_op(&ClientOp::Subscribe {
                        sid: MULTIPLEXER_SID,
                        subject: subject.clone(),
                        queue_group: None,
                    });

                    self.multiplexer.insert(Multiplexer {
                        subject,
                        prefix,
                        senders: HashMap::new(),
                    })
                };

                multiplexer.senders.insert(token.to_owned(), sender);

                let pub_op = ClientOp::Publish {
                    subject,
                    payload,
                    respond: Some(format!("{}{}", multiplexer.prefix, token).into()),
                    headers,
                };

                self.connection.enqueue_write_op(&pub_op);
            }

            Command::Publish {
                subject,
                payload,
                respond,
                headers,
            } => {
                self.connection.enqueue_write_op(&ClientOp::Publish {
                    subject,
                    payload,
                    respond,
                    headers,
                });
            }

            Command::Reconnect => {
                self.should_reconnect = true;
            }
        }
    }

    async fn handle_disconnect(&mut self) -> Result<(), ConnectError> {
        self.pending_pings = 0;
        self.connector.events_tx.try_send(Event::Disconnected).ok();
        self.connector.state_tx.send(State::Disconnected).ok();

        self.handle_reconnect().await
    }

    async fn handle_reconnect(&mut self) -> Result<(), ConnectError> {
        let (info, connection) = self.connector.connect().await?;
        self.connection = connection;
        let _ = self.info_sender.send(info);

        self.subscriptions
            .retain(|_, subscription| !subscription.sender.is_closed());

        for (sid, subscription) in &self.subscriptions {
            self.connection.enqueue_write_op(&ClientOp::Subscribe {
                sid: *sid,
                subject: subscription.subject.to_owned(),
                queue_group: subscription.queue_group.to_owned(),
            });
        }

        if let Some(multiplexer) = &self.multiplexer {
            self.connection.enqueue_write_op(&ClientOp::Subscribe {
                sid: MULTIPLEXER_SID,
                subject: multiplexer.subject.to_owned(),
                queue_group: None,
            });
        }
        Ok(())
    }
}

/// Connects to NATS with specified options.
///
/// It is generally advised to use [ConnectOptions] instead, as it provides a builder for whole
/// configuration.
///
/// # Examples
/// ```
/// # #[tokio::main]
/// # async fn main() ->  Result<(), async_nats::Error> {
/// let mut nc =
///     async_nats::connect_with_options("demo.nats.io", async_nats::ConnectOptions::new()).await?;
/// nc.publish("test", "data".into()).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_options<A: ToServerAddrs>(
    addrs: A,
    options: ConnectOptions,
) -> Result<Client, ConnectError> {
    let ping_period = options.ping_interval;

    let (events_tx, mut events_rx) = mpsc::channel(128);
    let (state_tx, state_rx) = tokio::sync::watch::channel(State::Pending);
    // We're setting it to the default server payload size.
    let max_payload = Arc::new(AtomicUsize::new(1024 * 1024));

    let mut connector = Connector::new(
        addrs,
        ConnectorOptions {
            tls_required: options.tls_required,
            certificates: options.certificates,
            client_key: options.client_key,
            client_cert: options.client_cert,
            tls_client_config: options.tls_client_config,
            tls_first: options.tls_first,
            auth: options.auth,
            no_echo: options.no_echo,
            connection_timeout: options.connection_timeout,
            name: options.name,
            ignore_discovered_servers: options.ignore_discovered_servers,
            retain_servers_order: options.retain_servers_order,
            read_buffer_capacity: options.read_buffer_capacity,
            reconnect_delay_callback: options.reconnect_delay_callback,
            auth_callback: options.auth_callback,
            max_reconnects: options.max_reconnects,
        },
        events_tx,
        state_tx,
        max_payload.clone(),
    )
    .map_err(|err| ConnectError::with_source(ConnectErrorKind::ServerParse, err))?;

    let mut info: ServerInfo = Default::default();
    let mut connection = None;
    if !options.retry_on_initial_connect {
        debug!("retry on initial connect failure is disabled");
        let (info_ok, connection_ok) = connector.try_connect().await?;
        connection = Some(connection_ok);
        info = info_ok;
    }

    let (info_sender, info_watcher) = tokio::sync::watch::channel(info.clone());
    let (sender, mut receiver) = mpsc::channel(options.sender_capacity);

    let client = Client::new(
        info_watcher,
        state_rx,
        sender,
        options.subscription_capacity,
        options.inbox_prefix,
        options.request_timeout,
        max_payload,
    );

    task::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            tracing::info!("event: {}", event);
            if let Some(event_callback) = &options.event_callback {
                event_callback.call(event).await;
            }
        }
    });

    task::spawn(async move {
        if connection.is_none() && options.retry_on_initial_connect {
            let (info, connection_ok) = match connector.connect().await {
                Ok((info, connection)) => (info, connection),
                Err(err) => {
                    error!("connection closed: {}", err);
                    return;
                }
            };
            info_sender.send(info).ok();
            connection = Some(connection_ok);
        }
        let connection = connection.unwrap();
        let mut connection_handler =
            ConnectionHandler::new(connection, connector, info_sender, ping_period);
        connection_handler.process(&mut receiver).await
    });

    Ok(client)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Connected,
    Disconnected,
    LameDuckMode,
    SlowConsumer(u64),
    ServerError(ServerError),
    ClientError(ClientError),
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Connected => write!(f, "connected"),
            Event::Disconnected => write!(f, "disconnected"),
            Event::LameDuckMode => write!(f, "lame duck mode detected"),
            Event::SlowConsumer(sid) => write!(f, "slow consumers for subscription {sid}"),
            Event::ServerError(err) => write!(f, "server error: {err}"),
            Event::ClientError(err) => write!(f, "client error: {err}"),
        }
    }
}

/// Connects to NATS with default config.
///
/// Returns cloneable [Client].
///
/// To have customized NATS connection, check [ConnectOptions].
///
/// # Examples
///
/// ## Single URL
/// ```
/// # #[tokio::main]
/// # async fn main() ->  Result<(), async_nats::Error> {
/// let mut nc = async_nats::connect("demo.nats.io").await?;
/// nc.publish("test", "data".into()).await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Connect with [Vec] of [ServerAddr].
/// ```no_run
/// #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// use async_nats::ServerAddr;
/// let client = async_nats::connect(vec![
///     "demo.nats.io".parse::<ServerAddr>()?,
///     "other.nats.io".parse::<ServerAddr>()?,
/// ])
/// .await
/// .unwrap();
/// # Ok(())
/// # }
/// ```
///
/// ## with [Vec], but parse URLs inside [crate::connect()]
/// ```no_run
/// #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// use async_nats::ServerAddr;
/// let servers = vec!["demo.nats.io", "other.nats.io"];
/// let client = async_nats::connect(
///     servers
///         .iter()
///         .map(|url| url.parse())
///         .collect::<Result<Vec<ServerAddr>, _>>()?,
/// )
/// .await?;
/// # Ok(())
/// # }
/// ```
///
///
/// ## with slice.
/// ```no_run
/// #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// use async_nats::ServerAddr;
/// let client = async_nats::connect(
///    [
///        "demo.nats.io".parse::<ServerAddr>()?,
///        "other.nats.io".parse::<ServerAddr>()?,
///    ]
///    .as_slice(),
/// )
/// .await?;
/// # Ok(())
/// # }
pub async fn connect<A: ToServerAddrs>(addrs: A) -> Result<Client, ConnectError> {
    connect_with_options(addrs, ConnectOptions::default()).await
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectErrorKind {
    /// Parsing the passed server address failed.
    ServerParse,
    /// DNS related issues.
    Dns,
    /// Failed authentication process, signing nonce, etc.
    Authentication,
    /// Server returned authorization violation error.
    AuthorizationViolation,
    /// Connect timed out.
    TimedOut,
    /// Erroneous TLS setup.
    Tls,
    /// Other IO error.
    Io,
    /// Reached the maximum number of reconnects.
    MaxReconnects,
}

impl Display for ConnectErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServerParse => write!(f, "failed to parse server or server list"),
            Self::Dns => write!(f, "DNS error"),
            Self::Authentication => write!(f, "failed signing nonce"),
            Self::AuthorizationViolation => write!(f, "authorization violation"),
            Self::TimedOut => write!(f, "timed out"),
            Self::Tls => write!(f, "TLS error"),
            Self::Io => write!(f, "IO error"),
            Self::MaxReconnects => write!(f, "reached maximum number of reconnects"),
        }
    }
}

/// Returned when initial connection fails.
/// To be enumerate over the variants, call [ConnectError::kind].
pub type ConnectError = error::Error<ConnectErrorKind>;

impl From<io::Error> for ConnectError {
    fn from(err: io::Error) -> Self {
        ConnectError::with_source(ConnectErrorKind::Io, err)
    }
}

/// Retrieves messages from given `subscription` created by [Client::subscribe].
///
/// Implements [futures::stream::Stream] for ergonomic async message processing.
///
/// # Examples
/// ```
/// # #[tokio::main]
/// # async fn main() ->  Result<(), async_nats::Error> {
/// let mut nc = async_nats::connect("demo.nats.io").await?;
/// # nc.publish("test", "data".into()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Subscriber {
    sid: u64,
    receiver: mpsc::Receiver<Message>,
    sender: mpsc::Sender<Command>,
}

impl Subscriber {
    fn new(
        sid: u64,
        sender: mpsc::Sender<Command>,
        receiver: mpsc::Receiver<Message>,
    ) -> Subscriber {
        Subscriber {
            sid,
            sender,
            receiver,
        }
    }

    /// Unsubscribes from subscription, draining all remaining messages.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    ///
    /// let mut subscriber = client.subscribe("foo").await?;
    ///
    /// subscriber.unsubscribe().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unsubscribe(&mut self) -> Result<(), UnsubscribeError> {
        self.sender
            .send(Command::Unsubscribe {
                sid: self.sid,
                max: None,
            })
            .await?;
        self.receiver.close();
        Ok(())
    }

    /// Unsubscribes from subscription after reaching given number of messages.
    /// This is the total number of messages received by this subscription in it's whole
    /// lifespan. If it already reached or surpassed the passed value, it will immediately stop.
    ///
    /// # Examples
    /// ```
    /// # use futures::StreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    ///
    /// let mut subscriber = client.subscribe("test").await?;
    /// subscriber.unsubscribe_after(3).await?;
    ///
    /// for _ in 0..3 {
    ///     client.publish("test", "data".into()).await?;
    /// }
    ///
    /// while let Some(message) = subscriber.next().await {
    ///     println!("message received: {:?}", message);
    /// }
    /// println!("no more messages, unsubscribed");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unsubscribe_after(&mut self, unsub_after: u64) -> Result<(), UnsubscribeError> {
        self.sender
            .send(Command::Unsubscribe {
                sid: self.sid,
                max: Some(unsub_after),
            })
            .await?;
        Ok(())
    }
}

#[derive(Error, Debug, PartialEq)]
#[error("failed to send unsubscribe")]
pub struct UnsubscribeError(String);

impl From<tokio::sync::mpsc::error::SendError<Command>> for UnsubscribeError {
    fn from(err: tokio::sync::mpsc::error::SendError<Command>) -> Self {
        UnsubscribeError(err.to_string())
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.receiver.close();
        tokio::spawn({
            let sender = self.sender.clone();
            let sid = self.sid;
            async move {
                sender
                    .send(Command::Unsubscribe { sid, max: None })
                    .await
                    .ok();
            }
        });
    }
}

impl Stream for Subscriber {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallbackError {
    Client(ClientError),
    Server(ServerError),
}
impl std::fmt::Display for CallbackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client(error) => write!(f, "{error}"),
            Self::Server(error) => write!(f, "{error}"),
        }
    }
}

impl From<ServerError> for CallbackError {
    fn from(server_error: ServerError) -> Self {
        CallbackError::Server(server_error)
    }
}

impl From<ClientError> for CallbackError {
    fn from(client_error: ClientError) -> Self {
        CallbackError::Client(client_error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum ServerError {
    AuthorizationViolation,
    SlowConsumer(u64),
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientError {
    Other(String),
    MaxReconnects,
}
impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(error) => write!(f, "nats: {error}"),
            Self::MaxReconnects => write!(f, "nats: max reconnects reached"),
        }
    }
}

impl ServerError {
    fn new(error: String) -> ServerError {
        match error.to_lowercase().as_str() {
            "authorization violation" => ServerError::AuthorizationViolation,
            // error messages can contain case-sensitive values which should be preserved
            _ => ServerError::Other(error),
        }
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AuthorizationViolation => write!(f, "nats: authorization violation"),
            Self::SlowConsumer(sid) => write!(f, "nats: subscription {sid} is a slow consumer"),
            Self::Other(error) => write!(f, "nats: {error}"),
        }
    }
}

/// Info to construct a CONNECT message.
#[derive(Clone, Debug, Serialize)]
pub struct ConnectInfo {
    /// Turns on +OK protocol acknowledgments.
    pub verbose: bool,

    /// Turns on additional strict format checking, e.g. for properly formed
    /// subjects.
    pub pedantic: bool,

    /// User's JWT.
    #[serde(rename = "jwt")]
    pub user_jwt: Option<String>,

    /// Public nkey.
    pub nkey: Option<String>,

    /// Signed nonce, encoded to Base64URL.
    #[serde(rename = "sig")]
    pub signature: Option<String>,

    /// Optional client name.
    pub name: Option<String>,

    /// If set to `true`, the server (version 1.2.0+) will not send originating
    /// messages from this connection to its own subscriptions. Clients should
    /// set this to `true` only for server supporting this feature, which is
    /// when proto in the INFO protocol is set to at least 1.
    pub echo: bool,

    /// The implementation language of the client.
    pub lang: String,

    /// The version of the client.
    pub version: String,

    /// Sending 0 (or absent) indicates client supports original protocol.
    /// Sending 1 indicates that the client supports dynamic reconfiguration
    /// of cluster topology changes by asynchronously receiving INFO messages
    /// with known servers it can reconnect to.
    pub protocol: Protocol,

    /// Indicates whether the client requires an SSL connection.
    pub tls_required: bool,

    /// Connection username (if `auth_required` is set)
    pub user: Option<String>,

    /// Connection password (if auth_required is set)
    pub pass: Option<String>,

    /// Client authorization token (if auth_required is set)
    pub auth_token: Option<String>,

    /// Whether the client supports the usage of headers.
    pub headers: bool,

    /// Whether the client supports no_responders.
    pub no_responders: bool,
}

/// Protocol version used by the client.
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Eq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum Protocol {
    /// Original protocol.
    Original = 0,
    /// Protocol with dynamic reconfiguration of cluster and lame duck mode functionality.
    Dynamic = 1,
}

/// Address of a NATS server.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServerAddr(Url);

impl FromStr for ServerAddr {
    type Err = io::Error;

    /// Parse an address of a NATS server.
    ///
    /// If not stated explicitly the `nats://` schema and port `4222` is assumed.
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let url: Url = if input.contains("://") {
            input.parse()
        } else {
            format!("nats://{input}").parse()
        }
        .map_err(|e| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!("NATS server URL is invalid: {e}"),
            )
        })?;

        Self::from_url(url)
    }
}

impl ServerAddr {
    /// Check if the URL is a valid NATS server address.
    pub fn from_url(url: Url) -> io::Result<Self> {
        if url.scheme() != "nats" && url.scheme() != "tls" {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("invalid scheme for NATS server URL: {}", url.scheme()),
            ));
        }

        Ok(Self(url))
    }

    /// Turn the server address into a standard URL.
    pub fn into_inner(self) -> Url {
        self.0
    }

    /// Returns if tls is required by the client for this server.
    pub fn tls_required(&self) -> bool {
        self.0.scheme() == "tls"
    }

    /// Returns if the server url had embedded username and password.
    pub fn has_user_pass(&self) -> bool {
        self.0.username() != ""
    }

    /// Returns the host.
    pub fn host(&self) -> &str {
        match self.0.host() {
            Some(Host::Domain(_)) | Some(Host::Ipv4 { .. }) => self.0.host_str().unwrap(),
            // `host_str()` for Ipv6 includes the []s
            Some(Host::Ipv6 { .. }) => {
                let host = self.0.host_str().unwrap();
                &host[1..host.len() - 1]
            }
            None => "",
        }
    }

    /// Returns the port.
    pub fn port(&self) -> u16 {
        self.0.port().unwrap_or(4222)
    }

    /// Returns the optional username in the url.
    pub fn username(&self) -> Option<&str> {
        let user = self.0.username();
        if user.is_empty() {
            None
        } else {
            Some(user)
        }
    }

    /// Returns the optional password in the url.
    pub fn password(&self) -> Option<&str> {
        self.0.password()
    }

    /// Return the sockets from resolving the server address.
    pub async fn socket_addrs(&self) -> io::Result<impl Iterator<Item = SocketAddr> + '_> {
        tokio::net::lookup_host((self.host(), self.port())).await
    }
}

/// Capability to convert into a list of NATS server addresses.
///
/// There are several implementations ensuring the easy passing of one or more server addresses to
/// functions like [`crate::connect()`].
pub trait ToServerAddrs {
    /// Returned iterator over socket addresses which this type may correspond
    /// to.
    type Iter: Iterator<Item = ServerAddr>;

    fn to_server_addrs(&self) -> io::Result<Self::Iter>;
}

impl ToServerAddrs for ServerAddr {
    type Iter = option::IntoIter<ServerAddr>;
    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        Ok(Some(self.clone()).into_iter())
    }
}

impl ToServerAddrs for str {
    type Iter = option::IntoIter<ServerAddr>;
    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        self.parse::<ServerAddr>()
            .map(|addr| Some(addr).into_iter())
    }
}

impl ToServerAddrs for String {
    type Iter = option::IntoIter<ServerAddr>;
    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        (**self).to_server_addrs()
    }
}

impl<T: AsRef<str>> ToServerAddrs for [T] {
    type Iter = std::vec::IntoIter<ServerAddr>;
    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        self.iter()
            .map(AsRef::as_ref)
            .map(str::parse)
            .collect::<io::Result<_>>()
            .map(Vec::into_iter)
    }
}

impl<T: AsRef<str>> ToServerAddrs for Vec<T> {
    type Iter = std::vec::IntoIter<ServerAddr>;
    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        self.as_slice().to_server_addrs()
    }
}

impl<'a> ToServerAddrs for &'a [ServerAddr] {
    type Iter = iter::Cloned<slice::Iter<'a, ServerAddr>>;

    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        Ok(self.iter().cloned())
    }
}

impl ToServerAddrs for Vec<ServerAddr> {
    type Iter = std::vec::IntoIter<ServerAddr>;

    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        Ok(self.clone().into_iter())
    }
}

impl<T: ToServerAddrs + ?Sized> ToServerAddrs for &T {
    type Iter = T::Iter;
    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        (**self).to_server_addrs()
    }
}

pub(crate) fn is_valid_subject<T: AsRef<str>>(subject: T) -> bool {
    !subject.as_ref().contains([' ', '.', '\r', '\n'])
}

macro_rules! from_with_timeout {
    ($t:ty, $k:ty, $origin: ty, $origin_kind: ty) => {
        impl From<$origin> for $t {
            fn from(err: $origin) -> Self {
                match err.kind() {
                    <$origin_kind>::TimedOut => Self::new(<$k>::TimedOut),
                    _ => Self::with_source(<$k>::Other, err),
                }
            }
        }
    };
}
pub(crate) use from_with_timeout;

use crate::connection::ShouldFlush;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_address_ipv6() {
        let address = ServerAddr::from_str("nats://[::]").unwrap();
        assert_eq!(address.host(), "::")
    }

    #[test]
    fn server_address_ipv4() {
        let address = ServerAddr::from_str("nats://127.0.0.1").unwrap();
        assert_eq!(address.host(), "127.0.0.1")
    }

    #[test]
    fn server_address_domain() {
        let address = ServerAddr::from_str("nats://example.com").unwrap();
        assert_eq!(address.host(), "example.com")
    }

    #[test]
    fn to_server_addrs_vec_str() {
        let vec = vec!["nats://127.0.0.1", "nats://[::]"];
        let mut addrs_iter = vec.to_server_addrs().unwrap();
        assert_eq!(addrs_iter.next().unwrap().host(), "127.0.0.1");
        assert_eq!(addrs_iter.next().unwrap().host(), "::");
        assert_eq!(addrs_iter.next(), None);
    }

    #[test]
    fn to_server_addrs_arr_str() {
        let arr = ["nats://127.0.0.1", "nats://[::]"];
        let mut addrs_iter = arr.to_server_addrs().unwrap();
        assert_eq!(addrs_iter.next().unwrap().host(), "127.0.0.1");
        assert_eq!(addrs_iter.next().unwrap().host(), "::");
        assert_eq!(addrs_iter.next(), None);
    }

    #[test]
    fn to_server_addrs_vec_string() {
        let vec = vec!["nats://127.0.0.1".to_string(), "nats://[::]".to_string()];
        let mut addrs_iter = vec.to_server_addrs().unwrap();
        assert_eq!(addrs_iter.next().unwrap().host(), "127.0.0.1");
        assert_eq!(addrs_iter.next().unwrap().host(), "::");
        assert_eq!(addrs_iter.next(), None);
    }

    #[test]
    fn to_server_addrs_arr_string() {
        let arr = ["nats://127.0.0.1".to_string(), "nats://[::]".to_string()];
        let mut addrs_iter = arr.to_server_addrs().unwrap();
        assert_eq!(addrs_iter.next().unwrap().host(), "127.0.0.1");
        assert_eq!(addrs_iter.next().unwrap().host(), "::");
        assert_eq!(addrs_iter.next(), None);
    }
}
