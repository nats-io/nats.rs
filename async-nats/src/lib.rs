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

//! A Rust async bleeding edge client for the NATS.io ecosystem.
//!
//! `git clone https://github.com/nats-io/nats.rs`
//!
//! NATS.io is a simple, secure and high performance open source messaging
//! system for cloud native applications, `IoT` messaging, and microservices
//! architectures.
//!
//! For sync API refer  <https://crates.io/crates/nats>
//!
//! For more information see [https://nats.io/].
//!
//! [https://nats.io/]: https://nats.io/
//!
//! ## Examples
//!
//! Below you can find some basic examples how to use this library.
//!
//! For details, refer docs for specific method/struct.
//!
//! ### Complete example
//!
//! ```
//! use bytes::Bytes;
//! use futures::StreamExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), async_nats::Error> {
//!     let client = async_nats::connect("demo.nats.io").await?;
//!     let mut subscriber = client.subscribe("messages".into()).await?.take(10);
//!
//!     for _ in 0..10 {
//!         client.publish("messages".into(), "data".into()).await?;
//!     }
//!
//!     while let Some(message) = subscriber.next().await {
//!       println!("Received message {:?}", message);
//!     }
//!
//!     Ok(())
//! }
//!
//! ```
//!
//! ### Publish
//!
//! ```
//! # use bytes::Bytes;
//! # use std::error::Error;
//! # use std::time::Instant;
//! # #[tokio::main]
//! # async fn main() -> Result<(), async_nats::Error> {
//! let client = async_nats::connect("demo.nats.io").await?;
//!
//! let subject = String::from("foo");
//! let data = Bytes::from("bar");
//! for _ in 0..10 {
//!     client.publish("subject".into(), "data".into()).await?;
//! }
//! #    Ok(())
//! # }
//! ```
//!
//! ### Subscribe
//!
//! ```no_run
//! # use bytes::Bytes;
//! # use futures::StreamExt;
//! # use std::error::Error;
//! # use std::time::Instant;
//! # #[tokio::main]
//! # async fn main() -> Result<(), async_nats::Error> {
//! let client = async_nats::connect("demo.nats.io").await?;
//!
//! let mut subscriber = client.subscribe("foo".into()).await.unwrap();
//!
//! while let Some(message) = subscriber.next().await {
//!     println!("Received message {:?}", message);
//! }
//! #     Ok(())
//! # }
//! ```

#![deny(unreachable_pub)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]
#![deny(rustdoc::invalid_codeblock_attributes)]
#![deny(rustdoc::invalid_rust_codeblocks)]

use thiserror::Error;

use futures::future::FutureExt;
use futures::select;
use futures::stream::Stream;
use tracing::{debug, error};

use core::fmt;
use std::collections::HashMap;
use std::fmt::Display;
use std::iter;
use std::net::{SocketAddr, ToSocketAddrs};
use std::option;
use std::pin::Pin;
use std::slice;
use std::str::{self, FromStr};
use std::task::{Context, Poll};
use tokio::io::ErrorKind;
use tokio::time::{interval, Duration, Interval, MissedTickBehavior};
use url::{Host, Url};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use tokio::io;
use tokio::sync::{mpsc, oneshot};
use tokio::task;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LANG: &str = "rust";

/// A re-export of the `rustls` crate used in this crate,
/// for use in cases where manual client configurations
/// must be provided using `Options::tls_client_config`.
pub use tokio_rustls::rustls;

use connection::{Connection, State};
use connector::{Connector, ConnectorOptions};
pub use header::{HeaderMap, HeaderName, HeaderValue};

pub(crate) mod auth_utils;
mod client;
pub mod connection;
mod connector;
mod options;

use crate::options::CallbackArg1;
pub use client::{Client, PublishError, Request, RequestError, RequestErrorKind};
pub use options::{AuthError, ConnectOptions};

pub mod header;
pub mod jetstream;
pub mod message;
#[cfg(feature = "service")]
pub mod service;
pub mod status;
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
        subject: String,
        reply: Option<String>,
        payload: Bytes,
        headers: Option<HeaderMap>,
        status: Option<StatusCode>,
        description: Option<String>,
        length: usize,
    },
}

#[derive(Debug)]
pub enum Command {
    Publish {
        subject: String,
        payload: Bytes,
        respond: Option<String>,
        headers: Option<HeaderMap>,
    },
    Subscribe {
        sid: u64,
        subject: String,
        queue_group: Option<String>,
        sender: mpsc::Sender<Message>,
    },
    Unsubscribe {
        sid: u64,
        max: Option<u64>,
    },
    Ping,
    Flush {
        result: oneshot::Sender<Result<(), io::Error>>,
    },
    TryFlush,
    Connect(ConnectInfo),
}

/// `ClientOp` represents all actions of `Client`.
#[derive(Debug)]
pub enum ClientOp {
    Publish {
        subject: String,
        payload: Bytes,
        respond: Option<String>,
        headers: Option<HeaderMap>,
    },
    Subscribe {
        sid: u64,
        subject: String,
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
    subject: String,
    sender: mpsc::Sender<Message>,
    queue_group: Option<String>,
    delivered: u64,
    max: Option<u64>,
}

/// A connection handler which facilitates communication from channels to a single shared connection.
pub(crate) struct ConnectionHandler {
    connection: Connection,
    connector: Connector,
    subscriptions: HashMap<u64, Subscription>,
    pending_pings: usize,
    max_pings: usize,
    info_sender: tokio::sync::watch::Sender<ServerInfo>,
    ping_interval: Interval,
    flush_interval: Interval,
}

impl ConnectionHandler {
    pub(crate) fn new(
        connection: Connection,
        connector: Connector,
        info_sender: tokio::sync::watch::Sender<ServerInfo>,
        ping_period: Duration,
        flush_period: Duration,
    ) -> ConnectionHandler {
        let mut ping_interval = interval(ping_period);
        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut flush_interval = interval(flush_period);
        flush_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        ConnectionHandler {
            connection,
            connector,
            subscriptions: HashMap::new(),
            pending_pings: 0,
            max_pings: 2,
            info_sender,
            ping_interval,
            flush_interval,
        }
    }

    pub(crate) async fn process(
        &mut self,
        mut receiver: mpsc::Receiver<Command>,
    ) -> Result<(), io::Error> {
        loop {
            select! {
                _ = self.ping_interval.tick().fuse() => {
                    self.pending_pings += 1;
                    if let Err(_err) = self.connection.write_op(&ClientOp::Ping).await {
                        self.handle_disconnect().await?;
                    }

                    self.handle_flush().await?;

                },
                _ = self.flush_interval.tick().fuse() => {
                    if let Err(_err) = self.handle_flush().await {
                        self.handle_disconnect().await?;
                    }
                },
                maybe_command = receiver.recv().fuse() => {
                    match maybe_command {
                        Some(command) => if let Err(err) = self.handle_command(command).await {
                            error!("error handling command {}", err);
                        }
                        None => {
                            break;
                        }
                    }
                }

                maybe_op_result = self.connection.read_op().fuse() => {
                    match maybe_op_result {
                        Ok(Some(server_op)) => if let Err(err) = self.handle_server_op(server_op).await {
                            error!("error handling operation {}", err);
                        }
                        Ok(None) => {
                            if let Err(err) = self.handle_disconnect().await {
                                error!("error handling operation {}", err);
                            }
                        }
                        Err(op_err) => {
                            if let Err(err) = self.handle_disconnect().await {
                                error!("error reconnecting {}. original error={}", err, op_err);
                            }
                        },
                    }
                }
            }
        }

        self.handle_flush().await?;

        Ok(())
    }

    async fn handle_server_op(&mut self, server_op: ServerOp) -> Result<(), io::Error> {
        match server_op {
            ServerOp::Ping => {
                self.connection.write_op(&ClientOp::Pong).await?;
                self.handle_flush().await?;
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
                    let message = Message {
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
                                .send(Event::SlowConsumer(sid))
                                .await
                                .ok();
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            self.subscriptions.remove(&sid);
                            self.connection
                                .write_op(&ClientOp::Unsubscribe { sid, max: None })
                                .await?;
                            self.handle_flush().await?;
                        }
                    }
                }
            }
            // TODO: we should probably update advertised server list here too.
            ServerOp::Info(info) => {
                if info.lame_duck_mode {
                    self.connector
                        .events_tx
                        .send(Event::LameDuckMode)
                        .await
                        .ok();
                }
            }

            _ => {
                // TODO: don't ignore.
            }
        }

        Ok(())
    }

    async fn handle_flush(&mut self) -> Result<(), io::Error> {
        self.connection.flush().await?;
        self.flush_interval.reset();

        Ok(())
    }

    async fn handle_command(&mut self, command: Command) -> Result<(), io::Error> {
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

                    if let Err(err) = self
                        .connection
                        .write_op(&ClientOp::Unsubscribe { sid, max })
                        .await
                    {
                        error!("Send failed with {:?}", err);
                    }
                }
            }
            Command::Ping => {
                debug!(
                    "PING command. Pending pings {}, max pings {}",
                    self.pending_pings, self.max_pings
                );
                self.pending_pings += 1;
                self.ping_interval.reset();

                if self.pending_pings > self.max_pings {
                    debug!(
                        "pending pings {}, max pings {}. disconnecting",
                        self.pending_pings, self.max_pings
                    );
                    self.handle_disconnect().await?;
                }

                if let Err(_err) = self.connection.write_op(&ClientOp::Ping).await {
                    self.handle_disconnect().await?;
                }

                self.handle_flush().await?;
            }
            Command::Flush { result } => {
                if let Err(_err) = self.handle_flush().await {
                    if let Err(err) = self.handle_disconnect().await {
                        result.send(Err(err)).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "one shot failed to be received")
                        })?;
                    } else if let Err(err) = self.handle_flush().await {
                        result.send(Err(err)).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "one shot failed to be received")
                        })?;
                    } else {
                        result.send(Ok(())).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "one shot failed to be received")
                        })?;
                    }
                } else {
                    result.send(Ok(())).map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "one shot failed to be received")
                    })?;
                }
            }
            Command::TryFlush => {
                self.handle_flush().await?;
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

                if let Err(err) = self
                    .connection
                    .write_op(&ClientOp::Subscribe {
                        sid,
                        subject,
                        queue_group,
                    })
                    .await
                {
                    error!("Sending Subscribe failed with {:?}", err);
                }
            }
            Command::Publish {
                subject,
                payload,
                respond,
                headers,
            } => {
                let pub_op = ClientOp::Publish {
                    subject: subject,
                    payload: payload,
                    respond: respond,
                    headers: headers,
                };
                while let Err(err) = self
                    .connection
                    .write_op(&pub_op)
                    .await
                {
                    self.handle_disconnect().await?;
                    error!("Sending Publish failed with {:?}", err);
                }
            }
            Command::Connect(connect_info) => {
                while let Err(_err) = self
                    .connection
                    .write_op(&ClientOp::Connect(connect_info.clone()))
                    .await
                {
                    self.handle_disconnect().await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_disconnect(&mut self) -> io::Result<()> {
        self.pending_pings = 0;
        self.connector.events_tx.try_send(Event::Disconnected).ok();
        self.connector.state_tx.send(State::Disconnected).ok();
        self.handle_reconnect().await?;

        Ok(())
    }

    async fn handle_reconnect(&mut self) -> Result<(), io::Error> {
        let (info, connection) = self.connector.connect().await?;
        self.connection = connection;
        self.info_sender.send(info).map_err(|err| {
            std::io::Error::new(
                ErrorKind::Other,
                format!("failed to send info update: {err}"),
            )
        })?;

        self.subscriptions
            .retain(|_, subscription| !subscription.sender.is_closed());

        for (sid, subscription) in &self.subscriptions {
            self.connection
                .write_op(&ClientOp::Subscribe {
                    sid: *sid,
                    subject: subscription.subject.to_owned(),
                    queue_group: subscription.queue_group.to_owned(),
                })
                .await
                .unwrap();
        }
        self.connector.events_tx.try_send(Event::Connected).ok();

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
/// let mut nc = async_nats::connect_with_options("demo.nats.io", async_nats::ConnectOptions::new()).await?;
/// nc.publish("test".into(), "data".into()).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_options<A: ToServerAddrs>(
    addrs: A,
    options: ConnectOptions,
) -> Result<Client, ConnectError> {
    let ping_period = options.ping_interval;
    let flush_period = options.flush_interval;

    let (events_tx, mut events_rx) = mpsc::channel(128);
    let (state_tx, state_rx) = tokio::sync::watch::channel(State::Pending);

    let mut connector = Connector::new(
        addrs,
        ConnectorOptions {
            tls_required: options.tls_required,
            certificates: options.certificates,
            client_key: options.client_key,
            client_cert: options.client_cert,
            tls_client_config: options.tls_client_config,
            auth: options.auth,
            no_echo: options.no_echo,
            connection_timeout: options.connection_timeout,
            name: options.name,
            ignore_discovered_servers: options.ignore_discovered_servers,
            retain_servers_order: options.retain_servers_order,
        },
        events_tx,
        state_tx,
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

    let (info_sender, info_watcher) = tokio::sync::watch::channel(info);
    let (sender, receiver) = mpsc::channel(options.sender_capacity);

    let client = Client::new(
        info_watcher,
        state_rx,
        sender,
        options.subscription_capacity,
        options.inbox_prefix,
        options.request_timeout,
    );

    task::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            options.event_callback.call(event).await
        }
    });

    task::spawn(async move {
        if connection.is_none() && options.retry_on_initial_connect {
            let (info, connection_ok) = connector.connect().await.unwrap();
            info_sender.send(info).ok();
            connection = Some(connection_ok);
        }
        let connection = connection.unwrap();
        let mut connection_handler = ConnectionHandler::new(
            connection,
            connector,
            info_sender,
            ping_period,
            flush_period,
        );
        connection_handler.process(receiver).await
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
/// nc.publish("test".into(), "data".into()).await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Connect with [Vec] of [ServerAddr].
/// ```no_run
///#[tokio::main]
///# async fn main() -> Result<(), async_nats::Error> {
///use async_nats::ServerAddr;
///let client = async_nats::connect(vec![
///    "demo.nats.io".parse::<ServerAddr>()?,
///    "other.nats.io".parse::<ServerAddr>()?,
///])
///.await
///.unwrap();
///# Ok(())
///# }
/// ```
///
/// ## with [Vec], but parse URLs inside [crate::connect()]
/// ```no_run
///#[tokio::main]
///# async fn main() -> Result<(), async_nats::Error> {
///use async_nats::ServerAddr;
///let servers = vec!["demo.nats.io", "other.nats.io"];
///let client = async_nats::connect(
///    servers
///        .iter()
///        .map(|url| url.parse())
///        .collect::<Result<Vec<ServerAddr>, _>>()?
///)
///.await?;
///# Ok(())
///# }
///```
///
///
/// ## with slice.
/// ```no_run
///#[tokio::main]
///# async fn main() -> Result<(), async_nats::Error> {
///use async_nats::ServerAddr;
///let client = async_nats::connect(
///    [
///        "demo.nats.io".parse::<ServerAddr>()?,
///        "other.nats.io".parse::<ServerAddr>()?,
///    ]
///    .as_slice(),
///)
///.await?;
///# Ok(())
///# }
///
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
}

/// Returned when initial connection fails.
/// To be enumerate over the variants, call [ConnectError::kind].
#[derive(Debug, Error)]
pub struct ConnectError {
    kind: ConnectErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source_info = self
            .source
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "no details".to_string());
        match self.kind {
            ConnectErrorKind::ServerParse => {
                write!(f, "failed to parse server or server list: {}", source_info)
            }
            ConnectErrorKind::Dns => write!(f, "DNS error: {}", source_info),
            ConnectErrorKind::Authentication => write!(f, "failed signing nonce"),
            ConnectErrorKind::AuthorizationViolation => write!(f, "authorization violation"),
            ConnectErrorKind::TimedOut => write!(f, "timed out"),
            ConnectErrorKind::Tls => write!(f, "TLS error: {}", source_info),
            ConnectErrorKind::Io => write!(f, "{}", source_info),
        }
    }
}

impl ConnectError {
    fn with_source<E>(kind: ConnectErrorKind, source: E) -> ConnectError
    where
        E: Into<Box<dyn std::error::Error + Sync + Send>>,
    {
        ConnectError {
            kind,
            source: Some(source.into()),
        }
    }
    fn new(kind: ConnectErrorKind) -> ConnectError {
        ConnectError { kind, source: None }
    }
    pub fn kind(&self) -> ConnectErrorKind {
        self.kind
    }
}

impl From<std::io::Error> for ConnectError {
    fn from(err: std::io::Error) -> Self {
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
/// # nc.publish("test".into(), "data".into()).await?;
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
    /// let mut subscriber = client.subscribe("foo".into()).await?;
    ///
    ///  subscriber.unsubscribe().await?;
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
    /// let mut subscriber = client.subscribe("test".into()).await?;
    /// subscriber.unsubscribe_after(3).await?;
    /// client.flush().await?;
    ///
    /// for _ in 0..3 {
    ///     client.publish("test".into(), "data".into()).await?;
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
}
impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(error) => write!(f, "nats: {error}"),
        }
    }
}

impl ServerError {
    fn new(error: String) -> ServerError {
        match error.to_lowercase().as_str() {
            "authorization violation" => ServerError::AuthorizationViolation,
            other => ServerError::Other(other.to_string()),
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
    pub fn username(&self) -> Option<String> {
        let user = self.0.username();
        if user.is_empty() {
            None
        } else {
            Some(user.to_string())
        }
    }

    /// Returns the optional password in the url.
    pub fn password(&self) -> Option<String> {
        self.0.password().map(|pwd| pwd.to_string())
    }

    /// Return the sockets from resolving the server address.
    pub fn socket_addrs(&self) -> io::Result<impl Iterator<Item = SocketAddr>> {
        (self.host(), self.port()).to_socket_addrs()
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

    ///
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

pub(crate) enum Authorization {
    /// No authentication.
    None,

    /// Authenticate using a token.
    Token(String),

    /// Authenticate using a username and password.
    UserAndPassword(String, String),

    /// Authenticate using nkey seed
    NKey(String),

    /// Authenticate using a jwt and signing function.
    Jwt(
        String,
        CallbackArg1<String, std::result::Result<String, AuthError>>,
    ),
}

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
}
