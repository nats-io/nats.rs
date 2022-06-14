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
//! async fn example() {
//!     let client = async_nats::connect("demo.nats.io").await.unwrap();
//!     let mut subscriber = client.subscribe("foo".into()).await.unwrap();
//!
//!     for _ in 0..10 {
//!         client.publish("foo".into(), "data".into()).await.unwrap();
//!     }
//!
//!     let mut i = 0;
//!     while subscriber.next()
//!         .await
//!         .is_some()
//!     {
//!         i += 1;
//!         if i >= 10 {
//!             break;
//!         }
//!     }
//!     assert_eq!(i, 10);
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
//!
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
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn Error>> {
//! let client = async_nats::connect("demo.nats.io").await?;
//!
//! let mut subscriber = client.subscribe("foo".into()).await.unwrap();
//!
//! while let Some(message) = subscriber.next().await {
//!     println!("Received message {:?}", message);
//! }
//! #     Ok(())
//! # }

use futures::future::FutureExt;
use futures::select;
use futures::stream::Stream;
use futures::stream::StreamExt;
use tls::TlsOptions;

use std::cmp;
use std::collections::HashMap;
use std::iter;
use std::net::{SocketAddr, ToSocketAddrs};
use std::option;
use std::pin::Pin;
use std::slice;
use std::str::{self, FromStr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::ErrorKind;
use tokio::time::sleep;
use url::Url;

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use tokio::io;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LANG: &str = "rust";

/// A re-export of the `rustls` crate used in this crate,
/// for use in cases where manual client configurations
/// must be provided using `Options::tls_client_config`.
pub use tokio_rustls::rustls;

use connection::Connection;
pub use header::{HeaderMap, HeaderValue};

pub(crate) mod auth_utils;
mod connection;
mod options;

use crate::options::CallbackArg1;
pub use options::{AuthError, ConnectOptions};

pub mod header;
pub mod jetstream;
pub mod message;
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

/// Maintains a list of servers and establishes connections.
pub(crate) struct Connector {
    /// A map of servers and number of connect attempts.
    servers: HashMap<ServerAddr, usize>,
    options: TlsOptions,
}

impl Connector {
    pub(crate) fn new<A: ToServerAddrs>(
        addrs: A,
        options: TlsOptions,
    ) -> Result<Connector, io::Error> {
        let servers = addrs
            .to_server_addrs()?
            .into_iter()
            .map(|addr| (addr, 0))
            .collect();

        Ok(Connector { servers, options })
    }

    pub(crate) async fn connect(&mut self) -> Result<(ServerInfo, Connection), io::Error> {
        for _ in 0..128 {
            if let Ok(inner) = self.try_connect().await {
                return Ok(inner);
            }
        }

        Err(io::Error::new(io::ErrorKind::Other, "unable to connect"))
    }

    pub(crate) async fn try_connect(&mut self) -> Result<(ServerInfo, Connection), io::Error> {
        let mut error = None;

        let server_addrs: Vec<ServerAddr> = self.servers.keys().cloned().collect();
        for server_addr in server_addrs {
            let server_attempts = self.servers.get_mut(&server_addr).unwrap();
            let duration = if *server_attempts == 0 {
                Duration::from_millis(0)
            } else {
                let exp: u32 = (*server_attempts - 1).try_into().unwrap_or(std::u32::MAX);
                let max = Duration::from_secs(4);

                cmp::min(Duration::from_millis(2_u64.saturating_pow(exp)), max)
            };

            *server_attempts += 1;
            sleep(duration).await;

            let socket_addrs = server_addr.socket_addrs()?;
            for socket_addr in socket_addrs {
                match self
                    .try_connect_to(&socket_addr, server_addr.tls_required(), server_addr.host())
                    .await
                {
                    Ok((info, connection)) => {
                        for url in &info.connect_urls {
                            let server_addr = url.parse::<ServerAddr>()?;
                            self.servers.entry(server_addr).or_insert(0);
                        }

                        let server_attempts = self.servers.get_mut(&server_addr).unwrap();
                        *server_attempts = 0;

                        return Ok((info, connection));
                    }

                    Err(inner) => error.replace(inner),
                };
            }
        }

        Err(error.unwrap())
    }

    pub(crate) async fn try_connect_to(
        &self,
        socket_addr: &SocketAddr,
        tls_required: bool,
        tls_host: &str,
    ) -> Result<(ServerInfo, Connection), io::Error> {
        let tls_config = tls::config_tls(&self.options).await?;

        let tcp_stream = TcpStream::connect(socket_addr).await?;
        tcp_stream.set_nodelay(true)?;

        let mut connection = Connection {
            stream: Box::new(BufWriter::new(tcp_stream)),
            buffer: BytesMut::new(),
        };

        let op = connection.read_op().await?;
        let info = match op {
            Some(ServerOp::Info(info)) => info,
            Some(op) => {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    format!("expected INFO, got {:?}", op),
                ))
            }
            None => {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    "expected INFO, got nothing",
                ))
            }
        };

        if self.options.tls_required || info.tls_required || tls_required {
            let tls_config = Arc::new(tls_config);
            let tls_connector =
                tokio_rustls::TlsConnector::try_from(tls_config).map_err(|err| {
                    io::Error::new(
                        ErrorKind::Other,
                        format!("failed to create TLS connector from TLS config: {}", err),
                    )
                })?;

            let domain = rustls::ServerName::try_from(info.host.as_str())
                .or_else(|_| rustls::ServerName::try_from(tls_host))
                .map_err(|_| {
                    io::Error::new(
                        ErrorKind::InvalidInput,
                        "cannot determine hostname for TLS connection",
                    )
                })?;

            connection = Connection {
                stream: Box::new(tls_connector.connect(domain, connection.stream).await?),
                buffer: BytesMut::new(),
            };
        };

        Ok((*info, connection))
    }
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
    events: mpsc::Sender<ServerEvent>,
    pending_pings: usize,
    max_pings: usize,
}

impl ConnectionHandler {
    pub(crate) fn new(
        connection: Connection,
        connector: Connector,
        events: mpsc::Sender<ServerEvent>,
    ) -> ConnectionHandler {
        ConnectionHandler {
            connection,
            connector,
            subscriptions: HashMap::new(),
            events,
            pending_pings: 0,
            max_pings: 2,
        }
    }

    pub async fn process(
        &mut self,
        mut receiver: mpsc::Receiver<Command>,
    ) -> Result<(), io::Error> {
        loop {
            select! {
                maybe_command = receiver.recv().fuse() => {
                    match maybe_command {
                        Some(command) => if let Err(err) = self.handle_command(command).await {
                            println!("error handling command {}", err);
                        }
                        None => {
                            break;
                        }
                    }
                }

                maybe_op_result = self.connection.read_op().fuse() => {
                    match maybe_op_result {
                        Ok(Some(server_op)) => if let Err(err) = self.handle_server_op(server_op).await {
                            println!("error handling operation {}", err);
                        }
                        Ok(None) => {
                            if let Err(err) = self.handle_disconnect().await {
                                println!("error handling operation {}", err);
                            } else {
                            }
                        }
                        Err(op_err) => {
                            if let Err(err) = self.handle_disconnect().await {
                                println!("error reconnecting {}. original error={}", err, op_err);
                            }
                        },
                    }
                }
            }
        }

        self.connection.flush().await?;

        Ok(())
    }

    async fn handle_server_op(&mut self, server_op: ServerOp) -> Result<(), io::Error> {
        match server_op {
            ServerOp::Ping => {
                self.connection.write_op(ClientOp::Pong).await?;
                self.connection.flush().await?;
            }
            ServerOp::Pong => {
                self.pending_pings -= 1;
            }
            ServerOp::Error(error) => {
                self.events.try_send(ServerEvent::Error(error)).ok();
            }
            ServerOp::Message {
                sid,
                subject,
                reply,
                payload,
                headers,
                status,
                description,
            } => {
                if let Some(subscription) = self.subscriptions.get_mut(&sid) {
                    let message = Message {
                        subject,
                        reply,
                        payload,
                        headers,
                        status,
                        description,
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
                            self.events.send(ServerEvent::SlowConsumer(sid)).await.ok();
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            self.subscriptions.remove(&sid);
                            self.connection
                                .write_op(ClientOp::Unsubscribe { sid, max: None })
                                .await?;
                            self.connection.flush().await?;
                        }
                    }
                }
            }
            // TODO: we should probably update advertised server list here too.
            ServerOp::Info(info) => {
                if info.lame_duck_mode {
                    self.events.send(ServerEvent::LameDuckMode).await.ok();
                }
            }

            _ => {
                // TODO: don't ignore.
            }
        }

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
                        .write_op(ClientOp::Unsubscribe { sid, max })
                        .await
                    {
                        println!("Send failed with {:?}", err);
                    }
                }
            }
            Command::Ping => {
                self.pending_pings += 1;

                if self.pending_pings > self.max_pings {
                    self.handle_disconnect().await?;
                }

                if let Err(_err) = self.connection.write_op(ClientOp::Ping).await {
                    self.handle_disconnect().await?;
                }

                self.connection.flush().await?;
            }
            Command::Flush { result } => {
                if let Err(_err) = self.connection.flush().await {
                    if let Err(err) = self.handle_disconnect().await {
                        result.send(Err(err)).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "one shot failed to be received")
                        })?;
                    } else if let Err(err) = self.connection.flush().await {
                        result.send(Err(err)).map_err(|_| {
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
                self.connection.flush().await?;
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
                    .write_op(ClientOp::Subscribe {
                        sid,
                        subject,
                        queue_group,
                    })
                    .await
                {
                    println!("Sending Subscribe failed with {:?}", err);
                }
            }
            Command::Publish {
                subject,
                payload,
                respond,
                headers,
            } => {
                while let Err(err) = self
                    .connection
                    .write_op(ClientOp::Publish {
                        subject: subject.clone(),
                        payload: payload.clone(),
                        respond: respond.clone(),
                        headers: headers.clone(),
                    })
                    .await
                {
                    self.handle_disconnect().await?;
                    println!("Sending Publish failed with {:?}", err);
                }
            }
            Command::Connect(connect_info) => {
                while let Err(_err) = self
                    .connection
                    .write_op(ClientOp::Connect(connect_info.clone()))
                    .await
                {
                    self.handle_disconnect().await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_disconnect(&mut self) -> io::Result<()> {
        self.events.try_send(ServerEvent::Disconnect).ok();
        self.handle_reconnect().await?;

        Ok(())
    }

    async fn handle_reconnect(&mut self) -> Result<(), io::Error> {
        let (_, connection) = self.connector.connect().await?;
        self.connection = connection;

        self.subscriptions
            .retain(|_, subscription| !subscription.sender.is_closed());

        for (sid, subscription) in &self.subscriptions {
            self.connection
                .write_op(ClientOp::Subscribe {
                    sid: *sid,
                    subject: subscription.subject.to_owned(),
                    queue_group: subscription.queue_group.to_owned(),
                })
                .await
                .unwrap();
        }
        self.events.try_send(ServerEvent::Reconnect).ok();
        Ok(())
    }
}

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

    pub async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
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
            .await?;
        Ok(())
    }

    pub async fn publish_with_reply_and_headers(
        &self,
        subject: String,
        reply: String,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<(), Error> {
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

    pub async fn request(&self, subject: String, payload: Bytes) -> Result<Message, Error> {
        let inbox = self.new_inbox();
        let mut sub = self.subscribe(inbox.clone()).await?;
        self.publish_with_reply(subject, inbox, payload).await?;
        self.flush().await?;
        match sub.next().await {
            Some(message) => Ok(message),
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
    /// # async fn main() -> std::io::Result<()> {
    /// # let mut nc = async_nats::connect("demo.nats.io").await?;
    /// let reply = nc.new_inbox();
    /// let rsub = nc.subscribe(reply).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_inbox(&self) -> String {
        format!("_INBOX.{}", nuid::next())
    }

    pub async fn queue_subscribe(
        &self,
        subject: String,
        queue_group: String,
    ) -> Result<Subscriber, io::Error> {
        self._subscribe(subject, Some(queue_group)).await
    }

    pub async fn subscribe(&self, subject: String) -> Result<Subscriber, io::Error> {
        self._subscribe(subject, None).await
    }

    // TODO: options/questions for nats team:
    //  - should there just be a single subscribe() function (would be breaking api against 0.11.0)
    //  - if queue_subscribe is a separate function, how do you want to name the private function here?
    async fn _subscribe(
        &self,
        subject: String,
        queue_group: Option<String>,
    ) -> Result<Subscriber, io::Error> {
        let sid = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(self.subscription_capacity);

        self.sender
            .send(Command::Subscribe {
                sid,
                subject,
                queue_group,
                sender,
            })
            .await
            .unwrap();

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
) -> Result<Client, io::Error> {
    let tls_required = options.tls_required;
    let ping_interval = options.ping_interval;
    let flush_interval = options.flush_interval;

    let tls_options = TlsOptions {
        tls_required: options.tls_required,
        certificates: options.certificates,
        client_key: options.client_key,
        client_cert: options.client_cert,
        tls_client_config: options.tls_client_config,
    };

    let mut connector = Connector::new(addrs, tls_options)?;
    let (server_info, connection) = connector.try_connect().await?;
    let (events_tx, mut events_rx) = mpsc::channel(128);

    let mut connection_handler = ConnectionHandler::new(connection, connector, events_tx);

    // TODO make channel size configurable
    let (sender, receiver) = mpsc::channel(options.sender_capacity);

    let client = Client::new(sender.clone(), options.subscription_capacity);
    let mut connect_info = ConnectInfo {
        tls_required,
        // FIXME(tp): have optional name
        name: Some("beta-rust-client".to_string()),
        pedantic: false,
        verbose: false,
        lang: LANG.to_string(),
        version: VERSION.to_string(),
        protocol: Protocol::Dynamic,
        user: None,
        pass: None,
        auth_token: None,
        user_jwt: None,
        nkey: None,
        signature: None,
        echo: true,
        headers: true,
        no_responders: true,
    };
    match options.auth {
        Authorization::None => {}
        Authorization::Token(token) => connect_info.auth_token = Some(token),
        Authorization::UserAndPassword(user, pass) => {
            connect_info.user = Some(user);
            connect_info.pass = Some(pass);
        }
        Authorization::Jwt(jwt, sign_fn) => match sign_fn.call(server_info.nonce.clone()).await {
            Ok(sig) => {
                connect_info.user_jwt = Some(jwt.clone());
                connect_info.signature = Some(sig);
            }
            Err(e) => {
                println!(
                    "JWT auth is disabled. sign error: {} (possibly invalid key or corrupt cred file?)",
                    e
                );
            }
        },
    }
    connection_handler
        .connection
        .write_op(ClientOp::Connect(connect_info))
        .await?;
    connection_handler.connection.flush().await?;
    match connection_handler.connection.read_op().await? {
        Some(ServerOp::Error(err)) => {
            return Err(io::Error::new(ErrorKind::InvalidInput, err.to_string()));
        }
        Some(op) => {
            connection_handler.handle_server_op(op).await?;
        }
        None => return Err(io::Error::new(ErrorKind::BrokenPipe, "connection aborted")),
    }

    tokio::spawn({
        let sender = sender.clone();
        async move {
            loop {
                match sender.send(Command::Ping).await {
                    Ok(()) => {}
                    Err(_) => return,
                }
                tokio::time::sleep(ping_interval).await;
            }
        }
    });

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(flush_interval).await;
            match sender.send(Command::TryFlush).await {
                Ok(()) => {}
                Err(_) => return,
            }
        }
    });

    task::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            match event {
                ServerEvent::Reconnect => options.reconnect_callback.call().await,
                ServerEvent::Disconnect => options.disconnect_callback.call().await,
                ServerEvent::Error(error) => options.error_callback.call(error).await,
                ServerEvent::LameDuckMode => options.lame_duck_callback.call().await,
                ServerEvent::SlowConsumer(sid) => {
                    options
                        .error_callback
                        .call(ServerError::SlowConsumer(sid))
                        .await
                }
            }
        }
    });

    task::spawn(async move { connection_handler.process(receiver).await });

    Ok(client)
}

pub(crate) enum ServerEvent {
    Reconnect,
    Disconnect,
    LameDuckMode,
    SlowConsumer(u64),
    Error(ServerError),
}

/// Connects to NATS with default config.
///
/// Returns clonable [Client].
///
/// To have customized NATS connection, check [ConnectOptions].
///
/// # Examples
/// ```
/// # #[tokio::main]
/// # async fn main() ->  Result<(), async_nats::Error> {
/// let mut nc = async_nats::connect("demo.nats.io").await?;
/// nc.publish("test".into(), "data".into()).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect<A: ToServerAddrs>(addrs: A) -> Result<Client, io::Error> {
    connect_with_options(addrs, ConnectOptions::default()).await
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
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    ///
    /// let mut subscriber = client.subscribe("foo".into()).await?;
    ///
    ///  subscriber.unsubscribe().await?;
    /// # Ok(())
    /// # }
    pub async fn unsubscribe(&mut self) -> io::Result<()> {
        self.sender
            .send(Command::Unsubscribe {
                sid: self.sid,
                max: None,
            })
            .await
            .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
        self.receiver.close();
        Ok(())
    }

    /// Unsubscribes from subscription after reaching given number of messages.
    /// This is the total number of messages received by this subcsription in it's whole
    /// lifespan. If it already reeached or surpassed the passed value, it will immediately stop.
    ///
    /// # Examples
    /// ```
    /// # use futures::StreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    ///
    /// let mut sub = client.subscribe("test".into()).await?;
    /// sub.unsubscribe_after(3).await?;
    /// client.flush().await?;
    ///
    /// for _ in 0..3 {
    ///     client.publish("test".into(), "data".into()).await?;
    /// }
    ///
    /// while let Some(message) = sub.next().await {
    ///     println!("message received: {:?}", message);
    /// }
    /// println!("no more messages, unsubscribed");
    /// # Ok(())
    /// # }
    pub async fn unsubscribe_after(&mut self, unsub_after: u64) -> io::Result<()> {
        self.sender
            .send(Command::Unsubscribe {
                sid: self.sid,
                max: Some(unsub_after),
            })
            .await
            .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
        Ok(())
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
pub enum ServerError {
    AuthorizationViolation,
    SlowConsumer(u64),
    Other(String),
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
            Self::SlowConsumer(sid) => write!(f, "nats: subscription {} is a slow consumer", sid),
            Self::Other(error) => write!(f, "nats: {}", error),
        }
    }
}

/// Info to construct a CONNECT message.
#[derive(Clone, Debug, Serialize)]
pub struct ConnectInfo {
    /// Turns on +OK protocol acknowledgements.
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
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone, Copy)]
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
            format!("nats://{}", input).parse()
        }
        .map_err(|e| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!("NATS server URL is invalid: {}", e),
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
        self.0.host_str().unwrap()
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
    ///
    /// # Fault injection
    ///
    /// If compiled with the `"fault_injection"` feature this method might fail artificially.
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
        (&**self).to_server_addrs()
    }
}

impl<'a> ToServerAddrs for &'a [ServerAddr] {
    type Iter = iter::Cloned<slice::Iter<'a, ServerAddr>>;

    fn to_server_addrs(&self) -> io::Result<Self::Iter> {
        Ok(self.iter().cloned())
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

    /// Authenticate using a jwt and signing function.
    Jwt(
        String,
        CallbackArg1<String, std::result::Result<String, AuthError>>,
    ),
}
