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
//! use futures_util::StreamExt;
//!
//! #[tokio::main]
//! async fn example() {
//!     let mut client = async_nats::connect("demo.nats.io").await.unwrap();
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
//! # async fn main() -> Result<(), Box<dyn Error>> {
//! let mut client = async_nats::connect("demo.nats.io").await?;
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
//! # use futures_util::StreamExt;
//! # use std::error::Error;
//! # use std::time::Instant;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn Error>> {
//! let mut client = async_nats::connect("demo.nats.io").await?;
//!
//! let mut subscriber = client.subscribe("foo".into()).await.unwrap();
//!
//! while let Some(message) = subscriber.next().await {
//!     println!("Received message {:?}", message);
//! }
//! #     Ok(())
//! # }

use futures_util::future::FutureExt;
use futures_util::select;
use futures_util::stream::Stream;
use futures_util::StreamExt;
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
use subslice::SubsliceExt;
use tokio::io::ErrorKind;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::time::sleep;
use url::Url;

use bytes::{Buf, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use tokio::io;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task;

pub type Error = Box<dyn std::error::Error>;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LANG: &str = "rust";

/// A re-export of the `rustls` crate used in this crate,
/// for use in cases where manual client configurations
/// must be provided using `Options::tls_client_config`.
pub use tokio_rustls::rustls;

use header::HeaderName;
pub use header::{HeaderMap, HeaderValue};

mod options;
pub use options::ConnectOptions;
pub mod header;
mod tls;

/// Information sent by the server back to this client
/// during initial connection, and possibly again later.
#[allow(unused)]
#[derive(Debug, Deserialize, Default, Clone)]
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
    #[serde(default)]
    pub lame_duck_mode: bool,
}

#[derive(Clone, Debug)]
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
    TryFlush,
    Connect(ConnectInfo),
}

/// Supertrait enabling trait object for containing both TLS and non TLS `TcpStream` connection.
trait AsyncReadWrite: AsyncWrite + AsyncRead + Send + Unpin {}

/// Blanked implementation that applies to both TLS and non-TLS `TcpStream`.
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// A framed connection
pub(crate) struct Connection {
    stream: Box<dyn AsyncReadWrite>,
    buffer: BytesMut,
}

/// Internal representation of the connection.
/// Helds connection with NATS Server and communicates with `Client` via channels.
impl Connection {
    pub(crate) fn try_read_op(&mut self) -> Result<Option<ServerOp>, io::Error> {
        if self.buffer.starts_with(b"+OK\r\n") {
            self.buffer.advance(5);
            return Ok(Some(ServerOp::Ok));
        }

        if self.buffer.starts_with(b"PING\r\n") {
            self.buffer.advance(6);

            return Ok(Some(ServerOp::Ping));
        }

        if self.buffer.starts_with(b"PONG\r\n") {
            self.buffer.advance(6);

            return Ok(Some(ServerOp::Pong));
        }

        if self.buffer.starts_with(b"-ERR") {
            if let Some(len) = self.buffer.find(b"\r\n") {
                let line = std::str::from_utf8(&self.buffer[5..len])
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
                let error_message = line.trim_matches('\'').to_string();
                self.buffer.advance(len + 2);

                return Ok(Some(ServerOp::Error(ServerError::new(error_message))));
            }
        }

        if self.buffer.starts_with(b"INFO ") {
            if let Some(len) = self.buffer.find(b"\r\n") {
                let line = std::str::from_utf8(&self.buffer[5..len])
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

                let server_info = serde_json::from_str(line)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

                self.buffer.advance(len + 2);

                return Ok(Some(ServerOp::Info(Box::new(server_info))));
            }

            return Ok(None);
        }

        if self.buffer.starts_with(b"MSG ") {
            if let Some(len) = self.buffer.find(b"\r\n") {
                let line = std::str::from_utf8(&self.buffer[4..len]).unwrap();
                let args = line.split(' ').filter(|s| !s.is_empty());
                // TODO(caspervonb) we can drop this alloc
                let args = args.collect::<Vec<_>>();

                // Parse the operation syntax: MSG <subject> <sid> [reply-to] <#bytes>
                let (subject, sid, reply_to, payload_len) = match args[..] {
                    [subject, sid, payload_len] => (subject, sid, None, payload_len),
                    [subject, sid, reply_to, payload_len] => {
                        (subject, sid, Some(reply_to), payload_len)
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid number of arguments after MSG",
                        ));
                    }
                };

                let sid = u64::from_str(sid)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

                // Parse the number of payload bytes.
                let payload_len = usize::from_str(payload_len)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

                // Only advance if there is enough data for the entire operation and payload remaining.
                if len + payload_len + 4 <= self.buffer.remaining() {
                    let subject = subject.to_owned();
                    let reply_to = reply_to.map(String::from);

                    self.buffer.advance(len + 2);
                    let payload = self.buffer.split_to(payload_len).freeze();
                    self.buffer.advance(2);

                    return Ok(Some(ServerOp::Message {
                        sid,
                        reply: reply_to,
                        headers: None,
                        subject,
                        payload,
                    }));
                }
            }

            return Ok(None);
        }

        if self.buffer.starts_with(b"HMSG ") {
            if let Some(len) = self.buffer.find(b"\r\n") {
                // Extract whitespace-delimited arguments that come after "HMSG".
                let line = std::str::from_utf8(&self.buffer[5..len]).unwrap();
                let args = line.split_whitespace().filter(|s| !s.is_empty());
                let args = args.collect::<Vec<_>>();

                // <subject> <sid> [reply-to] <# header bytes><# total bytes>
                let (subject, sid, reply_to, num_header_bytes, num_bytes) = match args[..] {
                    [subject, sid, num_header_bytes, num_bytes] => {
                        (subject, sid, None, num_header_bytes, num_bytes)
                    }
                    [subject, sid, reply_to, num_header_bytes, num_bytes] => {
                        (subject, sid, Some(reply_to), num_header_bytes, num_bytes)
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid number of arguments after HMSG",
                        ));
                    }
                };

                // Convert the slice into an owned string.
                let subject = subject.to_string();

                // Parse the subject ID.
                let sid = u64::from_str(sid).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot parse sid argument after HMSG",
                    )
                })?;

                // Convert the slice into an owned string.
                let reply_to = reply_to.map(ToString::to_string);

                // Parse the number of payload bytes.
                let num_header_bytes = usize::from_str(num_header_bytes).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot parse the number of header bytes argument after \
                     HMSG",
                    )
                })?;

                // Parse the number of payload bytes.
                let num_bytes = usize::from_str(num_bytes).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot parse the number of bytes argument after HMSG",
                    )
                })?;

                if num_bytes < num_header_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "number of header bytes was greater than or equal to the \
                 total number of bytes after HMSG",
                    ));
                }

                // Only advance if there is enough data for the entire operation and payload remaining.
                if len + num_bytes + 4 <= self.buffer.remaining() {
                    self.buffer.advance(len + 2);
                    let buffer = self.buffer.split_to(num_header_bytes).freeze();
                    let payload = self.buffer.split_to(num_bytes - num_header_bytes).freeze();

                    let mut lines = std::str::from_utf8(&buffer).unwrap().lines().peekable();

                    let version_line = lines.next().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "no header version line found")
                    })?;

                    if !version_line.starts_with("NATS/1.0") {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "header version line does not begin with nats/1.0",
                        ));
                    }

                    let mut headers = HeaderMap::new();
                    while let Some(line) = lines.next() {
                        if line.is_empty() {
                            continue;
                        }

                        let (key, value) = line.split_once(':').ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "no header version line found",
                            )
                        })?;

                        let mut value = String::from_str(value).unwrap();
                        while let Some(v) = lines.next_if(|s| s.starts_with(char::is_whitespace)) {
                            value.push_str(v);
                        }

                        headers.append(
                            HeaderName::from_str(key).unwrap(),
                            HeaderValue::from_str(&value).unwrap(),
                        );
                    }

                    return Ok(Some(ServerOp::Message {
                        sid,
                        reply: reply_to,
                        subject,
                        headers: Some(headers),
                        payload,
                    }));
                }
            }

            return Ok(None);
        }

        Ok(None)
    }

    pub(crate) async fn read_op(&mut self) -> Result<Option<ServerOp>, io::Error> {
        loop {
            if let Some(op) = self.try_read_op()? {
                return Ok(Some(op));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(io::ErrorKind::ConnectionReset, ""));
                }
            }
        }
    }

    pub(crate) async fn write_op(&mut self, item: ClientOp) -> Result<(), io::Error> {
        match item {
            ClientOp::Connect(connect_info) => {
                let op = format!(
                    "CONNECT {}\r\n",
                    serde_json::to_string(&connect_info)
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
                );
                self.stream.write_all(op.as_bytes()).await?;
            }
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
            } => {
                if headers.is_some() {
                    self.stream.write_all(b"HPUB ").await?;
                } else {
                    self.stream.write_all(b"PUB ").await?;
                }

                self.stream.write_all(subject.as_bytes()).await?;
                self.stream.write_all(b" ").await?;

                if let Some(respond) = respond {
                    self.stream.write_all(respond.as_bytes()).await?;
                    self.stream.write_all(b" ").await?;
                }

                if let Some(headers) = headers {
                    let mut header = Vec::new();
                    header.extend_from_slice(b"NATS/1.0\r\n");
                    for (key, value) in headers.iter() {
                        header.extend_from_slice(key.as_ref());
                        header.push(b':');
                        header.extend_from_slice(value.as_ref());
                        header.extend_from_slice(b"\r\n");
                    }

                    header.extend_from_slice(b"\r\n");

                    let mut header_len_buf = itoa::Buffer::new();
                    self.stream
                        .write_all(header_len_buf.format(header.len()).as_bytes())
                        .await?;

                    self.stream.write_all(b" ").await?;

                    let mut total_len_buf = itoa::Buffer::new();
                    self.stream
                        .write_all(
                            total_len_buf
                                .format(header.len() + payload.len())
                                .as_bytes(),
                        )
                        .await?;

                    self.stream.write_all(b"\r\n").await?;
                    self.stream.write_all(&header).await?;
                } else {
                    let mut len_buf = itoa::Buffer::new();
                    self.stream
                        .write_all(len_buf.format(payload.len()).as_bytes())
                        .await?;
                    self.stream.write_all(b"\r\n").await?;
                }

                self.stream.write_all(&payload).await?;
                self.stream.write_all(b"\r\n").await?;
            }

            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                self.stream.write_all(b"SUB ").await?;
                self.stream.write_all(subject.as_bytes()).await?;
                if let Some(queue_group) = queue_group {
                    self.stream
                        .write_all(format!(" {}", queue_group).as_bytes())
                        .await?;
                }
                self.stream
                    .write_all(format!(" {}\r\n", sid).as_bytes())
                    .await?;
                self.stream.flush().await?;
            }

            ClientOp::Unsubscribe { sid, max } => {
                self.stream.write_all(b"UNSUB ").await?;
                self.stream.write_all(format!("{}", sid).as_bytes()).await?;
                if let Some(max) = max {
                    self.stream
                        .write_all(format!(" {}", max).as_bytes())
                        .await?;
                }
                self.stream.write_all(b"\r\n").await?;
            }
            ClientOp::Ping => {
                self.stream.write_all(b"PING\r\n").await?;
                self.stream.flush().await?;
            }
            ClientOp::Pong => {
                self.stream.write_all(b"PONG\r\n").await?;
                self.stream.flush().await?;
            }
            ClientOp::TryFlush => {
                self.stream.flush().await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> Result<(), io::Error> {
        self.stream.flush().await
    }
}

/// Maintains a list of servers and establishes connections.
pub(crate) struct Connector {
    server_addrs: Vec<ServerAddr>,
    options: TlsOptions,
}

impl Connector {
    pub(crate) async fn connect(&mut self) -> Result<(ServerInfo, Connection), io::Error> {
        for i in 0..128 {
            if let Ok(inner) = self.try_connect().await {
                return Ok(inner);
            }

            let exp: u32 = i.try_into().unwrap_or(std::u32::MAX);
            let max = Duration::from_secs(4);
            let duration = cmp::min(Duration::from_millis(2_u64.saturating_pow(exp)), max);

            sleep(duration).await;
        }

        Err(io::Error::new(io::ErrorKind::Other, "unable to connect"))
    }

    pub(crate) async fn try_connect(&mut self) -> Result<(ServerInfo, Connection), io::Error> {
        let mut error = None;

        for server_addr in &self.server_addrs {
            match self.try_connect_to(server_addr).await {
                Ok(inner) => return Ok(inner),
                Err(inner) => error.replace(inner),
            };
        }

        Err(error.unwrap())
    }

    pub(crate) async fn try_connect_to(
        &self,
        server_addr: &ServerAddr,
    ) -> Result<(ServerInfo, Connection), io::Error> {
        let tls_config = tls::config_tls(&self.options).await?;

        let tcp_stream = TcpStream::connect((server_addr.host(), server_addr.port())).await?;
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

        let tls_required =
            self.options.tls_required || info.tls_required || server_addr.tls_required();

        if tls_required {
            let tls_config = Arc::new(tls_config);
            let tls_connector =
                tokio_rustls::TlsConnector::try_from(tls_config).map_err(|err| {
                    io::Error::new(
                        ErrorKind::Other,
                        format!("failed to create TLS connector from TLS config: {}", err),
                    )
                })?;

            let domain = rustls::ServerName::try_from(info.host.as_str())
                .or_else(|_| rustls::ServerName::try_from(server_addr.host()))
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
                        Err(err) => {
                            if let Err(err) = self.handle_disconnect().await {
                                println!("error handling operation {}", err);
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
            } => {
                if let Some(subscription) = self.subscriptions.get_mut(&sid) {
                    let message = Message {
                        subject,
                        reply,
                        payload,
                        headers,
                    };

                    // if the channel for subscription was dropped, remove the
                    // subscription from the map and unsubscribe.
                    if subscription.sender.send(message).await.is_err() {
                        self.subscriptions.remove(&sid);
                        self.connection
                            .write_op(ClientOp::Unsubscribe { sid, max: None })
                            .await?;
                        self.connection.flush().await?;
                    // if channel was open and we sent the messsage, increase the
                    // `delivered` counter.
                    } else {
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
                if let Err(err) = self.connection.write_op(ClientOp::Ping).await {
                    self.handle_disconnect().await?;
                }
            }
            Command::Flush { result } => {
                if let Err(err) = self.connection.flush().await {
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
                if let Err(err) = self.connection.write_op(ClientOp::TryFlush).await {
                    println!("Sending TryFlush failed with {:?}", err);
                }
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
                while let Err(err) = self
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
        self.events.try_send(ServerEvent::Reconnect).ok();
        Ok(())
    }

    async fn handle_reconnect(&mut self) -> Result<(), io::Error> {
        let (_, connection) = self.connector.connect().await?;
        self.connection = connection;

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
}

impl Client {
    pub(crate) fn new(sender: mpsc::Sender<Command>) -> Client {
        Client {
            sender,
            next_subscription_id: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn publish(&mut self, subject: String, payload: Bytes) -> Result<(), Error> {
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
        &mut self,
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
        &mut self,
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
        &mut self,
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

    pub async fn request(&mut self, subject: String, payload: Bytes) -> Result<Message, Error> {
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
        &mut self,
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
            Some(message) => Ok(message),
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
        &mut self,
        subject: String,
        queue_group: String,
    ) -> Result<Subscriber, io::Error> {
        self._subscribe(subject, Some(queue_group)).await
    }

    pub async fn subscribe(&mut self, subject: String) -> Result<Subscriber, io::Error> {
        self._subscribe(subject, None).await
    }

    // TODO: options/questions for nats team:
    //  - should there just be a single subscribe() function (would be breaking api against 0.11.0)
    //  - if queue_subscribe is a separate function, how do you want to name the private function here?
    async fn _subscribe(
        &mut self,
        subject: String,
        queue_group: Option<String>,
    ) -> Result<Subscriber, io::Error> {
        let sid = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(16);

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

    pub async fn flush(&mut self) -> Result<(), Error> {
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
/// # async fn main() ->  Result<(), Box<dyn std::error::Error>> {
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

    let mut connector = Connector {
        server_addrs: addrs.to_server_addrs()?.into_iter().collect(),
        options: tls_options,
    };

    let (_, connection) = connector.try_connect().await?;
    let (events_tx, mut events_rx) = mpsc::channel(128);

    let mut connection_handler = ConnectionHandler::new(connection, connector, events_tx);

    // TODO make channel size configurable
    let (sender, receiver) = mpsc::channel(128);

    let client = Client::new(sender.clone());
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

    client
        .sender
        .send(Command::Ping)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to send ping"))?;

    tokio::spawn({
        let sender = sender.clone();
        async move {
            loop {
                tokio::time::sleep(ping_interval).await;
                match sender.send(Command::Ping).await {
                    Ok(()) => {}
                    Err(_) => return,
                }
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
            }
        }
    });

    task::spawn(async move { connection_handler.process(receiver).await });

    Ok(client)
}

pub(crate) enum ServerEvent {
    Reconnect,
    Disconnect,
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
/// # async fn main() ->  Result<(), Box<dyn std::error::Error>> {
/// let mut nc = async_nats::connect("demo.nats.io").await?;
/// nc.publish("test".into(), "data".into()).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect<A: ToServerAddrs>(addrs: A) -> Result<Client, io::Error> {
    connect_with_options(addrs, ConnectOptions::default()).await
}

#[derive(Debug)]
pub struct Message {
    pub subject: String,
    pub reply: Option<String>,
    pub payload: Bytes,
    pub headers: Option<HeaderMap>,
}

/// Retrieves messages from given `subscription` created by [Client::subscribe].
///
/// Implements [futures_util::stream::Stream] for ergonomic async message processing.
///
/// # Examples
/// ```
/// # #[tokio::main]
/// # async fn main() ->  Result<(), Box<dyn std::error::Error>> {
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
    /// # async fn unsubscribe() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = async_nats::connect("demo.nats.io").await?;
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
    /// # use futures_util::StreamExt;
    /// # #[tokio::main]
    /// # async fn unsubscribe() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = async_nats::connect("demo.nats.io").await?;
    ///
    /// for _ in 0..3 {
    ///     client.publish("test".into(), "data".into()).await?;
    /// }
    ///
    /// let mut sub = client.subscribe("test".into()).await?;
    /// sub.unsubscribe_after(3).await?;
    /// client.flush().await?;
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

#[derive(Clone, Debug)]
pub enum ServerError {
    AuthorizationViolation,
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
            Self::Other(error) => write!(f, "nats: {}", error),
        }
    }
}

/// Info to construct a CONNECT message.
#[derive(Clone, Debug, Serialize)]
#[doc(hidden)]
#[allow(clippy::module_name_repetitions)]
pub struct ConnectInfo {
    /// Turns on +OK protocol acknowledgements.
    pub verbose: bool,

    /// Turns on additional strict format checking, e.g. for properly formed
    /// subjects.
    pub pedantic: bool,

    /// User's JWT.
    pub user_jwt: Option<String>,

    /// Public nkey.
    pub nkey: Option<String>,

    /// Signed nonce, encoded to Base64URL.
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

#[derive(Clone)]
pub(crate) enum Authorization {
    /// No authentication.
    None,

    /// Authenticate using a token.
    Token(String),

    /// Authenticate using a username and password.
    UserAndPassword(String, String),
}
