use futures_util::stream::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::{self, FromStr};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use subslice::SubsliceExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

use bytes::{Buf, Bytes, BytesMut};
use futures_util::future::FutureExt;
use futures_util::select;
use serde::{Deserialize, Serialize};
use serde_json;
use serde_repr::{Deserialize_repr, Serialize_repr};
use tokio::io;
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task;

pub type Error = Box<dyn std::error::Error>;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LANG: &str = "rust";

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
pub enum ServerOp {
    Ok,
    Info(Box<ServerInfo>),
    Ping,
    Pong,
    Message {
        sid: u64,
        subject: String,
        reply_to: Option<String>,
        payload: Bytes,
    },
}

#[derive(Debug)]
pub enum ClientOp {
    Publish {
        subject: String,
        payload: Bytes,
    },
    Subscribe {
        sid: u64,
        subject: String,
    },
    Unsubscribe {
        sid: u64,
    },
    Ping,
    Pong,
    Flush {
        result: oneshot::Sender<io::Result<()>>,
    },
    Connect(ConnectInfo),
}

/// A framed connection
///
/// The type will probably not be public.
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Connection, io::Error> {
        let tcp_stream = TcpStream::connect(addr).await?;
        tcp_stream.set_nodelay(true)?;

        Ok(Connection {
            stream: BufWriter::new(tcp_stream),
            buffer: BytesMut::new(),
        })
    }

    pub fn parse_op(&mut self) -> Result<Option<ServerOp>, io::Error> {
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

        if self.buffer.starts_with(b"INFO ") {
            if let Some(len) = self.buffer.find(b"\r\n") {
                let line = std::str::from_utf8(&self.buffer[5..len]).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "cannot convert server info")
                })?;

                let server_info = serde_json::from_str(line).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "cannot parse server info")
                })?;

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

                let sid = u64::from_str(sid).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot parse sid argument after MSG",
                    )
                })?;

                // Parse the number of payload bytes.
                let payload_len = usize::from_str(payload_len).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot parse the number of bytes argument after MSG",
                    )
                })?;

                // Only advance if there is enough data for the entire operation and payload remaining.
                if len + payload_len + 4 <= self.buffer.remaining() {
                    let subject = subject.to_owned();
                    let reply_to = reply_to.map(String::from);

                    self.buffer.advance(len + 2);
                    let payload = self.buffer.split_to(payload_len).freeze();
                    self.buffer.advance(2);

                    return Ok(Some(ServerOp::Message {
                        sid,
                        reply_to,
                        subject,
                        payload,
                    }));
                }
            }

            return Ok(None);
        }

        Ok(None)
    }

    pub async fn read_op(&mut self) -> Result<Option<ServerOp>, io::Error> {
        loop {
            if let Some(op) = self.parse_op()? {
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

    pub async fn write_op(&mut self, item: ClientOp) -> Result<(), io::Error> {
        match item {
            ClientOp::Connect(connect_info) => {
                let op = format!(
                    "CONNECT {}\r\n",
                    serde_json::to_string(&connect_info).map_err(|_| io::Error::new(
                        io::ErrorKind::InvalidData,
                        "cannot serialize connect info"
                    ))?
                );
                self.stream.write_all(op.as_bytes()).await?;
            }
            ClientOp::Publish { subject, payload } => {
                let mut bufi = itoa::Buffer::new();
                self.stream.write_all(b"PUB ").await?;
                self.stream.write_all(subject.as_bytes()).await?;
                self.stream.write_all(b" ").await?;
                self.stream
                    .write_all(bufi.format(payload.len()).as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                self.stream.write_all(&payload).await?;
                self.stream.write_all(b"\r\n").await?;
            }

            ClientOp::Subscribe { sid, subject } => {
                self.stream.write_all(b"SUB ").await?;
                self.stream.write_all(subject.as_bytes()).await?;
                self.stream
                    .write_all(format!(" {}\r\n", sid).as_bytes())
                    .await?;
                self.stream.flush().await?;
            }

            ClientOp::Unsubscribe { sid } => {
                self.stream.write_all(b"UNSUB ").await?;
                self.stream
                    .write_all(format!("{}\r\n", sid).as_bytes())
                    .await?;
            }
            ClientOp::Ping => {
                self.stream.write_all(b"PING\r\n").await?;
            }
            ClientOp::Pong => {
                self.stream.write_all(b"PONG\r\n").await?;
            }
            ClientOp::Flush { result } => {
                result.send(self.stream.flush().await).map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "one shot failed to be received")
                })?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Subscription {
    sender: mpsc::Sender<Message>,
}

#[derive(Debug)]
struct SubscriptionContext {
    next_sid: u64,
    subscription_map: HashMap<u64, Subscription>,
}

impl SubscriptionContext {
    pub fn new() -> SubscriptionContext {
        SubscriptionContext {
            next_sid: 1,
            subscription_map: HashMap::new(),
        }
    }

    pub fn get(&mut self, sid: u64) -> Option<&Subscription> {
        self.subscription_map.get(&sid)
    }

    pub fn insert(&mut self, subscription: Subscription) -> u64 {
        let sid = self.next_sid;
        self.next_sid += 1;

        self.subscription_map.insert(sid, subscription);

        sid
    }
}

/// A connector which facilitates communication from channels to a single shared connection.
/// The connector takes ownership of the channel.
///
/// The type will probably not be public.
pub struct Connector {
    connection: Connection,
    subscription_context: Arc<Mutex<SubscriptionContext>>,
}

impl Connector {
    pub(crate) fn new(
        connection: Connection,
        subscription_context: Arc<Mutex<SubscriptionContext>>,
    ) -> Connector {
        Connector {
            connection,
            subscription_context,
        }
    }

    pub async fn process(
        &mut self,
        mut receiver: mpsc::Receiver<ClientOp>,
    ) -> Result<(), io::Error> {
        loop {
            select! {
                maybe_op = receiver.recv().fuse() => {
                    match maybe_op {
                        Some(op) => {
                            if let Err(err) = self.connection.write_op(op).await {
                                println!("Send failed with {:?}", err);
                            }
                        }
                        None => {
                            println!("Sender closed");
                            // Sender dropped, return.
                            break
                        }
                    }
                }

                result = self.connection.read_op().fuse() => {
                    if let Ok(maybe_op) = result {
                        match maybe_op {
                            Some(ServerOp::Ping) => {
                                self.connection.write_op(ClientOp::Pong).await?;
                            }
                            Some(ServerOp::Message { sid, subject, reply_to, payload }) => {
                                let mut context = self.subscription_context.lock().await;
                                if let Some(subscription) = context.get(sid) {
                                    let message = Message {
                                        subject,
                                        reply_to,
                                        payload,
                                    };

                                    subscription.sender.send(message).await.unwrap();
                                }
                            }

                            None => {
                                return Ok(())
                            }

                            _ => {
                                // ignore.
                            }
                        }
                    }
                }
            }
            // ...
        }

        self.connection.stream.flush().await?;

        Ok(())
    }
}

pub struct Client {
    sender: mpsc::Sender<ClientOp>,
    subscription_context: Arc<Mutex<SubscriptionContext>>,
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::Sender<ClientOp>,
        subscription_context: Arc<Mutex<SubscriptionContext>>,
    ) -> Client {
        Client {
            sender,
            subscription_context,
        }
    }

    pub async fn publish(&mut self, subject: String, payload: Bytes) -> Result<(), Error> {
        self.sender
            .send(ClientOp::Publish { subject, payload })
            .await?;

        Ok(())
    }

    pub async fn subscribe(&mut self, subject: String) -> Result<Subscriber, io::Error> {
        let (sender, receiver) = mpsc::channel(16);

        // Aiming to make this the only lock (aside from internal locks in channels).
        let mut context = self.subscription_context.lock().await;
        let sid = context.insert(Subscription { sender });

        self.sender
            .send(ClientOp::Subscribe { sid, subject })
            .await
            .unwrap();

        Ok(Subscriber::new(sid, receiver))
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(ClientOp::Flush { result: tx }).await?;
        // first question mark is an error from rx itself, second for error from flush.
        rx.await??;
        Ok(())
    }
}

pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client, io::Error> {
    let mut connection = Connection::connect(addr).await?;
    let subscription_context = Arc::new(Mutex::new(SubscriptionContext::new()));
    let mut connector = Connector::new(connection, subscription_context.clone());

    // TODO make channel size configurable
    let (sender, receiver) = mpsc::channel(128);
    let client = Client::new(sender.clone(), subscription_context);
    let connect_info = ConnectInfo {
        // FIXME(tp): handle TLS properly
        tls_required: false,
        // FIXME(tp): have optional name
        name: Some("beta-rust-client".to_string()),
        pedantic: false,
        verbose: false,
        lang: crate::LANG.to_string(),
        version: crate::VERSION.to_string(),
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
    client
        .sender
        .send(ClientOp::Connect(connect_info))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to send connect"))?;
    client
        .sender
        .send(ClientOp::Ping)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to send ping"))?;

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            match sender.send(ClientOp::Ping).await {
                Ok(()) => {}
                Err(_) => {
                    return;
                }
            }
        }
    });

    task::spawn(async move { connector.process(receiver).await });

    Ok(client)
}

#[derive(Debug)]
pub struct Message {
    pub subject: String,
    pub reply_to: Option<String>,
    pub payload: Bytes,
}

pub struct Subscriber {
    _sid: u64,
    receiver: mpsc::Receiver<Message>,
}

impl Subscriber {
    fn new(sid: u64, receiver: mpsc::Receiver<Message>) -> Subscriber {
        Subscriber {
            _sid: sid,
            receiver,
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        // Can we get away with just closing, and then handling that on the sender side?
        self.receiver.close();
    }
}

impl Stream for Subscriber {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
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
