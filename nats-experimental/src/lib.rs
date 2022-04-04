use futures_util::stream::Stream;
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::str::{self, FromStr};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use subslice::SubsliceExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::io::ErrorKind;
use url::Url;

use bytes::{Buf, Bytes, BytesMut};
use futures_util::future::FutureExt;
use futures_util::select;
use tokio::io;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task;

pub type Error = Box<dyn std::error::Error>;

/// Information sent by the server back to this client
/// during initial connection, and possibly again later.
#[allow(unused)]
#[derive(Debug, Default, Clone)]
pub struct ServerInfo {
    /// The unique identifier of the NATS server.
    pub server_id: String,
    /// Generated Server Name.
    pub server_name: String,
    /// The host specified in the cluster parameter/options.
    pub host: String,
    /// The port number specified in the cluster parameter/options.
    pub port: u16,
    /// The version of the NATS server.
    pub version: String,
    /// If this is set, then the server should try to authenticate upon
    /// connect.
    pub auth_required: bool,
    /// If this is set, then the server must authenticate using TLS.
    pub tls_required: bool,
    /// Maximum payload size that the server will accept.
    pub max_payload: usize,
    /// The protocol version in use.
    pub proto: i8,
    /// The server-assigned client ID. This may change during reconnection.
    pub client_id: u64,
    /// The version of golang the NATS server was built with.
    pub go: String,
    /// The nonce used for nkeys.
    pub nonce: String,
    /// A list of server urls that a client can connect to.
    pub connect_urls: Vec<String>,
    /// The client IP as known by the server.
    pub client_ip: String,
    /// Whether the server supports headers.
    pub headers: bool,
    /// Whether server goes into lame duck mode.
    pub lame_duck_mode: bool,
}

impl ServerInfo {
    fn parse(s: &str) -> Option<ServerInfo> {
        let mut obj = json::parse(s).ok()?;
        Some(ServerInfo {
            server_id: obj["server_id"].take_string()?,
            server_name: obj["server_name"].take_string().unwrap_or_default(),
            host: obj["host"].take_string()?,
            port: obj["port"].as_u16()?,
            version: obj["version"].take_string()?,
            auth_required: obj["auth_required"].as_bool().unwrap_or(false),
            tls_required: obj["tls_required"].as_bool().unwrap_or(false),
            max_payload: obj["max_payload"].as_usize()?,
            proto: obj["proto"].as_i8()?,
            client_id: obj["client_id"].as_u64()?,
            go: obj["go"].take_string()?,
            nonce: obj["nonce"].take_string().unwrap_or_default(),
            connect_urls: obj["connect_urls"]
                .members_mut()
                .filter_map(|m| m.take_string())
                .collect(),
            client_ip: obj["client_ip"].take_string().unwrap_or_default(),
            headers: obj["headers"].as_bool().unwrap_or(false),
            lame_duck_mode: obj["ldm"].as_bool().unwrap_or(false),
        })
    }
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

#[derive(Clone, Debug)]
pub enum ClientOp {
    Publish { subject: String, payload: Bytes },
    Subscribe { sid: u64, subject: String },
    Unsubscribe { sid: u64 },
    Ping,
    Pong,
    Flush,
}

/// A framed connection
///
/// The type will probably not be public.
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub async fn connect(addrs: impl IntoServerList) -> Result<Connection, io::Error> {
        let a = addrs
            .into_server_list()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let tcp_stream = TcpStream::connect((a.host(), a.port())).await?;
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

                let server_info = ServerInfo::parse(line).ok_or_else(|| {
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

    pub async fn write_op(&mut self, item: &ClientOp) -> Result<(), io::Error> {
        match item {
            ClientOp::Publish { subject, payload } => {
                let mut bufi = itoa::Buffer::new();
                self.stream.write_all(b"PUB ").await?;
                self.stream.write_all(subject.as_bytes()).await?;
                self.stream.write_all(b" ").await?;
                self.stream
                    .write_all(bufi.format(payload.len()).as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                self.stream.write_all(payload).await?;
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
            ClientOp::Flush => {
                self.stream.flush().await?;
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
                            if let Err(err) = self.connection.write_op(&op).await {
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
                                self.connection.write_op(&ClientOp::Pong).await?;
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
        self.sender.send(ClientOp::Flush).await?;
        Ok(())
    }
}

pub async fn connect(addr: impl IntoServerList) -> Result<Client, io::Error> {
    let mut connection = Connection::connect(addr).await?;
    connection.stream.write_all(b"CONNECT { \"no_responders\": true, \"headers\": true, \"verbose\": false, \"pedantic\": false }\r\n").await?;
    connection.stream.write_all(b"PING\r\n").await?;

    let subscription_context = Arc::new(Mutex::new(SubscriptionContext::new()));
    let mut connector = Connector::new(connection, subscription_context.clone());

    // TODO make channel size configurable
    let (sender, receiver) = mpsc::channel(128);
    let client = Client::new(sender.clone(), subscription_context);

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
    subject: String,
    reply_to: Option<String>,
    payload: Bytes,
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

/// Address of a NATS server.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServerAddress(Url);

/// Capability to convert into a list of NATS server addresses.
///
/// There are several implementations ensuring the easy passing of one or more server addresses to
/// functions like [`crate::connect()`].
pub trait IntoServerList {
    /// Convert the instance into a list of [`ServerAddress`]es.
    fn into_server_list(self) -> io::Result<Vec<ServerAddress>>;
}

impl FromStr for ServerAddress {
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

impl ServerAddress {
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

impl<'s> IntoServerList for &'s str {
    fn into_server_list(self) -> io::Result<Vec<ServerAddress>> {
        self.split(',').map(|url| url.parse()).collect()
    }
}

impl<'s> IntoServerList for &'s [&'s str] {
    fn into_server_list(self) -> io::Result<Vec<ServerAddress>> {
        self.iter().map(|url| url.parse()).collect()
    }
}

impl<'s, const N: usize> IntoServerList for &'s [&'s str; N] {
    fn into_server_list(self) -> io::Result<Vec<ServerAddress>> {
        self.as_ref().into_server_list()
    }
}

impl IntoServerList for String {
    fn into_server_list(self) -> io::Result<Vec<ServerAddress>> {
        self.as_str().into_server_list()
    }
}

impl<'s> IntoServerList for &'s String {
    fn into_server_list(self) -> io::Result<Vec<ServerAddress>> {
        self.as_str().into_server_list()
    }
}

impl IntoServerList for ServerAddress {
    fn into_server_list(self) -> io::Result<Vec<ServerAddress>> {
        Ok(vec![self])
    }
}

impl IntoServerList for Vec<ServerAddress> {
    fn into_server_list(self) -> io::Result<Vec<ServerAddress>> {
        Ok(self)
    }
}

impl IntoServerList for io::Result<Vec<ServerAddress>> {
    fn into_server_list(self) -> io::Result<Vec<ServerAddress>> {
        self
    }
}
