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

use crate::auth::Auth;
use crate::client::Statistics;
#[cfg(feature = "websockets")]
use crate::connection::AsyncReadWrite;
use crate::connection::Connection;
use crate::connection::State;
#[cfg(feature = "websockets")]
use crate::connection::WebSocketAdapter;
use crate::options::CallbackArg1;
use crate::tls;
use crate::AuthError;
use crate::ClientError;
use crate::ClientOp;
use crate::ConnectError;
use crate::ConnectErrorKind;
use crate::ConnectInfo;
use crate::Event;
use crate::Protocol;
use crate::ServerAddr;
use crate::ServerError;
use crate::ServerInfo;
use crate::ServerOp;
use crate::SetServerPoolError;
use crate::SetServerPoolErrorKind;
use crate::SocketAddr;
use crate::ToServerAddrs;
use crate::LANG;
use crate::VERSION;
#[cfg(feature = "nkeys")]
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
#[cfg(feature = "nkeys")]
use base64::engine::Engine;
use rand::rng;
use rand::seq::SliceRandom;
use std::borrow::Cow;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::sleep;
use tokio_rustls::rustls;
use url::Url;

/// Metadata about a server in the connection pool.
///
/// This is a snapshot of the server's state at the time it was queried.
/// Modifying this struct has no effect on the connection.
///
/// # Examples
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// let client = async_nats::connect("demo.nats.io").await?;
/// let pool = client.server_pool().await?;
/// for server in &pool {
///     println!(
///         "{:?}: {} failed attempts, connected: {}",
///         server.addr, server.failed_attempts, server.did_connect
///     );
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Server {
    /// The server address.
    pub addr: ServerAddr,
    /// Number of consecutive failed connection attempts to this server.
    /// Reset to zero on a successful connection.
    pub failed_attempts: usize,
    /// Whether the client has ever successfully connected to this server.
    pub did_connect: bool,
    /// Whether this server was discovered from a cluster INFO message
    /// rather than explicitly configured by the user.
    pub is_discovered: bool,
    /// The last connection error for this server, if any.
    pub last_error: Option<String>,
}

impl fmt::Display for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.addr.as_url_str())
    }
}

/// The result of a reconnect-to-server callback, specifying which server
/// to connect to and how long to wait before attempting.
///
/// Returned from the callback set via
/// [`ConnectOptions::reconnect_to_server_callback`][crate::ConnectOptions::reconnect_to_server_callback].
#[derive(Debug, Clone)]
pub struct ReconnectToServer {
    /// The server to connect to. Must be from the pool provided to the callback;
    /// if not, the library falls back to default server selection.
    pub addr: ServerAddr,
    /// Delay before connecting. [`None`] uses the default reconnect delay
    /// (exponential backoff). `Some(Duration::ZERO)` reconnects immediately.
    pub delay: Option<Duration>,
}

pub(crate) type ReconnectToServerCallback =
    CallbackArg1<(Vec<Server>, ServerInfo), Option<ReconnectToServer>>;

impl Server {
    fn new(addr: ServerAddr) -> Self {
        Server {
            addr,
            failed_attempts: 0,
            did_connect: false,
            is_discovered: false,
            last_error: None,
        }
    }

    fn new_discovered(addr: ServerAddr) -> Self {
        Server {
            addr,
            failed_attempts: 0,
            did_connect: false,
            is_discovered: true,
            last_error: None,
        }
    }
}

pub(crate) struct ConnectorOptions {
    pub(crate) tls_required: bool,
    pub(crate) certificates: Vec<PathBuf>,
    pub(crate) client_cert: Option<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) tls_client_config: Option<rustls::ClientConfig>,
    pub(crate) tls_first: bool,
    pub(crate) tls_optional: bool,
    pub(crate) tls_server_name: Option<String>,
    pub(crate) verify_discovered_servers_by_ip: bool,
    pub(crate) auth: Auth,
    pub(crate) no_echo: bool,
    pub(crate) connection_timeout: Duration,
    pub(crate) name: Option<String>,
    pub(crate) ignore_discovered_servers: bool,
    pub(crate) retain_servers_order: bool,
    pub(crate) read_buffer_capacity: u16,
    pub(crate) reconnect_delay_callback: Arc<dyn Fn(usize) -> Duration + Send + Sync + 'static>,
    pub(crate) auth_callback: Option<CallbackArg1<Vec<u8>, Result<Auth, AuthError>>>,
    pub(crate) max_reconnects: Option<usize>,
    pub(crate) local_address: Option<SocketAddr>,
    pub(crate) reconnect_to_server_callback: Option<ReconnectToServerCallback>,
}

/// Maintains a list of servers and establishes connections.
pub(crate) struct Connector {
    /// Server pool with per-server metadata.
    servers: Vec<Server>,
    options: ConnectorOptions,
    /// The setting or URL that asked for TLS, if any, as error messages name it. When set,
    /// every attempt to every server uses TLS, discovered servers and reconnects included,
    /// and nothing the server sends turns it off.
    tls_asked_by: Option<Cow<'static, str>>,
    pub(crate) connect_stats: Arc<Statistics>,
    attempts: usize,
    pub(crate) events_tx: tokio::sync::mpsc::Sender<Event>,
    pub(crate) state_tx: tokio::sync::watch::Sender<State>,
    pub(crate) max_payload: Arc<AtomicUsize>,
    /// Last known server info, updated after each successful connection.
    last_info: ServerInfo,
    /// What the client learned about each discovered server, keyed by host and port.
    discovered: HashMap<(String, u16), Discovered>,
    /// Servers of the pool that keep TLS because the client learned them over TLS before a
    /// pool listed them, and every pool since has listed them, keyed by host and port. Empty
    /// under `tls_optional`.
    kept_tls: HashSet<(String, u16)>,
    /// The server of the established connection. `None` while the client connects.
    current: Option<CurrentServer>,
}

/// What the client learned about a discovered server from the connections it was
/// reported on.
struct Discovered {
    /// It was reported on a connection with TLS, so it is connected to with TLS. Once
    /// set, it stays set.
    tls: bool,
    /// The host name of the server that reported it, over any number of hops: the name
    /// that server was verified under when the report came over TLS, else the name of the
    /// configured server the client learned it through. A discovered IP address is one
    /// more address for that name and is verified against it. `None` when that server was
    /// reached by an IP address.
    name: Option<String>,
    /// Whether the client logged that it has no name to verify the server against.
    no_name_logged: bool,
}

/// The server of the established connection, and whether that connection has TLS.
#[derive(Clone)]
struct CurrentServer {
    addr: ServerAddr,
    security: Security,
}

/// Whether a connection has TLS.
#[derive(Clone)]
enum Security {
    Plaintext,
    /// TLS, with the name the certificate was verified against.
    Tls {
        name: String,
    },
}

/// A server's host and port as written: two entries with the same key are one server.
fn server_key(addr: &ServerAddr) -> (String, u16) {
    (addr.host().to_owned(), addr.port())
}

pub(crate) fn reconnect_delay_callback_default(attempts: usize) -> Duration {
    if attempts <= 1 {
        Duration::from_millis(0)
    } else {
        let exp: u32 = (attempts - 1).try_into().unwrap_or(u32::MAX);
        let max = Duration::from_secs(4);
        cmp::min(Duration::from_millis(2_u64.saturating_pow(exp)), max)
    }
}

/// Whether the scheme of the URL asks for TLS.
fn url_asks_for_tls(addr: &ServerAddr) -> bool {
    matches!(addr.scheme(), "tls" | "wss")
}

/// Names the setting that asks for TLS, if any: an option by its name, a `tls://` or
/// `wss://` URL of the list by the first such URL, without its user info. `tls_first` is
/// read before `require_tls`, which it also sets. With `tls_optional` nothing asks for TLS;
/// a `tls://` or `wss://` URL then gets TLS by its own scheme only.
fn tls_asked_by(options: &ConnectorOptions, servers: &[Server]) -> Option<Cow<'static, str>> {
    let tls_url = servers.iter().find(|server| url_asks_for_tls(&server.addr));
    if options.tls_optional {
        None
    } else if options.tls_first {
        Some("tls_first()".into())
    } else if options.tls_required {
        Some("require_tls(true)".into())
    } else if let Some(server) = tls_url {
        Some(format!("the URL {:?}", without_user_info(&server.addr)).into())
    } else if !options.certificates.is_empty() {
        Some("add_root_certificates()".into())
    } else if options.client_cert.is_some() {
        Some("add_client_certificate()".into())
    } else if options.tls_client_config.is_some() {
        Some("tls_client_config()".into())
    } else if options.tls_server_name.is_some() {
        Some("tls_server_name()".into())
    } else if options.verify_discovered_servers_by_ip {
        Some("verify_discovered_servers_by_ip()".into())
    } else {
        None
    }
}

/// The URL as an error message prints it: with its username and password cleared.
fn without_user_info(addr: &ServerAddr) -> String {
    let mut url = addr.clone().into_inner();
    // Clearing fails only for a URL without a host, which a server URL always has.
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.into()
}

/// The configuration error for a URL in `addrs` that this build or the client's settings
/// cannot connect to, if there is one: a WebSocket URL in a build without the `websockets`
/// feature, or a `ws://` URL while `tls_asked_by` names a setting or URL that asks for
/// TLS. The server list at connect and a pool given later are checked alike.
///
/// A `ws://` URL cannot be upgraded: in WebSocket, TLS is fixed by the scheme before the
/// opening handshake, so dialing `ws://` with TLS would replace the scheme the user wrote,
/// and dialing it as written would leave out the TLS asked for.
fn refused_url<'a>(
    tls_asked_by: Option<&str>,
    mut addrs: impl Iterator<Item = &'a ServerAddr> + Clone,
) -> Option<String> {
    #[cfg(not(feature = "websockets"))]
    if addrs.clone().any(ServerAddr::is_websocket) {
        return Some("a ws:// or wss:// URL needs the websockets feature of async-nats".into());
    }
    let asked_by = tls_asked_by?;
    let addr = addrs.find(|addr| addr.scheme() == "ws")?;
    Some(format!(
        "{asked_by} asks for TLS, but {:?} is a ws:// URL; wss:// is the TLS form",
        without_user_info(addr)
    ))
}

/// The address of a `connect_urls` entry learned on a WebSocket connection, or on a TCP
/// connection, or why the entry is skipped. The address has the scheme of that transport,
/// `ws` or `nats`; whether it gets TLS is not the scheme's to say.
///
/// nats-server writes an entry as `host:port`, with an IPv6 address in brackets. User info
/// in an entry is dropped: the server is connected to with the client's own credentials. An
/// entry with a scheme is skipped rather than reduced to `host:port`, and so is anything
/// else that is not `host:port`.
fn discovered_addr(entry: &str, websocket: bool) -> Result<ServerAddr, &'static str> {
    const NOT_HOST_PORT: &str = "it is not host:port";
    if entry.contains("://") {
        return Err("it has a scheme");
    }
    let url = Url::parse(&format!("nats://{entry}")).map_err(|_| NOT_HOST_PORT)?;
    match (url.host_str(), url.port()) {
        (Some(host), Some(port))
            if !host.is_empty()
                && url.path().is_empty()
                && url.query().is_none()
                && url.fragment().is_none() =>
        {
            let scheme = if websocket { "ws" } else { "nats" };
            format!("{scheme}://{host}:{port}")
                .parse()
                .map_err(|_| NOT_HOST_PORT)
        }
        _ => Err(NOT_HOST_PORT),
    }
}

/// Says that the discovered server `addr` has no name to verify its certificate against,
/// and which settings give it one. The caller names the server.
fn no_tls_name(addr: &ServerAddr) -> String {
    let remedy = if addr.host().parse::<IpAddr>().is_ok() {
        "tls_server_name() sets the name to verify every server against, or \
         verify_discovered_servers_by_ip() verifies a discovered IP address against itself, \
         which is safe only where the CA that issued the certificates is not shared with others"
    } else {
        "tls_server_name() sets the name to verify every server against"
    };
    format!("a discovered server with no name to verify its certificate against; {remedy}")
}

/// A name a certificate presents, as rustls reports it (`DnsName("nats.example.com")`,
/// `IpAddress(10.0.0.1)`), without the wrapper, and an IP address in its usual form.
fn presented_name(name: &str) -> String {
    if let Some(dns) = name
        .strip_prefix("DnsName(\"")
        .and_then(|name| name.strip_suffix("\")"))
    {
        return dns.to_owned();
    }
    name.strip_prefix("IpAddress(")
        .and_then(|name| name.strip_suffix(')'))
        .and_then(|ip| ip.parse::<IpAddr>().ok())
        .map_or_else(|| name.to_owned(), |ip| ip.to_string())
}

/// A failed name check as an attempt reports it: the client's message, with the
/// `io::Error` that carries the error rustls reported as its source.
#[derive(Debug)]
struct NameCheckFailed {
    message: String,
    source: std::io::Error,
}

impl fmt::Display for NameCheckFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for NameCheckFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// A text the server chose, such as a `connect_urls` entry or a URL with its host, as a log
/// line quotes it: cut at 256 bytes and escaped, since the server chose it.
fn quoted(entry: &str) -> String {
    let mut end = entry.len().min(256);
    while !entry.is_char_boundary(end) {
        end -= 1;
    }
    format!("{:?}", &entry[..end])
}

/// Whether the server of an INFO with `version` sends another INFO after the first PONG,
/// which nats-server does from 2.12.0 on. As nats-server compares versions, only the three
/// numbers count, so a pre-release counts as its release. A version whose part before any
/// `-` or `+` is not exactly three numbers counts as earlier.
fn sends_info_after_first_pong(version: &str) -> bool {
    let core = version.split(['-', '+']).next().unwrap_or_default();
    let mut parts = core.split('.').map(str::parse::<u64>);
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(Ok(major)), Some(Ok(minor)), Some(Ok(patch)), None)
            if (major, minor, patch) >= (2, 12, 0)
    )
}

/// Reads the next operation of an attempt after CONNECT. A PING is answered and `+OK` is
/// skipped; a `-ERR`, a line that does not parse and a closed connection fail the attempt.
async fn next_op(connection: &mut Connection) -> Result<ServerOp, ConnectError> {
    loop {
        match connection.read_op().await? {
            Some(ServerOp::Ping) => {
                connection
                    .easy_write_and_flush([ClientOp::Pong].iter())
                    .await?;
            }
            Some(ServerOp::Ok) => {}
            Some(ServerOp::Error(err)) => {
                return Err(match err {
                    ServerError::AuthorizationViolation => {
                        tracing::error!(error = %err, "authorization violation");
                        ConnectError::with_source(
                            crate::ConnectErrorKind::AuthorizationViolation,
                            err,
                        )
                    }
                    err => {
                        tracing::error!(error = %err, "server error during connection");
                        ConnectError::with_source(crate::ConnectErrorKind::Io, err)
                    }
                });
            }
            Some(op) => return Ok(op),
            None => {
                tracing::error!("connection closed unexpectedly");
                return Err(ConnectError::with_source(
                    crate::ConnectErrorKind::Io,
                    "broken pipe",
                ));
            }
        }
    }
}

/// A connection up to the first PONG, as an attempt opens it.
pub(crate) struct Handshake {
    /// The INFO of the connection.
    info: ServerInfo,
    connection: Connection,
    /// Whether the connection has TLS.
    tls: bool,
}

impl Connector {
    pub(crate) fn new<A: ToServerAddrs>(
        addrs: A,
        options: ConnectorOptions,
        events_tx: tokio::sync::mpsc::Sender<Event>,
        state_tx: tokio::sync::watch::Sender<State>,
        max_payload: Arc<AtomicUsize>,
        connect_stats: Arc<Statistics>,
    ) -> Result<Connector, ConnectError> {
        let servers: Vec<Server> = addrs
            .to_server_addrs()
            .map_err(|err| ConnectError::with_source(ConnectErrorKind::ServerParse, err))?
            .map(Server::new)
            .collect();

        // Refused here, before any attempt, so that retry_on_initial_connect cannot
        // retry what no attempt can fix.
        if options.tls_optional && options.tls_first {
            return Err(ConnectError::with_source(
                ConnectErrorKind::Configuration,
                "tls_optional() cannot be set together with tls_first()",
            ));
        }
        if options.tls_optional && options.tls_required {
            return Err(ConnectError::with_source(
                ConnectErrorKind::Configuration,
                "tls_optional() cannot be set together with require_tls(true)",
            ));
        }
        let tls_asked_by = tls_asked_by(&options, &servers);
        if let Some(message) = refused_url(
            tls_asked_by.as_deref(),
            servers.iter().map(|server| &server.addr),
        ) {
            return Err(ConnectError::with_source(
                ConnectErrorKind::Configuration,
                message,
            ));
        }

        Ok(Connector {
            attempts: 0,
            servers,
            options,
            tls_asked_by,
            events_tx,
            state_tx,
            max_payload,
            connect_stats,
            last_info: ServerInfo::default(),
            discovered: HashMap::new(),
            kept_tls: HashSet::new(),
            current: None,
        })
    }

    /// Replaces the server pool. Preserves per-server state for servers that
    /// appear in both the old and new pools. Every server in the new pool is a configured
    /// server, verified as its URL says. One the client learned over TLS keeps TLS as long
    /// as every pool given since lists it, unless `tls_optional` is set: a pool URL then
    /// keeps its own scheme. A pool that leaves out a server, or a list from the cluster
    /// that leaves out a discovered server other than the current one, drops what the
    /// client learned about it.
    ///
    /// A `tls://` or `wss://` URL in the pool asks for TLS as it does at connect, unless
    /// `tls_optional` is set: every later attempt uses TLS, and a later pool does not turn
    /// it off.
    ///
    /// Returns an error, and leaves the pool and whether the client asks for TLS as they
    /// were, if the pool mixes WebSocket (`ws://`, `wss://`) and non-websocket (`nats://`,
    /// `tls://`) schemes, or holds a URL that the server list at connect would have been
    /// refused for: a `ws://` URL is refused when the client asks for TLS, or would once
    /// the pool is taken.
    pub(crate) fn set_server_pool(
        &mut self,
        addrs: Vec<ServerAddr>,
    ) -> Result<(), SetServerPoolError> {
        if addrs.is_empty() {
            return Err(SetServerPoolError::new(SetServerPoolErrorKind::EmptyPool));
        }

        // Validate: cannot mix websocket and non-websocket schemes.
        let has_ws = addrs.iter().any(|a| a.is_websocket());
        let has_non_ws = addrs.iter().any(|a| !a.is_websocket());
        if has_ws && has_non_ws {
            return Err(SetServerPoolError::new(
                SetServerPoolErrorKind::MixedSchemes,
            ));
        }

        let tls_asked_by = self.tls_asked_by.clone().or_else(|| {
            addrs
                .iter()
                .find(|addr| !self.options.tls_optional && url_asks_for_tls(addr))
                // A URL that server_pool() listed can carry a host the server chose: it is
                // cut and escaped.
                .map(|addr| {
                    format!(
                        "the URL {} given to set_server_pool()",
                        quoted(&without_user_info(addr))
                    )
                    .into()
                })
        });
        if let Some(message) = refused_url(tls_asked_by.as_deref(), addrs.iter()) {
            return Err(SetServerPoolError::with_source(
                SetServerPoolErrorKind::Configuration,
                message,
            ));
        }
        self.tls_asked_by = tls_asked_by;

        // A server keeps its state when the new pool lists it as server_pool() did.
        let listed = self.server_pool();
        let new_servers = addrs
            .into_iter()
            .map(|addr| {
                if let Some(existing) = listed.iter().find(|s| s.addr == addr) {
                    Server {
                        addr,
                        failed_attempts: existing.failed_attempts,
                        did_connect: existing.did_connect,
                        is_discovered: false,
                        last_error: existing.last_error.clone(),
                    }
                } else {
                    Server::new(addr)
                }
            })
            .collect();
        self.servers = new_servers;
        if !self.options.tls_optional {
            // A ws:// URL is dialed as written, also when a TCP server with its host and port
            // was learned over TLS: without tls_optional, a WebSocket connection has TLS only
            // when the client asks for TLS, and a ws:// URL is then refused.
            self.kept_tls = self
                .servers
                .iter()
                .filter(|server| !server.addr.is_websocket())
                .map(|server| server_key(&server.addr))
                .filter(|key| {
                    self.kept_tls.contains(key)
                        || self
                            .discovered
                            .get(key)
                            .is_some_and(|discovered| discovered.tls)
                })
                .collect();
        }
        self.discovered.clear();
        self.attempts = 0;
        Ok(())
    }

    /// Returns a snapshot of the current server pool, each server as `listed_addr` gives it.
    pub(crate) fn server_pool(&self) -> Vec<Server> {
        self.servers
            .iter()
            .map(|server| Server {
                addr: self.listed_addr(&server.addr),
                ..server.clone()
            })
            .collect()
    }

    /// `addr` as the pool is listed: a discovered server learned over TLS with the TLS form
    /// of its scheme, `tls://` or `wss://`, so that a pool made from the list keeps TLS for
    /// it, also under `tls_optional`. Other servers as they are.
    fn listed_addr(&self, addr: &ServerAddr) -> ServerAddr {
        let learned_over_tls = self
            .discovered
            .get(&server_key(addr))
            .is_some_and(|discovered| discovered.tls);
        if !learned_over_tls {
            return addr.clone();
        }
        let mut url = addr.clone().into_inner();
        // nats and tls are both non-special schemes, ws and wss both special: the change
        // cannot fail, and the URL stays a server address. The URL holds no default port,
        // such as 80 for ws, so the port is set again to stay the same.
        let _ = url.set_scheme(if addr.is_websocket() { "wss" } else { "tls" });
        let _ = url.set_port(Some(addr.port()));
        ServerAddr::from_url(url).unwrap_or_else(|_| addr.clone())
    }

    /// Whether `addr` was learned on a connection with TLS and keeps TLS for it: as a
    /// discovered server, or as a server of the pool in `kept_tls`.
    fn learned_over_tls(&self, addr: &ServerAddr) -> bool {
        let key = server_key(addr);
        self.kept_tls.contains(&key)
            || self
                .discovered
                .get(&key)
                .is_some_and(|discovered| discovered.tls)
    }

    /// The name the certificate of `addr` is verified against, and sent as SNI unless it
    /// is an IP address, or `None` when there is no such name.
    ///
    /// The user's `tls_server_name` comes first, for every server. Without it, a configured
    /// server is verified as configured, against the host of its URL. A discovered IP
    /// address is verified against itself under `verify_discovered_servers_by_ip`, and
    /// else against the host name it was learned under. A discovered host name is verified
    /// against itself when it was learned over TLS, and has no name otherwise: a name
    /// learned without TLS is not trusted.
    fn tls_name<'a>(&'a self, addr: &'a ServerAddr) -> Option<&'a str> {
        if let Some(name) = &self.options.tls_server_name {
            return Some(name);
        }
        let host = addr.host();
        let ip = host.parse::<IpAddr>().is_ok();
        match self.discovered.get(&server_key(addr)) {
            None => Some(host),
            Some(_) if ip && self.options.verify_discovered_servers_by_ip => Some(host),
            Some(discovered) if ip => discovered.name.as_deref(),
            Some(discovered) => discovered.tls.then_some(host),
        }
    }

    /// Whether `addr` is skipped: it is connected to with TLS whatever its INFO says, and
    /// has no name to verify its certificate against. Logged once per server.
    fn skip_without_tls_name(&mut self, addr: &ServerAddr) -> bool {
        if self.tls_name(addr).is_some()
            || (self.tls_asked_by.is_none() && !self.learned_over_tls(addr))
        {
            return false;
        }
        if let Some(discovered) = self.discovered.get_mut(&server_key(addr)) {
            if !discovered.no_name_logged {
                discovered.no_name_logged = true;
                // The server chose the host, and nothing has looked it up yet: it is cut.
                tracing::warn!(
                    "skipping {}: {}",
                    quoted(&without_user_info(addr)),
                    no_tls_name(addr)
                );
            }
        }
        true
    }

    /// Adds the servers in `connect_urls`, learned on a connection to `reporter` that has
    /// TLS when `tls` is set, to the pool as discovered servers.
    ///
    /// A discovered server is connected to with the transport of the connection it was
    /// learned on, and with TLS when that connection had TLS. Learned again on a
    /// connection with TLS, it gains TLS, and it never loses it. Two entries are one
    /// server when host and port match as written; an entry that is a configured server
    /// changes nothing about it.
    ///
    /// A discovered IP address is verified against the name `reporter` was verified under
    /// when `tls` is set, or else against the name of the configured server the client
    /// learned it through. The first report decides the name, unless a later one comes
    /// over TLS and the server gains TLS: it then takes the name of that report. A server
    /// the pool does not list, such as the current server after a pool that leaves it out,
    /// lends no name to what it lists without TLS; with TLS it lends the name its connection
    /// was verified under.
    fn add_discovered_servers(
        &mut self,
        connect_urls: &[String],
        reporter: &ServerAddr,
        tls: bool,
    ) {
        let reporter_key = server_key(reporter);
        let name = if tls {
            match &self.current {
                Some(current) if server_key(&current.addr) == reporter_key => {
                    match &current.security {
                        Security::Tls { name } => Some(name.as_str()),
                        Security::Plaintext => None,
                    }
                }
                _ => self.tls_name(reporter),
            }
        } else {
            match self.discovered.get(&reporter_key) {
                Some(discovered) => discovered.name.as_deref(),
                None => self
                    .servers
                    .iter()
                    .any(|server| server_key(&server.addr) == reporter_key)
                    .then_some(reporter.host()),
            }
        }
        .filter(|name| name.parse::<IpAddr>().is_err())
        .map(str::to_owned);
        for url in connect_urls {
            let discovered_addr = match discovered_addr(url, reporter.is_websocket()) {
                Ok(addr) => addr,
                Err(reason) => {
                    tracing::warn!("skipping the connect_urls entry {}: {reason}", quoted(url));
                    continue;
                }
            };
            let key = server_key(&discovered_addr);
            if let Some(discovered) = self.discovered.get_mut(&key) {
                if tls && !discovered.tls {
                    discovered.tls = true;
                    discovered.name = name.clone();
                }
            } else if !self.servers.iter().any(|s| server_key(&s.addr) == key) {
                tracing::debug!(
                    discovered_url = discovered_addr.as_url_str(),
                    "adding discovered server"
                );
                self.discovered.insert(
                    key,
                    Discovered {
                        tls,
                        name: name.clone(),
                        no_name_logged: false,
                    },
                );
                self.servers.push(Server::new_discovered(discovered_addr));
            }
        }
    }

    /// Replaces the discovered servers with those in `connect_urls`, from an INFO that
    /// arrived on the established connection to `current`, which has TLS when `tls` is set.
    /// The configured servers and `current` are kept. A discovered server that drops out of
    /// the list and comes back is learned again, from the report that brings it back. An
    /// INFO without servers changes nothing.
    fn replace_discovered_servers(
        &mut self,
        connect_urls: &[String],
        current: &ServerAddr,
        tls: bool,
    ) {
        if connect_urls.is_empty() {
            return;
        }
        let kept: HashSet<(String, u16)> = connect_urls
            .iter()
            .filter_map(|url| discovered_addr(url, current.is_websocket()).ok())
            .map(|addr| server_key(&addr))
            .chain([server_key(current)])
            .collect();
        self.servers
            .retain(|server| !server.is_discovered || kept.contains(&server_key(&server.addr)));
        self.discovered.retain(|key, _| kept.contains(key));
        self.add_discovered_servers(connect_urls, current, tls);
    }

    pub(crate) async fn connect(&mut self) -> Result<(ServerInfo, Connection), ConnectError> {
        loop {
            match self.try_connect().await {
                Ok(inner) => {
                    return Ok(inner);
                }
                Err(error) => match error.kind() {
                    ConnectErrorKind::MaxReconnects => {
                        return Err(ConnectError::with_source(
                            crate::ConnectErrorKind::MaxReconnects,
                            error,
                        ))
                    }
                    // A TLS error's text says what asked for TLS or why a certificate cannot be
                    // verified; the server addresses and names it quotes are escaped. Other errors
                    // keep only their kind: their text can quote a line from the server, which is
                    // not escaped.
                    ConnectErrorKind::Tls => {
                        self.events_tx
                            .try_send(Event::ClientError(ClientError::Other(error.to_string())))
                            .ok();
                    }
                    other => {
                        self.events_tx
                            .try_send(Event::ClientError(ClientError::Other(other.to_string())))
                            .ok();
                    }
                },
            }
        }
    }

    pub(crate) async fn try_connect(&mut self) -> Result<(ServerInfo, Connection), ConnectError> {
        tracing::debug!(attempt = %self.attempts, "connecting to server");
        // The servers a new connection lists take the name that connection is verified
        // under, not that of an earlier connection to the same server.
        self.current = None;
        let mut error = None;

        // If a reconnect-to-server callback is set, try it first.
        if let Some(ref callback) = self.options.reconnect_to_server_callback {
            let pool_snapshot = self.server_pool();
            let info_snapshot = self.last_info.clone();
            let selection = callback.call((pool_snapshot, info_snapshot)).await;

            // A selected server with no name to verify against is skipped as below.
            if let Some(target) =
                selection.filter(|target| !self.skip_without_tls_name(&target.addr))
            {
                // Validate the selected server is in the pool as the snapshot lists it, and
                // connect to it by the address the pool keeps for it.
                let selected = self
                    .servers
                    .iter()
                    .map(|server| &server.addr)
                    .find(|addr| self.listed_addr(addr) == target.addr)
                    .cloned();
                if let Some(addr) = selected {
                    self.attempts += 1;
                    if let Some(max_reconnects) = self.options.max_reconnects {
                        if self.attempts > max_reconnects {
                            self.events_tx
                                .try_send(Event::ClientError(ClientError::MaxReconnects))
                                .ok();
                            return Err(ConnectError::new(crate::ConnectErrorKind::MaxReconnects));
                        }
                    }

                    // Use the callback's delay if specified, otherwise fall back
                    // to the default reconnect delay to prevent tight-loop spinning.
                    let delay = match target.delay {
                        Some(d) => d,
                        None => (self.options.reconnect_delay_callback)(self.attempts),
                    };
                    if !delay.is_zero() {
                        sleep(delay).await;
                    }

                    match self.try_connect_to_server(&addr).await {
                        Ok(result) => return Ok(result),
                        Err(inner) => match inner.kind() {
                            ConnectErrorKind::AuthorizationViolation
                            | ConnectErrorKind::Authentication => return Err(inner),
                            _ => {
                                tracing::debug!(
                                    server = ?target.addr,
                                    error = %inner,
                                    "callback-selected server connection failed"
                                );
                                error.replace(inner);
                            }
                        },
                    }
                    return Err(error.unwrap());
                } else {
                    tracing::warn!(
                        server = ?target.addr,
                        "reconnect callback returned server not in pool, using default selection"
                    );
                    self.events_tx
                        .try_send(Event::ClientError(ClientError::ServerNotInPool))
                        .ok();
                    // Fall through to default selection below.
                }
            }
            // Callback returned None or invalid server — fall through to default.
        }

        // Default server selection: shuffle, sort by failure count, iterate.
        let mut servers = self.servers.clone();
        if !self.options.retain_servers_order {
            servers.shuffle(&mut rng());
            // sort_by is stable, meaning it will retain the order for equal elements.
            servers.sort_by_key(|a| a.failed_attempts);
        }

        // A server with no name to verify its certificate against is skipped: it costs no
        // attempt and no sleep. A configured server always has a name.
        servers.retain(|server| !self.skip_without_tls_name(&server.addr));

        for entry in servers {
            self.attempts += 1;
            if let Some(max_reconnects) = self.options.max_reconnects {
                if self.attempts > max_reconnects {
                    tracing::error!(
                        attempts = %self.attempts,
                        max_reconnects = %max_reconnects,
                        "max reconnection attempts reached"
                    );
                    self.events_tx
                        .try_send(Event::ClientError(ClientError::MaxReconnects))
                        .ok();
                    return Err(ConnectError::new(crate::ConnectErrorKind::MaxReconnects));
                }
            }

            let duration = (self.options.reconnect_delay_callback)(self.attempts);
            tracing::debug!(
                attempt = %self.attempts,
                server = ?entry.addr,
                delay_ms = %duration.as_millis(),
                "attempting connection"
            );

            sleep(duration).await;

            match self.try_connect_to_server(&entry.addr).await {
                Ok(result) => return Ok(result),
                Err(inner) => match inner.kind() {
                    ConnectErrorKind::AuthorizationViolation | ConnectErrorKind::Authentication => {
                        return Err(inner);
                    }
                    _ => {
                        tracing::debug!(
                            server = ?entry.addr,
                            error = %inner,
                            "connection attempt failed"
                        );
                        error.replace(inner);
                    }
                },
            }
        }

        Err(error.unwrap_or_else(|| {
            ConnectError::with_source(
                crate::ConnectErrorKind::Io,
                "all connection attempts failed",
            )
        }))
    }

    /// Attempts to connect to a specific server address, handling DNS resolution,
    /// timeout, and updating per-server state.
    async fn try_connect_to_server(
        &mut self,
        server_addr: &ServerAddr,
    ) -> Result<(ServerInfo, Connection), ConnectError> {
        let socket_addrs = server_addr
            .socket_addrs()
            .await
            .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Dns, err))?;

        // A server learned on a connection with TLS keeps TLS, whatever its INFO says.
        let tls_required = server_addr.tls_required() || self.learned_over_tls(server_addr);
        let mut last_err = None;
        for socket_addr in socket_addrs {
            let attempt = tokio::time::timeout(
                self.options.connection_timeout,
                self.try_connect_to(&socket_addr, tls_required, server_addr.clone()),
            )
            .await;
            // The wait for the INFO that follows the first PONG has a timer of its own.
            let attempt = match attempt {
                Ok(Ok(Handshake {
                    info,
                    mut connection,
                    tls,
                })) => self
                    .info_after_first_pong(server_addr, info, &mut connection, tls)
                    .await
                    .map(|info| Handshake {
                        info,
                        connection,
                        tls,
                    }),
                Ok(Err(err)) => Err(err),
                Err(_) => {
                    tracing::debug!(
                        server = ?server_addr,
                        "connection handshake timed out"
                    );
                    Err(ConnectError::new(crate::ConnectErrorKind::TimedOut))
                }
            };
            match attempt {
                Ok(Handshake {
                    info: server_info,
                    connection,
                    tls,
                }) => {
                    tracing::info!(
                        server = %server_info.port,
                        max_payload = %server_info.max_payload,
                        "connected successfully"
                    );
                    self.attempts = 0;
                    self.connect_stats.connects.add(1, Ordering::Relaxed);
                    self.events_tx.try_send(Event::Connected).ok();
                    self.state_tx.send(State::Connected).ok();
                    self.max_payload.store(
                        server_info.max_payload,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    self.last_info = server_info.clone();
                    let security = match self.tls_name(server_addr) {
                        Some(name) if tls => Security::Tls {
                            name: name.to_owned(),
                        },
                        _ => Security::Plaintext,
                    };
                    self.current = Some(CurrentServer {
                        addr: server_addr.clone(),
                        security,
                    });

                    // Update per-server state on success.
                    if let Some(entry) = self.servers.iter_mut().find(|s| s.addr == *server_addr) {
                        entry.did_connect = true;
                        entry.failed_attempts = 0;
                        entry.last_error = None;
                    }

                    return Ok((server_info, connection));
                }

                Err(inner) => {
                    // Update per-server state on failure.
                    if let Some(entry) = self.servers.iter_mut().find(|s| s.addr == *server_addr) {
                        entry.failed_attempts += 1;
                        entry.last_error = Some(inner.to_string());
                    }
                    last_err = Some(inner);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ConnectError::with_source(crate::ConnectErrorKind::Dns, "no addresses resolved")
        }))
    }

    /// Runs the TLS handshake with `server_addr` on `stream` and verifies the server
    /// certificate against `tls_name`, which `Connector::tls_name` gives for TCP and
    /// WebSocket alike. Without a name there is no handshake.
    async fn tls_handshake<S>(
        &self,
        stream: S,
        server_addr: &ServerAddr,
        tls_name: Option<&str>,
    ) -> Result<tokio_rustls::client::TlsStream<S>, ConnectError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let tls_name = tls_name.ok_or_else(|| {
            ConnectError::with_source(
                crate::ConnectErrorKind::Tls,
                format!(
                    "{:?}: {}",
                    without_user_info(server_addr),
                    no_tls_name(server_addr)
                ),
            )
        })?;
        tracing::debug!("upgrading connection to TLS");
        let tls_config = Arc::new(
            tls::config_tls(&self.options)
                .await
                .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?,
        );
        let tls_connector = tokio_rustls::TlsConnector::from(tls_config);

        let domain = crate::rustls::pki_types::ServerName::try_from(tls_name).map_err(|err| {
            // tls_server_name() overrides every name, so the value is the user's.
            if self.options.tls_server_name.is_some() {
                ConnectError::with_source(
                    crate::ConnectErrorKind::Tls,
                    format!(
                        "tls_server_name() is set to {tls_name:?}, which is neither a \
                         host name nor an IP address"
                    ),
                )
            } else {
                ConnectError::with_source(crate::ConnectErrorKind::Tls, err)
            }
        })?;

        tls_connector
            .connect(domain.to_owned(), stream)
            .await
            .map_err(|err| {
                use rustls::CertificateError::{NotValidForName, NotValidForNameContext};
                let presented = match err.get_ref().and_then(|err| err.downcast_ref()) {
                    Some(rustls::Error::InvalidCertificate(NotValidForNameContext {
                        presented,
                        ..
                    })) => Some(presented.as_slice()),
                    Some(rustls::Error::InvalidCertificate(NotValidForName)) => None,
                    _ => return err.into(),
                };
                ConnectError::with_source(
                    crate::ConnectErrorKind::Tls,
                    NameCheckFailed {
                        message: self.name_check_failed(server_addr, tls_name, presented),
                        source: err,
                    },
                )
            })
    }

    /// The error for a certificate of `addr` that is not valid for `tls_name`: the name
    /// the client expected, the names the certificate presents where rustls reports them
    /// (at most four), and the settings that change the name. A discovered server also
    /// gets the remedy on the server's side. It never suggests turning verification off.
    fn name_check_failed(
        &self,
        addr: &ServerAddr,
        tls_name: &str,
        presented: Option<&[String]>,
    ) -> String {
        const SHOWN: usize = 4;
        let mut message = format!(
            "the certificate of {:?} is not valid for {tls_name:?}",
            without_user_info(addr)
        );
        let names: Vec<String> = presented
            .unwrap_or_default()
            .iter()
            .map(|name| presented_name(name))
            .collect();
        match (presented, names.len()) {
            (None, _) => {}
            (Some(_), 0) => message.push_str(": it is issued for no name"),
            (Some(_), count) => {
                let shown: Vec<String> = names
                    .iter()
                    .take(SHOWN)
                    .map(|name| format!("{name:?}"))
                    .collect();
                message.push_str(&format!(": it is issued for {}", shown.join(", ")));
                if count > SHOWN {
                    message.push_str(&format!(" and {} more", count - SHOWN));
                }
            }
        }
        message.push_str("; tls_server_name() sets the name every server is verified against");
        if self.options.tls_server_name.is_some()
            || !self.discovered.contains_key(&server_key(addr))
        {
            return message;
        }
        message.push_str(
            ", or each server of the cluster can advertise a name its certificate is issued \
             for (client_advertise in nats-server)",
        );
        let ip = addr.host().parse::<IpAddr>().ok();
        if ip.is_some()
            && names.iter().any(|name| name.parse::<IpAddr>().ok() == ip)
            && !self.options.verify_discovered_servers_by_ip
        {
            message.push_str(
                ", or verify_discovered_servers_by_ip() verifies a discovered IP address \
                 against itself, which is safe only where the CA that issued the certificates \
                 is not shared with others",
            );
        }
        message
    }

    /// Connects to `socket_addr`, an address of `server_addr`, up to the first PONG, and
    /// returns the first INFO, the connection and whether the connection has TLS.
    pub(crate) async fn try_connect_to(
        &mut self,
        socket_addr: &SocketAddr,
        tls_required: bool,
        server_addr: ServerAddr,
    ) -> Result<Handshake, ConnectError> {
        tracing::debug!(
            socket_addr = %socket_addr,
            tls_required = %tls_required,
            "establishing connection"
        );
        let tcp_stream = if let Some(local_addr) = self.options.local_address {
            let socket = if local_addr.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            socket
                .bind(local_addr)
                .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Io, err))?;
            socket.connect(*socket_addr).await?
        } else {
            TcpStream::connect(socket_addr).await?
        };
        tcp_stream.set_nodelay(true)?;

        // A discovered server without a name is not connected to over TLS. It is skipped
        // when it is known to need TLS; it fails here when its INFO asks for TLS.
        let tls_name = self.tls_name(&server_addr);

        // Whether this connection has TLS: the servers it reports are learned over TLS.
        let mut tls = self.options.tls_first && !server_addr.is_websocket();
        let mut connection = match server_addr.scheme() {
            // The websocket handshake runs on the client's own stream, so every resolved
            // address is tried, and TLS sits below the websocket framing: a wss:// URL gets
            // the same handshake as a tls:// URL, and so does a ws:// server learned over
            // TLS or while the client asks for TLS. A ws:// URL the user gave is refused
            // at connect, or by set_server_pool(), when the client asks for TLS.
            #[cfg(feature = "websockets")]
            "ws" | "wss" => {
                tls = server_addr.scheme() == "wss" || tls_required || self.tls_asked_by.is_some();
                let stream: Box<dyn AsyncReadWrite> = if tls {
                    Box::new(
                        self.tls_handshake(tcp_stream, &server_addr, tls_name)
                            .await?,
                    )
                } else {
                    Box::new(tcp_stream)
                };
                let ws = tokio_websockets::client::Builder::new()
                    .uri(server_addr.as_url_str())
                    .map_err(|err| {
                        ConnectError::with_source(crate::ConnectErrorKind::ServerParse, err)
                    })?
                    .connect_on(stream)
                    .await
                    .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Io, err))?;

                let con = WebSocketAdapter::new(ws.0);
                Connection::new(Box::new(con), 0, self.connect_stats.clone())
            }
            _ => Connection::new(
                Box::new(tcp_stream),
                self.options.read_buffer_capacity.into(),
                self.connect_stats.clone(),
            ),
        };

        let tls_connection = |connection: Connection| async {
            let tls_stream = self
                .tls_handshake(connection.stream, &server_addr, tls_name)
                .await?;

            Ok::<Connection, ConnectError>(Connection::new(
                Box::new(tls_stream),
                0,
                self.connect_stats.clone(),
            ))
        };

        // If `tls_first` was set, establish TLS connection before getting INFO.
        // There is no point in  checking if tls is required, because
        // the connection has to be be upgraded to TLS anyway as it's different flow.
        if self.options.tls_first && !server_addr.is_websocket() {
            connection = tls_connection(connection).await?;
        }

        let op = connection.read_op().await?;
        let info = match op {
            Some(ServerOp::Info(info)) => {
                tracing::debug!(
                    server_id = %info.server_id,
                    version = %info.version,
                    "received server info"
                );
                info
            }
            Some(op) => {
                tracing::error!(received_op = ?op, "expected INFO, got different operation");
                return Err(ConnectError::with_source(
                    crate::ConnectErrorKind::Io,
                    format!("expected INFO, got {op:?}"),
                ));
            }
            None => {
                tracing::error!("expected INFO, got nothing");
                return Err(ConnectError::with_source(
                    crate::ConnectErrorKind::Io,
                    "expected INFO, got nothing",
                ));
            }
        };

        // If `tls_first` was not set, establish TLS connection if it is required.
        if !self.options.tls_first
            && !server_addr.is_websocket()
            && (self.tls_asked_by.is_some() || info.tls_required || tls_required)
        {
            // A server whose INFO neither requires nor offers TLS cannot answer the
            // handshake. Instead of starting one, say which server, what asked for TLS and,
            // where the server's own URL did not, how to leave TLS to it.
            if !info.tls_required && !info.tls_available {
                let server = format!("{:?}", without_user_info(&server_addr));
                let message = match &self.tls_asked_by {
                    Some(asked_by) if !tls_required => format!(
                        "{asked_by} asks for TLS, but {server} does not offer it; \
                         tls_optional() leaves TLS to each server"
                    ),
                    Some(asked_by) => {
                        format!("{asked_by} asks for TLS, but {server} does not offer it")
                    }
                    None if server_addr.tls_required() => {
                        format!("the URL {server} asks for TLS, but {server} does not offer it")
                    }
                    None => format!(
                        "{server} was discovered on a connection with TLS, but does not offer it"
                    ),
                };
                return Err(ConnectError::with_source(
                    crate::ConnectErrorKind::Tls,
                    message,
                ));
            }
            connection = tls_connection(connection).await?;
            tls = true;
        } else if !self.options.ignore_discovered_servers {
            // Discover servers from INFO. The servers of an INFO read before a TLS handshake
            // are not used, whether the attempt goes on or fails: they come from the INFOs
            // received inside TLS.
            self.add_discovered_servers(&info.connect_urls, &server_addr, tls);
        }

        // Build CONNECT message with auth info.
        let tls_required = self.tls_asked_by.is_some() || tls_required;
        let mut connect_info = ConnectInfo {
            tls_required,
            name: self.options.name.clone(),
            pedantic: false,
            verbose: false,
            lang: LANG.to_string(),
            version: VERSION.to_string(),
            protocol: Protocol::Dynamic,
            user: self.options.auth.username.to_owned(),
            pass: self.options.auth.password.to_owned(),
            auth_token: self.options.auth.token.to_owned(),
            user_jwt: None,
            nkey: None,
            signature: None,
            echo: !self.options.no_echo,
            headers: true,
            no_responders: true,
        };

        #[cfg(feature = "nkeys")]
        if let Some(nkey) = self.options.auth.nkey.as_ref() {
            match nkeys::KeyPair::from_seed(nkey.as_str()) {
                Ok(key_pair) => {
                    let nonce = info.nonce.clone();
                    match key_pair.sign(nonce.as_bytes()) {
                        Ok(signed) => {
                            connect_info.nkey = Some(key_pair.public_key());
                            connect_info.signature = Some(URL_SAFE_NO_PAD.encode(signed));
                        }
                        Err(_) => {
                            tracing::error!("failed to sign nonce with nkey");
                            return Err(ConnectError::new(crate::ConnectErrorKind::Authentication));
                        }
                    };
                }
                Err(_) => {
                    tracing::error!("failed to create key pair from nkey seed");
                    return Err(ConnectError::new(crate::ConnectErrorKind::Authentication));
                }
            }
        }

        #[cfg(feature = "nkeys")]
        if let Some(jwt) = self.options.auth.jwt.as_ref() {
            if let Some(sign_fn) = self.options.auth.signature_callback.as_ref() {
                match sign_fn.call(info.nonce.clone()).await {
                    Ok(sig) => {
                        connect_info.user_jwt = Some(jwt.clone());
                        connect_info.signature = Some(sig);
                    }
                    Err(_) => {
                        tracing::error!("failed to sign nonce with JWT callback");
                        return Err(ConnectError::new(crate::ConnectErrorKind::Authentication));
                    }
                }
            }
        }

        if let Some(callback) = self.options.auth_callback.as_ref() {
            let auth: crate::Auth = callback
                .call(info.nonce.as_bytes().to_vec())
                .await
                .map_err(|err| {
                    tracing::error!(error = %err, "auth callback failed");
                    ConnectError::with_source(crate::ConnectErrorKind::Authentication, err)
                })?;
            connect_info.user = auth.username;
            connect_info.pass = auth.password;
            connect_info.user_jwt = auth.jwt;
            #[cfg(feature = "nkeys")]
            {
                connect_info.signature = auth
                    .signature
                    .map(|signature| URL_SAFE_NO_PAD.encode(signature));
            }
            #[cfg(not(feature = "nkeys"))]
            {
                if auth.signature.is_some() {
                    tracing::error!("signature authentication requires 'nkeys' feature");
                    return Err(ConnectError::new(crate::ConnectErrorKind::Authentication));
                }
                connect_info.signature = None;
            }
            connect_info.auth_token = auth.token;
            connect_info.nkey = auth.nkey;
        }

        // Send CONNECT + PING, then wait for PONG. Other operations before it are skipped.
        connection
            .easy_write_and_flush([ClientOp::Connect(connect_info), ClientOp::Ping].iter())
            .await?;
        while !matches!(next_op(&mut connection).await?, ServerOp::Pong) {}

        Ok(Handshake {
            info: *info,
            connection,
            tls,
        })
    }

    /// On a connection that started TLS after its first INFO, `info`, from a server whose
    /// `version` there is 2.12.0 or later, waits for the INFO that nats-server sends after
    /// the first PONG, and replaces the discovered servers with the servers it lists. The
    /// wait has a timer of its own, up to the connection timeout; when the timer runs out,
    /// the client reports connected without that INFO. Other operations are skipped.
    /// Returns the INFO of the connection: the one received inside TLS when it came, which
    /// replaces `info`.
    ///
    /// The `version` of `info`, read before the TLS handshake, decides only whether the
    /// client waits, so a wrong one costs at most one wait.
    ///
    /// When that INFO gives lame duck mode, the client raises the lame duck event.
    async fn info_after_first_pong(
        &mut self,
        server_addr: &ServerAddr,
        info: ServerInfo,
        connection: &mut Connection,
        tls: bool,
    ) -> Result<ServerInfo, ConnectError> {
        let tls_after_info = tls && !self.options.tls_first && !server_addr.is_websocket();
        if !tls_after_info
            || self.options.ignore_discovered_servers
            || !sends_info_after_first_pong(&info.version)
        {
            return Ok(info);
        }
        let next_info = async {
            loop {
                if let ServerOp::Info(info) = next_op(connection).await? {
                    return Ok::<_, ConnectError>(info);
                }
            }
        };
        match tokio::time::timeout(self.options.connection_timeout, next_info).await {
            Ok(next_info) => {
                let next_info = next_info?;
                if next_info.lame_duck_mode {
                    self.events_tx.try_send(Event::LameDuckMode).ok();
                }
                self.replace_discovered_servers(&next_info.connect_urls, server_addr, true);
                Ok(*next_info)
            }
            Err(_) => {
                tracing::debug!(
                    server = ?server_addr,
                    "no INFO after the first PONG, connected without it"
                );
                Ok(info)
            }
        }
    }

    /// Takes an INFO that arrived on the established connection: its server info and
    /// `max_payload` replace those the client has, which on a connection that started TLS
    /// after INFO can come from the INFO read before the handshake, and its servers replace
    /// the discovered servers.
    pub(crate) fn info_received(&mut self, info: &ServerInfo) {
        self.max_payload.store(info.max_payload, Ordering::Relaxed);
        self.last_info = info.clone();
        if self.options.ignore_discovered_servers {
            return;
        }
        if let Some(current) = self.current.clone() {
            let tls = matches!(current.security, Security::Tls { .. });
            self.replace_discovered_servers(&info.connect_urls, &current.addr, tls);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> ConnectorOptions {
        ConnectorOptions {
            tls_required: false,
            certificates: Vec::new(),
            client_cert: None,
            client_key: None,
            tls_client_config: None,
            tls_first: false,
            tls_optional: false,
            tls_server_name: None,
            verify_discovered_servers_by_ip: false,
            auth: Auth::default(),
            no_echo: false,
            connection_timeout: Duration::from_secs(5),
            name: None,
            ignore_discovered_servers: false,
            retain_servers_order: false,
            read_buffer_capacity: 65535,
            reconnect_delay_callback: Arc::new(reconnect_delay_callback_default),
            auth_callback: None,
            max_reconnects: None,
            local_address: None,
            reconnect_to_server_callback: None,
        }
    }

    fn servers(urls: &[&str]) -> Vec<Server> {
        urls.iter()
            .map(|url| Server::new(url.parse().unwrap()))
            .collect()
    }

    #[test]
    fn tls_is_asked_for_by_every_tls_setting() {
        let plain = servers(&["nats://localhost:4222", "ws://localhost:8080"]);
        assert_eq!(tls_asked_by(&options(), &plain), None);

        let tls_client_config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        for (expected, options) in [
            (
                "require_tls(true)",
                ConnectorOptions {
                    tls_required: true,
                    ..options()
                },
            ),
            (
                "tls_first()",
                ConnectorOptions {
                    tls_required: true,
                    tls_first: true,
                    ..options()
                },
            ),
            (
                "add_root_certificates()",
                ConnectorOptions {
                    certificates: vec![PathBuf::from("ca.pem")],
                    ..options()
                },
            ),
            (
                "add_client_certificate()",
                ConnectorOptions {
                    client_cert: Some(PathBuf::from("cert.pem")),
                    client_key: Some(PathBuf::from("key.pem")),
                    ..options()
                },
            ),
            (
                "tls_client_config()",
                ConnectorOptions {
                    tls_client_config: Some(tls_client_config),
                    ..options()
                },
            ),
            (
                "tls_server_name()",
                ConnectorOptions {
                    tls_server_name: Some("nats.example.com".into()),
                    ..options()
                },
            ),
            (
                "verify_discovered_servers_by_ip()",
                ConnectorOptions {
                    verify_discovered_servers_by_ip: true,
                    ..options()
                },
            ),
        ] {
            assert_eq!(tls_asked_by(&options, &plain).as_deref(), Some(expected));
        }

        // One URL asks for TLS for the whole list, whatever its case, and is named without
        // its user info.
        for (urls, expected) in [
            (
                ["nats://localhost:4222", "tls://localhost:4223"],
                r#"the URL "tls://localhost:4223""#,
            ),
            (
                ["ws://localhost:8080", "wss://derek:porkchop@localhost:8443"],
                r#"the URL "wss://localhost:8443/""#,
            ),
            (
                ["nats://localhost:4222", "TLS://localhost:4223"],
                r#"the URL "tls://localhost:4223""#,
            ),
        ] {
            assert_eq!(
                tls_asked_by(&options(), &servers(&urls)).as_deref(),
                Some(expected),
                "{urls:?}"
            );
        }
    }

    #[test]
    fn tls_optional_asks_for_no_tls() {
        let options = ConnectorOptions {
            tls_optional: true,
            certificates: vec![PathBuf::from("ca.pem")],
            client_cert: Some(PathBuf::from("cert.pem")),
            client_key: Some(PathBuf::from("key.pem")),
            tls_server_name: Some("nats.example.com".into()),
            verify_discovered_servers_by_ip: true,
            ..options()
        };
        let mixed = servers(&["nats://localhost:4222", "tls://localhost:4223"]);
        assert_eq!(tls_asked_by(&options, &mixed), None);
    }

    #[test]
    fn connect_urls_entries_are_host_port() {
        for (entry, url, host) in [
            ("127.0.0.1:4222", "nats://127.0.0.1:4222", "127.0.0.1"),
            ("[::1]:4222", "nats://[::1]:4222", "::1"),
            (
                "nats.example.com:4222",
                "nats://nats.example.com:4222",
                "nats.example.com",
            ),
            // User info is dropped; the host it was written in front of is used.
            (
                "derek:porkchop@127.0.0.1:4222",
                "nats://127.0.0.1:4222",
                "127.0.0.1",
            ),
            ("token@[::1]:4222", "nats://[::1]:4222", "::1"),
            (
                "@nats.example.com:4222",
                "nats://nats.example.com:4222",
                "nats.example.com",
            ),
        ] {
            let addr = discovered_addr(entry, false).unwrap();
            assert_eq!(addr.as_url_str(), url, "{entry}");
            assert_eq!(addr.host(), host, "{entry}");
            assert_eq!(addr.username(), None, "{entry}");
            assert_eq!(addr.password(), None, "{entry}");
        }

        for entry in [
            "nats://127.0.0.1:4222",
            "tls://127.0.0.1:4222",
            "ws://127.0.0.1:4222",
            "WSS://127.0.0.1:4222",
            "nats://derek:porkchop@127.0.0.1:4222",
        ] {
            assert_eq!(
                discovered_addr(entry, false).unwrap_err(),
                "it has a scheme",
                "{entry}"
            );
        }

        for entry in [
            "",
            "127.0.0.1",
            "127.0.0.1:",
            ":4222",
            "127.0.0.1:4222/",
            "127.0.0.1:4222/path",
            "127.0.0.1:4222?x=1",
            "127.0.0.1:4222#x",
            "127.0.0.1:99999",
            "::::",
            "[::1",
            "[::1]",
            "a b:4222",
        ] {
            assert_eq!(
                discovered_addr(entry, false).unwrap_err(),
                "it is not host:port",
                "{entry:?}"
            );
        }
    }

    fn connector(urls: &[&str], options: ConnectorOptions) -> Connector {
        let (events_tx, _) = tokio::sync::mpsc::channel(1);
        let (state_tx, _) = tokio::sync::watch::channel(State::Pending);
        Connector::new(
            urls,
            options,
            events_tx,
            state_tx,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Statistics::default()),
        )
        .unwrap()
    }

    fn addr(url: &str) -> ServerAddr {
        url.parse().unwrap()
    }

    fn pool(connector: &Connector) -> Vec<&str> {
        connector
            .servers
            .iter()
            .map(|server| server.addr.as_url_str())
            .collect()
    }

    /// The URLs `server_pool()` lists.
    fn listed(connector: &Connector) -> Vec<String> {
        connector
            .server_pool()
            .iter()
            .map(|server| server.addr.as_url_str().to_owned())
            .collect()
    }

    #[test]
    fn discovered_servers_keep_the_transport_and_tls_they_were_learned_on() {
        // The WebSocket reporter is not configured: a build without the websockets feature
        // refuses a ws:// URL at connect.
        let mut connector = connector(&["nats://seed.example:4222"], options());
        let (tcp, ws) = (
            addr("nats://seed.example:4222"),
            addr("ws://seed.example:8080"),
        );

        connector.add_discovered_servers(&["10.0.0.2:4222".into()], &tcp, false);
        connector.add_discovered_servers(&["10.0.0.3:4222".into()], &tcp, true);
        connector.add_discovered_servers(&["10.0.0.4:8080".into()], &ws, false);
        connector.add_discovered_servers(&["10.0.0.5:8080".into()], &ws, true);
        connector.add_discovered_servers(&["10.0.0.6:80".into()], &ws, true);

        assert_eq!(
            pool(&connector)[1..],
            [
                "nats://10.0.0.2:4222",
                "nats://10.0.0.3:4222",
                "ws://10.0.0.4:8080/",
                "ws://10.0.0.5:8080/",
                "ws://10.0.0.6/",
            ]
        );
        for (url, tls) in [
            ("nats://10.0.0.2:4222", false),
            ("nats://10.0.0.3:4222", true),
            ("ws://10.0.0.4:8080", false),
            ("ws://10.0.0.5:8080", true),
            ("ws://10.0.0.6:80", true),
        ] {
            assert_eq!(connector.learned_over_tls(&addr(url)), tls, "{url}");
        }
        // server_pool() lists a server learned over TLS with the TLS form of its scheme, on
        // the same port: ws:// on port 80 becomes wss:// on port 80.
        assert_eq!(
            listed(&connector)[1..],
            [
                "nats://10.0.0.2:4222",
                "tls://10.0.0.3:4222",
                "ws://10.0.0.4:8080/",
                "wss://10.0.0.5:8080/",
                "wss://10.0.0.6:80/",
            ]
        );
    }

    #[test]
    fn a_discovered_server_gains_tls_and_never_loses_it() {
        let mut connector = connector(&["nats://seed.example:4222"], options());
        let seed = addr("nats://seed.example:4222");
        let discovered = addr("nats://10.0.0.2:4222");

        connector.add_discovered_servers(&["10.0.0.2:4222".into()], &seed, false);
        assert!(!connector.learned_over_tls(&discovered));
        connector.add_discovered_servers(&["10.0.0.2:4222".into()], &seed, true);
        assert!(connector.learned_over_tls(&discovered));
        connector.add_discovered_servers(&["10.0.0.2:4222".into()], &seed, false);
        assert!(connector.learned_over_tls(&discovered));
        assert_eq!(
            pool(&connector),
            ["nats://seed.example:4222", "nats://10.0.0.2:4222"]
        );
        // Once it has gained TLS, it is listed with it.
        assert_eq!(
            listed(&connector),
            ["nats://seed.example:4222", "tls://10.0.0.2:4222"]
        );
    }

    #[test]
    fn a_server_given_in_a_pool_is_verified_as_configured() {
        // Under tls_optional, a server learned over TLS and then given in a pool as nats://
        // is dialed as its URL says, also after a later report over TLS.
        let mut optional = connector(
            &["tls://nats.example.com:4222"],
            ConnectorOptions {
                tls_optional: true,
                ..options()
            },
        );
        let seed = addr("tls://nats.example.com:4222");
        optional.add_discovered_servers(&["10.0.0.2:4222".into()], &seed, true);
        optional
            .set_server_pool(vec![addr("nats://10.0.0.2:4222")])
            .unwrap();
        assert!(!optional.learned_over_tls(&addr("nats://10.0.0.2:4222")));
        optional.add_discovered_servers(
            &["10.0.0.2:4222".into()],
            &addr("nats://10.0.0.2:4222"),
            true,
        );
        assert!(!optional.learned_over_tls(&addr("nats://10.0.0.2:4222")));

        // A discovered IP address given in a pool is verified against itself.
        for seed in ["tls://10.0.0.1:4222", "tls://nats.example.com:4222"] {
            let mut pooled = connector(&[seed], options());
            pooled.add_discovered_servers(&["10.0.0.2:4222".into()], &addr(seed), true);
            pooled
                .set_server_pool(vec![addr("tls://10.0.0.2:4222")])
                .unwrap();
            assert_eq!(
                tls_name(&pooled, "tls://10.0.0.2:4222").as_deref(),
                Some("10.0.0.2")
            );
            assert!(!pooled.skip_without_tls_name(&addr("tls://10.0.0.2:4222")));
        }
    }

    #[test]
    fn a_pool_made_from_the_listed_pool_keeps_the_state_of_each_server() {
        let seed = addr("nats://seed.example.com:4222");
        let learned = || {
            let mut learned = connector(&["nats://seed.example.com:4222"], options());
            learned.add_discovered_servers(&["10.0.0.2:4222".into()], &seed, true);
            for server in &mut learned.servers {
                server.failed_attempts = 2;
                server.did_connect = true;
                server.last_error = Some("timed out".into());
            }
            learned
        };
        let state = |server: &Server| {
            (
                server.failed_attempts,
                server.did_connect,
                server.last_error.clone(),
            )
        };
        let kept = (2, true, Some("timed out".to_string()));

        // Given back as listed, every server keeps its state.
        let mut round_trip = learned();
        let listed = round_trip
            .server_pool()
            .into_iter()
            .map(|server| server.addr)
            .collect();
        round_trip.set_server_pool(listed).unwrap();
        assert_eq!(
            pool(&round_trip),
            ["nats://seed.example.com:4222", "tls://10.0.0.2:4222"]
        );
        assert_eq!(
            round_trip.servers.iter().map(state).collect::<Vec<_>>(),
            [kept.clone(), kept.clone()]
        );

        // The address the pool kept for it is another URL than the listed one: given in a
        // pool, it starts afresh.
        let mut by_stored_url = learned();
        by_stored_url
            .set_server_pool(vec![seed.clone(), addr("nats://10.0.0.2:4222")])
            .unwrap();
        assert_eq!(
            by_stored_url.servers.iter().map(state).collect::<Vec<_>>(),
            [kept, (0, false, None)]
        );
    }

    #[test]
    fn a_server_given_in_a_pool_keeps_the_tls_it_was_learned_with() {
        let seed = addr("nats://seed.example.com:4222");
        let learned = || {
            let mut learned = connector(&["nats://seed.example.com:4222"], options());
            learned.add_discovered_servers(&["10.0.0.2:4222".into()], &seed, true);
            learned.add_discovered_servers(&["10.0.0.3:4222".into()], &seed, false);
            learned
        };
        let (with_tls, without_tls) = (addr("nats://10.0.0.2:4222"), addr("nats://10.0.0.3:4222"));

        // Without tls_optional, the pool that server_pool() lists keeps TLS for the server
        // learned over TLS, and its tls:// URL asks for TLS for every server.
        let mut round_trip = learned();
        let listed = round_trip
            .server_pool()
            .into_iter()
            .map(|server| server.addr)
            .collect();
        round_trip.set_server_pool(listed).unwrap();
        assert!(round_trip.tls_asked_by.is_some());
        assert!(round_trip.learned_over_tls(&with_tls));
        assert!(!round_trip.learned_over_tls(&without_tls));

        // Given as nats:// to a client that does not ask for TLS, it keeps TLS as long as every
        // pool given since lists it.
        let mut connector = learned();
        connector
            .set_server_pool(vec![seed.clone(), with_tls.clone()])
            .unwrap();
        assert!(connector.tls_asked_by.is_none());
        assert!(connector.learned_over_tls(&with_tls));
        connector
            .set_server_pool(vec![seed.clone(), with_tls.clone()])
            .unwrap();
        assert!(connector.learned_over_tls(&with_tls));
        connector.set_server_pool(vec![seed.clone()]).unwrap();
        assert!(!connector.learned_over_tls(&with_tls));
        // A pool that left the server out dropped what the client learned about it.
        connector
            .set_server_pool(vec![seed, with_tls.clone()])
            .unwrap();
        assert!(!connector.learned_over_tls(&with_tls));
    }

    #[test]
    fn quotes_the_pool_url_that_asks_for_tls_cut_at_256_bytes() {
        // The host of a server learned over TLS, which the pool lists as tls://, is one the
        // server chose.
        let mut connector = connector(&["nats://seed.example.com:4222"], options());
        let host = "h".repeat(300);
        connector.add_discovered_servers(
            &[format!("{host}:4222")],
            &addr("nats://seed.example.com:4222"),
            true,
        );

        let listed = connector
            .server_pool()
            .into_iter()
            .map(|server| server.addr)
            .collect();
        connector.set_server_pool(listed).unwrap();

        let url = quoted(&format!("tls://{host}:4222"));
        assert_eq!(
            connector.tls_asked_by.as_deref(),
            Some(format!("the URL {url} given to set_server_pool()").as_str())
        );
    }

    #[cfg(feature = "websockets")]
    #[test]
    fn a_ws_url_given_in_a_pool_is_dialed_as_written() {
        // Also when a TCP server with the same host and port was learned over TLS.
        let mut connector = connector(&["nats://seed.example.com:4222"], options());
        let seed = addr("nats://seed.example.com:4222");
        connector.add_discovered_servers(&["10.0.0.2:4222".into()], &seed, true);
        let ws = addr("ws://10.0.0.2:4222");
        connector.set_server_pool(vec![ws.clone()]).unwrap();
        assert!(!connector.learned_over_tls(&ws));
    }

    #[test]
    fn an_entry_for_a_known_server_adds_nothing() {
        // A configured URL without a port counts with the port it is dialed on.
        let mut connector = connector(
            &["nats://seed.example", "tls://other.example:4223"],
            options(),
        );
        let seed = addr("nats://seed.example");

        connector.add_discovered_servers(
            &[
                "seed.example:4222".into(),
                "other.example:4223".into(),
                "10.0.0.2:4222".into(),
                "derek@10.0.0.2:4222".into(),
            ],
            &seed,
            true,
        );

        assert_eq!(
            pool(&connector),
            [
                "nats://seed.example",
                "tls://other.example:4223",
                "nats://10.0.0.2:4222"
            ]
        );
        // A configured server keeps how the user configured it.
        assert!(!connector.learned_over_tls(&seed));
    }

    #[test]
    fn a_server_the_pool_left_out_lends_no_name_to_what_it_lists() {
        // A connection without TLS to the configured server lists a host name, and the
        // client then connects to that host without TLS.
        let mut connector = connector(&["nats://seed.example:4222"], options());
        let seed = addr("nats://seed.example:4222");
        connector.add_discovered_servers(&["other.example.com:4222".into()], &seed, false);
        connector.current = Some(CurrentServer {
            addr: addr("nats://other.example.com:4222"),
            security: Security::Plaintext,
        });
        // A pool that asks for TLS and does not list that host.
        connector
            .set_server_pool(vec![addr("tls://seed.example:4222")])
            .unwrap();
        assert!(connector.tls_asked_by.is_some());

        // A later INFO on that connection lists an IP address.
        connector.info_received(&ServerInfo {
            connect_urls: vec!["203.0.113.5:4222".into()],
            ..Default::default()
        });

        let ip = addr("nats://203.0.113.5:4222");
        assert_eq!(connector.tls_name(&ip), None);
        assert!(connector.skip_without_tls_name(&ip));
    }

    #[test]
    fn a_server_that_drops_out_of_the_list_is_learned_again() {
        let mut connector = connector(&["nats://seed.example:4222"], options());
        let seed = addr("nats://seed.example:4222");

        connector.replace_discovered_servers(
            &["10.0.0.2:4222".into(), "10.0.0.3:4222".into()],
            &seed,
            true,
        );
        connector.replace_discovered_servers(&["10.0.0.3:4222".into()], &seed, false);
        assert_eq!(
            pool(&connector),
            ["nats://seed.example:4222", "nats://10.0.0.3:4222"]
        );

        // Back in a list learned without TLS, it is a new entry without TLS; the server that
        // stayed keeps what the client learned about it.
        connector.replace_discovered_servers(
            &["10.0.0.3:4222".into(), "10.0.0.2:4222".into()],
            &seed,
            false,
        );
        assert_eq!(
            pool(&connector),
            [
                "nats://seed.example:4222",
                "nats://10.0.0.3:4222",
                "nats://10.0.0.2:4222"
            ]
        );
        assert!(!connector.learned_over_tls(&addr("nats://10.0.0.2:4222")));
        assert!(connector.learned_over_tls(&addr("nats://10.0.0.3:4222")));
    }

    fn tls_name(connector: &Connector, url: &str) -> Option<String> {
        connector.tls_name(&addr(url)).map(str::to_owned)
    }

    #[test]
    fn a_server_learned_over_tls_is_verified_against_the_name_it_was_learned_under() {
        let mut connector = connector(&["tls://nats.example.com:4222"], options());
        let seed = addr("tls://nats.example.com:4222");

        connector.add_discovered_servers(
            &["10.0.0.2:4222".into(), "node3.example.com:4222".into()],
            &seed,
            true,
        );
        // Over any number of hops: from an IP address verified under the seed's name, and
        // from a host name verified under its own.
        connector.add_discovered_servers(
            &["10.0.0.4:4222".into()],
            &addr("nats://10.0.0.2:4222"),
            true,
        );
        connector.add_discovered_servers(
            &["10.0.0.5:4222".into()],
            &addr("nats://node3.example.com:4222"),
            true,
        );

        for (url, name) in [
            ("tls://nats.example.com:4222", "nats.example.com"),
            ("nats://10.0.0.2:4222", "nats.example.com"),
            ("nats://node3.example.com:4222", "node3.example.com"),
            ("nats://10.0.0.4:4222", "nats.example.com"),
            ("nats://10.0.0.5:4222", "node3.example.com"),
        ] {
            assert_eq!(tls_name(&connector, url).as_deref(), Some(name), "{url}");
        }
    }

    #[test]
    fn a_server_learned_without_tls_follows_the_configured_name() {
        let mut connector = connector(
            &[
                "nats://nats.example.com:4222",
                "tls://nats2.example.com:4222",
            ],
            ConnectorOptions {
                tls_optional: true,
                ..options()
            },
        );
        let seed = addr("nats://nats.example.com:4222");

        connector.add_discovered_servers(
            &["10.0.0.2:4222".into(), "node3.example.com:4222".into()],
            &seed,
            false,
        );
        connector.add_discovered_servers(
            &["10.0.0.3:4222".into()],
            &addr("nats://node3.example.com:4222"),
            false,
        );
        // A discovered IP address is one more address of the configured server's name,
        // over any number of hops. A host name learned without TLS has no name.
        assert_eq!(
            tls_name(&connector, "nats://10.0.0.2:4222").as_deref(),
            Some("nats.example.com")
        );
        assert_eq!(
            tls_name(&connector, "nats://10.0.0.3:4222").as_deref(),
            Some("nats.example.com")
        );
        assert_eq!(tls_name(&connector, "nats://node3.example.com:4222"), None);

        // Learned again over TLS, they gain TLS and take the name of that report.
        connector.add_discovered_servers(
            &["10.0.0.2:4222".into(), "node3.example.com:4222".into()],
            &addr("tls://nats2.example.com:4222"),
            true,
        );
        assert_eq!(
            tls_name(&connector, "nats://10.0.0.2:4222").as_deref(),
            Some("nats2.example.com")
        );
        assert_eq!(
            tls_name(&connector, "nats://node3.example.com:4222").as_deref(),
            Some("node3.example.com")
        );
        // A later report does not change the name of a server that already has TLS.
        connector.add_discovered_servers(
            &["10.0.0.2:4222".into()],
            &addr("tls://nats.example.com:4222"),
            true,
        );
        assert_eq!(
            tls_name(&connector, "nats://10.0.0.2:4222").as_deref(),
            Some("nats2.example.com")
        );
    }

    #[test]
    fn a_discovered_server_without_a_name_is_skipped() {
        let mut connector = connector(&["tls://10.0.0.1:4222"], options());
        let seed = addr("tls://10.0.0.1:4222");
        connector.add_discovered_servers(
            &["10.0.0.2:4222".into(), "node3.example.com:4222".into()],
            &seed,
            true,
        );

        // A configured IP address is verified against itself; the IP addresses it reports
        // have no host name to be verified against.
        assert_eq!(
            tls_name(&connector, "tls://10.0.0.1:4222").as_deref(),
            Some("10.0.0.1")
        );
        assert_eq!(tls_name(&connector, "nats://10.0.0.2:4222"), None);
        assert!(!connector.skip_without_tls_name(&seed));
        assert!(connector.skip_without_tls_name(&addr("nats://10.0.0.2:4222")));
        assert!(!connector.skip_without_tls_name(&addr("nats://node3.example.com:4222")));
        assert!(connector.discovered[&("10.0.0.2".to_string(), 4222)].no_name_logged);
    }

    /// A connector configured with `seed` that learned, over TLS from `seed`, an IP address
    /// and a host name, and without TLS from `seed` another host name.
    fn connector_with_discovered_servers(seed: &str, options: ConnectorOptions) -> Connector {
        let mut connector = connector(&[seed], options);
        let seed = addr(seed);
        connector.add_discovered_servers(
            &["10.0.0.2:4222".into(), "node3.example.com:4222".into()],
            &seed,
            true,
        );
        connector.add_discovered_servers(&["node4.example.com:4222".into()], &seed, false);
        connector
    }

    #[test]
    fn tls_server_name_overrides_every_name() {
        for seed in ["tls://nats.example.com:4222", "tls://10.0.0.1:4222"] {
            let connector = connector_with_discovered_servers(
                seed,
                ConnectorOptions {
                    tls_server_name: Some("cluster.example.com".into()),
                    verify_discovered_servers_by_ip: true,
                    ..options()
                },
            );
            for url in [
                seed,
                "nats://10.0.0.2:4222",
                "nats://node3.example.com:4222",
                "nats://node4.example.com:4222",
            ] {
                assert_eq!(
                    tls_name(&connector, url).as_deref(),
                    Some("cluster.example.com"),
                    "{url}"
                );
            }
        }
    }

    #[test]
    fn verify_discovered_servers_by_ip_verifies_a_discovered_ip_against_itself() {
        for seed in ["tls://nats.example.com:4222", "tls://10.0.0.1:4222"] {
            let mut connector = connector_with_discovered_servers(
                seed,
                ConnectorOptions {
                    verify_discovered_servers_by_ip: true,
                    ..options()
                },
            );
            let seed_host = addr(seed).host().to_owned();
            // Only discovered IP addresses: a configured server stays as configured, and a
            // host name keeps its own rule.
            for (url, name) in [
                (seed, Some(seed_host.as_str())),
                ("nats://10.0.0.2:4222", Some("10.0.0.2")),
                ("nats://node3.example.com:4222", Some("node3.example.com")),
                ("nats://node4.example.com:4222", None),
            ] {
                assert_eq!(tls_name(&connector, url).as_deref(), name, "{url}");
            }
            assert!(!connector.skip_without_tls_name(&addr("nats://10.0.0.2:4222")));
        }
    }

    #[test]
    fn the_error_without_a_name_names_the_settings_that_give_one() {
        assert_eq!(
            no_tls_name(&addr("nats://10.0.0.2:4222")),
            "a discovered server with no name to verify its certificate against; \
             tls_server_name() sets the name to verify every server against, or \
             verify_discovered_servers_by_ip() verifies a discovered IP address against \
             itself, which is safe only where the CA that issued the certificates is not \
             shared with others"
        );
        assert_eq!(
            no_tls_name(&addr("nats://node4.example.com:4222")),
            "a discovered server with no name to verify its certificate against; \
             tls_server_name() sets the name to verify every server against"
        );
    }

    /// Log lines written while it runs.
    fn logs_of(run: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Logs {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let logs = Logs::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, run);
        let text = String::from_utf8_lossy(&logs.0.lock().unwrap()).into_owned();
        text
    }

    #[test]
    fn cuts_the_url_in_the_warning_for_a_skipped_server() {
        // A host name learned without TLS while the client asks for TLS, as after a pool
        // asked for TLS on a connection without it. The host is longer than the cut.
        let mut connector = connector(
            &["nats://seed.example:4222"],
            ConnectorOptions {
                tls_required: true,
                ..options()
            },
        );
        let url = format!("nats://{}:4222", "h".repeat(300));
        connector.add_discovered_servers(
            &[format!("{}:4222", "h".repeat(300))],
            &addr("nats://seed.example:4222"),
            false,
        );

        let mut skipped = false;
        let logs = logs_of(|| skipped = connector.skip_without_tls_name(&addr(&url)));

        assert!(skipped);
        assert!(logs.contains(&quoted(&url)), "{logs}");
        assert!(!logs.contains(&url), "{logs}");
    }

    #[test]
    fn a_failed_name_check_names_the_expected_and_presented_names() {
        const NAME_SETTING: &str =
            "tls_server_name() sets the name every server is verified against";
        const CLIENT_ADVERTISE: &str = ", or each server of the cluster can advertise a name \
                                        its certificate is issued for (client_advertise in \
                                        nats-server)";
        const FLAG: &str = ", or verify_discovered_servers_by_ip() verifies a discovered IP \
                            address against itself, which is safe only where the CA that \
                            issued the certificates is not shared with others";
        let presented = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let connector = connector_with_discovered_servers("tls://nats.example.com:4222", options());
        let flag = connector_with_discovered_servers(
            "tls://nats.example.com:4222",
            ConnectorOptions {
                verify_discovered_servers_by_ip: true,
                ..options()
            },
        );
        let named = connector_with_discovered_servers(
            "tls://nats.example.com:4222",
            ConnectorOptions {
                tls_server_name: Some("cluster.example.com".into()),
                ..options()
            },
        );

        for (connector, url, name, names, expected) in [
            // A configured server: the name setting only.
            (
                &connector,
                "tls://derek:porkchop@nats.example.com:4222",
                "nats.example.com",
                Some(presented(&[r#"DnsName("other.example.com")"#])),
                format!(
                    r#"the certificate of "tls://nats.example.com:4222" is not valid for "nats.example.com": it is issued for "other.example.com"; {NAME_SETTING}"#
                ),
            ),
            // A discovered server: also the remedy on the server's side.
            (
                &connector,
                "nats://node3.example.com:4222",
                "node3.example.com",
                Some(presented(&[r#"DnsName("nats.example.com")"#])),
                format!(
                    r#"the certificate of "nats://node3.example.com:4222" is not valid for "node3.example.com": it is issued for "nats.example.com"; {NAME_SETTING}{CLIENT_ADVERTISE}"#
                ),
            ),
            // A discovered IP address whose certificate names IP addresses: also the flag.
            // At most four names are shown.
            (
                &connector,
                "nats://10.0.0.2:4222",
                "nats.example.com",
                Some(presented(&[
                    "IpAddress(10.0.0.2)",
                    "IpAddress(10.0.0.3)",
                    "IpAddress(10.0.0.4)",
                    "IpAddress(10.0.0.5)",
                    "IpAddress(10.0.0.6)",
                    "IpAddress(10.0.0.7)",
                ])),
                format!(
                    r#"the certificate of "nats://10.0.0.2:4222" is not valid for "nats.example.com": it is issued for "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5" and 2 more; {NAME_SETTING}{CLIENT_ADVERTISE}{FLAG}"#
                ),
            ),
            // Not when the certificate does not name the server's own address.
            (
                &connector,
                "nats://10.0.0.2:4222",
                "nats.example.com",
                Some(presented(&["IpAddress(10.0.0.9)"])),
                format!(
                    r#"the certificate of "nats://10.0.0.2:4222" is not valid for "nats.example.com": it is issued for "10.0.0.9"; {NAME_SETTING}{CLIENT_ADVERTISE}"#
                ),
            ),
            // Not when the flag is set already.
            (
                &flag,
                "nats://10.0.0.2:4222",
                "10.0.0.2",
                Some(presented(&["IpAddress(10.0.0.9)"])),
                format!(
                    r#"the certificate of "nats://10.0.0.2:4222" is not valid for "10.0.0.2": it is issued for "10.0.0.9"; {NAME_SETTING}{CLIENT_ADVERTISE}"#
                ),
            ),
            // Nor the remedy on the server's side while tls_server_name() is set.
            (
                &named,
                "nats://10.0.0.2:4222",
                "cluster.example.com",
                Some(presented(&[r#"DnsName("node2.example.com")"#])),
                format!(
                    r#"the certificate of "nats://10.0.0.2:4222" is not valid for "cluster.example.com": it is issued for "node2.example.com"; {NAME_SETTING}"#
                ),
            ),
            // A certificate without names, and a check that does not report them.
            (
                &connector,
                "tls://nats.example.com:4222",
                "nats.example.com",
                Some(Vec::new()),
                format!(
                    r#"the certificate of "tls://nats.example.com:4222" is not valid for "nats.example.com": it is issued for no name; {NAME_SETTING}"#
                ),
            ),
            (
                &connector,
                "tls://nats.example.com:4222",
                "nats.example.com",
                None,
                format!(
                    r#"the certificate of "tls://nats.example.com:4222" is not valid for "nats.example.com"; {NAME_SETTING}"#
                ),
            ),
        ] {
            assert_eq!(
                connector.name_check_failed(&addr(url), name, names.as_deref()),
                expected
            );
        }
    }

    #[test]
    fn skipped_entries_are_quoted_escaped_and_cut() {
        assert_eq!(quoted("a\r\n\u{1b}[31mb\"c"), r#""a\r\n\u{1b}[31mb\"c""#);
        assert_eq!(quoted(&"x".repeat(300)), format!("{:?}", "x".repeat(256)));
        // Cut at a character boundary, never inside a character.
        let entry = format!("{}\u{e9}", "x".repeat(255));
        assert_eq!(quoted(&entry), format!("{:?}", "x".repeat(255)));
    }

    #[test]
    fn waits_after_the_first_pong_only_for_2_12_0_and_later() {
        // The version comes from the INFO read before TLS: a changed one costs at most one
        // wait, or leaves the INFO that follows the first PONG to be taken when it arrives.
        for (version, waits) in [
            ("2.12.0", true),
            ("2.12.1", true),
            ("2.14.0", true),
            ("3.0.0", true),
            ("2.11.99", false),
            ("1.99.0", false),
            ("2", false),
            ("2.12", false),
            ("", false),
            ("2.12.0-beta.1", true),
            ("2.12.1-RC.1", true),
            ("2.16.0-dev", true),
            ("2.12.0+build", true),
            ("2.11.0-beta.1", false),
            ("2.12.0.1", false),
            ("v2.12.0", false),
            ("18446744073709551616.0.0", false),
            ("2.18446744073709551616.0", false),
        ] {
            assert_eq!(sends_info_after_first_pong(version), waits, "{version:?}");
        }
    }

    #[test]
    fn reconnect_delay_callback_duration() {
        let duration = reconnect_delay_callback_default(0);
        assert_eq!(duration.as_millis(), 0);

        let duration = reconnect_delay_callback_default(1);
        assert_eq!(duration.as_millis(), 0);

        let duration = reconnect_delay_callback_default(4);
        assert_eq!(duration.as_millis(), 8);

        let duration = reconnect_delay_callback_default(12);
        assert_eq!(duration.as_millis(), 2048);

        let duration = reconnect_delay_callback_default(13);
        assert_eq!(duration.as_millis(), 4000);

        // The max (4s) was reached and we shouldn't exceed it, regardless of the no of attempts
        let duration = reconnect_delay_callback_default(50);
        assert_eq!(duration.as_millis(), 4000);
    }
}
