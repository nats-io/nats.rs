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
use std::collections::HashMap;
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
    /// TLS names to verify discovered IP servers against, keyed by address.
    /// Holds the DNS name of the server through which the IP was discovered,
    /// so that the certificate check is not downgraded to the bare IP.
    tls_names: HashMap<ServerAddr, String>,
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

/// The address of a `connect_urls` entry, or why the entry is skipped.
///
/// nats-server writes an entry as `host:port`, with an IPv6 address in brackets. User info
/// in an entry is dropped: the server is connected to with the client's own credentials. An
/// entry with a scheme is skipped rather than reduced to `host:port`, and so is anything
/// else that is not `host:port`.
fn discovered_addr(entry: &str) -> Result<ServerAddr, &'static str> {
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
            format!("nats://{host}:{port}")
                .parse()
                .map_err(|_| NOT_HOST_PORT)
        }
        _ => Err(NOT_HOST_PORT),
    }
}

/// A `connect_urls` entry as a log line quotes it: cut at 256 bytes and escaped, since the
/// server chose it.
fn quoted(entry: &str) -> String {
    let mut end = entry.len().min(256);
    while !entry.is_char_boundary(end) {
        end -= 1;
    }
    format!("{:?}", &entry[..end])
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
            tls_names: HashMap::new(),
        })
    }

    /// Replaces the server pool. Preserves per-server state for servers that
    /// appear in both the old and new pools.
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
                .map(|addr| {
                    format!(
                        "the URL {:?} given to set_server_pool()",
                        without_user_info(addr)
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

        let new_servers = addrs
            .into_iter()
            .map(|addr| {
                if let Some(existing) = self.servers.iter().find(|s| s.addr == addr) {
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
        let servers = &self.servers;
        self.tls_names
            .retain(|addr, _| servers.iter().any(|s| &s.addr == addr));
        self.attempts = 0;
        Ok(())
    }

    /// Returns a snapshot of the current server pool.
    pub(crate) fn server_pool(&self) -> Vec<Server> {
        self.servers.to_vec()
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
                    // A TLS error's text names the setting that asked for TLS. Other errors keep
                    // only their kind: their text can quote a line from the server, which is not
                    // escaped.
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
        let mut error = None;

        // If a reconnect-to-server callback is set, try it first.
        if let Some(ref callback) = self.options.reconnect_to_server_callback {
            let pool_snapshot = self.servers.to_vec();
            let info_snapshot = self.last_info.clone();
            let selection = callback.call((pool_snapshot, info_snapshot)).await;

            if let Some(target) = selection {
                // Validate the selected server is in the pool.
                if self.servers.iter().any(|s| s.addr == target.addr) {
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

                    match self.try_connect_to_server(&target.addr).await {
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

        let mut last_err = None;
        for socket_addr in socket_addrs {
            match tokio::time::timeout(
                self.options.connection_timeout,
                self.try_connect_to(
                    &socket_addr,
                    server_addr.tls_required(),
                    server_addr.clone(),
                ),
            )
            .await
            {
                Ok(Ok((server_info, connection))) => {
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

                    // Update per-server state on success.
                    if let Some(entry) = self.servers.iter_mut().find(|s| s.addr == *server_addr) {
                        entry.did_connect = true;
                        entry.failed_attempts = 0;
                        entry.last_error = None;
                    }

                    return Ok((server_info, connection));
                }

                Ok(Err(inner)) => {
                    // Update per-server state on failure.
                    if let Some(entry) = self.servers.iter_mut().find(|s| s.addr == *server_addr) {
                        entry.failed_attempts += 1;
                        entry.last_error = Some(inner.to_string());
                    }
                    last_err = Some(inner);
                }

                Err(_) => {
                    tracing::debug!(
                        server = ?server_addr,
                        "connection handshake timed out"
                    );
                    if let Some(entry) = self.servers.iter_mut().find(|s| s.addr == *server_addr) {
                        entry.failed_attempts += 1;
                        entry.last_error = Some("timed out".to_string());
                    }
                    last_err = Some(ConnectError::new(crate::ConnectErrorKind::TimedOut));
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ConnectError::with_source(crate::ConnectErrorKind::Dns, "no addresses resolved")
        }))
    }

    /// Runs the TLS handshake on `stream` and verifies the server certificate against
    /// `tls_name`.
    async fn tls_handshake<S>(
        &self,
        stream: S,
        tls_name: &str,
    ) -> Result<tokio_rustls::client::TlsStream<S>, ConnectError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        tracing::debug!("upgrading connection to TLS");
        let tls_config = Arc::new(
            tls::config_tls(&self.options)
                .await
                .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?,
        );
        let tls_connector = tokio_rustls::TlsConnector::from(tls_config);

        let domain = crate::rustls::pki_types::ServerName::try_from(tls_name)
            .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?;

        Ok(tls_connector.connect(domain.to_owned(), stream).await?)
    }

    pub(crate) async fn try_connect_to(
        &mut self,
        socket_addr: &SocketAddr,
        tls_required: bool,
        server_addr: ServerAddr,
    ) -> Result<(ServerInfo, Connection), ConnectError> {
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

        let mut connection = match server_addr.scheme() {
            // The websocket handshake runs on the client's own stream, so every resolved
            // address is tried, and TLS sits below the websocket framing as the scheme says:
            // a wss:// URL gets the same handshake as a tls:// URL, a ws:// URL none.
            #[cfg(feature = "websockets")]
            "ws" | "wss" => {
                let stream: Box<dyn AsyncReadWrite> = if server_addr.scheme() == "wss" {
                    Box::new(self.tls_handshake(tcp_stream, server_addr.host()).await?)
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

        // Discovered IP servers are verified against the DNS name of the
        // server they were discovered through rather than the bare IP.
        let tls_name = self
            .tls_names
            .get(&server_addr)
            .cloned()
            .unwrap_or_else(|| server_addr.host().to_string());

        let tls_connection = |connection: Connection| async {
            let tls_stream = self.tls_handshake(connection.stream, &tls_name).await?;

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
                    Some(asked_by) if !server_addr.tls_required() => format!(
                        "{asked_by} asks for TLS, but {server} does not offer it; \
                         tls_optional() leaves TLS to each server"
                    ),
                    Some(asked_by) => {
                        format!("{asked_by} asks for TLS, but {server} does not offer it")
                    }
                    None => {
                        format!("the URL {server} asks for TLS, but {server} does not offer it")
                    }
                };
                return Err(ConnectError::with_source(
                    crate::ConnectErrorKind::Tls,
                    message,
                ));
            }
            connection = tls_connection(connection).await?;
        };

        // Discover servers from INFO.
        if !self.options.ignore_discovered_servers {
            for url in &info.connect_urls {
                let discovered_addr = match discovered_addr(url) {
                    Ok(addr) => addr,
                    Err(reason) => {
                        tracing::warn!("skipping the connect_urls entry {}: {reason}", quoted(url));
                        continue;
                    }
                };
                if !self.servers.iter().any(|s| s.addr == discovered_addr) {
                    tracing::debug!(
                        discovered_url = discovered_addr.as_url_str(),
                        "adding discovered server"
                    );
                    if discovered_addr.host().parse::<IpAddr>().is_ok()
                        && tls_name.parse::<IpAddr>().is_err()
                    {
                        self.tls_names
                            .insert(discovered_addr.clone(), tls_name.clone());
                    }
                    self.servers.push(Server::new_discovered(discovered_addr));
                }
            }
        }

        // Build CONNECT message with auth info.
        let tls_required = self.tls_asked_by.is_some() || server_addr.tls_required();
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

        // Send CONNECT + PING, then wait for PONG.
        connection
            .easy_write_and_flush([ClientOp::Connect(connect_info), ClientOp::Ping].iter())
            .await?;

        match connection.read_op().await? {
            Some(ServerOp::Error(err)) => match err {
                ServerError::AuthorizationViolation => {
                    tracing::error!(error = %err, "authorization violation");
                    Err(ConnectError::with_source(
                        crate::ConnectErrorKind::AuthorizationViolation,
                        err,
                    ))
                }
                err => {
                    tracing::error!(error = %err, "server error during connection");
                    Err(ConnectError::with_source(crate::ConnectErrorKind::Io, err))
                }
            },
            Some(_) => Ok((*info, connection)),
            None => {
                tracing::error!("connection closed unexpectedly");
                Err(ConnectError::with_source(
                    crate::ConnectErrorKind::Io,
                    "broken pipe",
                ))
            }
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
            let addr = discovered_addr(entry).unwrap();
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
                discovered_addr(entry).unwrap_err(),
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
                discovered_addr(entry).unwrap_err(),
                "it is not host:port",
                "{entry:?}"
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
