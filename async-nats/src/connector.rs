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
use crate::SocketAddr;
use crate::ToServerAddrs;
use crate::LANG;
use crate::VERSION;
#[cfg(feature = "nkeys")]
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
#[cfg(feature = "nkeys")]
use base64::engine::Engine;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::cmp;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::sleep;
use tokio_rustls::rustls;

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

#[derive(Debug, Clone)]
pub(crate) struct ServerEntry {
    pub(crate) addr: ServerAddr,
    pub(crate) failed_attempts: usize,
    pub(crate) did_connect: bool,
    pub(crate) is_discovered: bool,
    pub(crate) last_error: Option<String>,
}

pub(crate) type ReconnectToServerCallback =
    CallbackArg1<(Vec<Server>, ServerInfo), Option<ReconnectToServer>>;

impl ServerEntry {
    fn new(addr: ServerAddr) -> Self {
        ServerEntry {
            addr,
            failed_attempts: 0,
            did_connect: false,
            is_discovered: false,
            last_error: None,
        }
    }

    fn new_implicit(addr: ServerAddr) -> Self {
        ServerEntry {
            addr,
            failed_attempts: 0,
            did_connect: false,
            is_discovered: true,
            last_error: None,
        }
    }

    fn to_server(&self) -> Server {
        Server {
            addr: self.addr.clone(),
            failed_attempts: self.failed_attempts,
            did_connect: self.did_connect,
            is_discovered: self.is_discovered,
            last_error: self.last_error.clone(),
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
    servers: Vec<ServerEntry>,
    options: ConnectorOptions,
    pub(crate) connect_stats: Arc<Statistics>,
    attempts: usize,
    pub(crate) events_tx: tokio::sync::mpsc::Sender<Event>,
    pub(crate) state_tx: tokio::sync::watch::Sender<State>,
    pub(crate) max_payload: Arc<AtomicUsize>,
    /// Last known server info, updated after each successful connection.
    last_info: ServerInfo,
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

impl Connector {
    pub(crate) fn new<A: ToServerAddrs>(
        addrs: A,
        options: ConnectorOptions,
        events_tx: tokio::sync::mpsc::Sender<Event>,
        state_tx: tokio::sync::watch::Sender<State>,
        max_payload: Arc<AtomicUsize>,
        connect_stats: Arc<Statistics>,
    ) -> Result<Connector, io::Error> {
        let servers = addrs.to_server_addrs()?.map(ServerEntry::new).collect();

        Ok(Connector {
            attempts: 0,
            servers,
            options,
            events_tx,
            state_tx,
            max_payload,
            connect_stats,
            last_info: ServerInfo::default(),
        })
    }

    /// Replaces the server pool. Preserves per-server state for servers that
    /// appear in both the old and new pools.
    ///
    /// Returns an error if the pool mixes WebSocket (`ws://`, `wss://`) and
    /// non-websocket (`nats://`, `tls://`) schemes.
    pub(crate) fn set_server_pool(&mut self, addrs: Vec<ServerAddr>) -> Result<(), String> {
        if addrs.is_empty() {
            return Err("server pool cannot be empty".to_string());
        }

        // Validate: cannot mix websocket and non-websocket schemes.
        let has_ws = addrs.iter().any(|a| a.is_websocket());
        let has_non_ws = addrs.iter().any(|a| !a.is_websocket());
        if has_ws && has_non_ws {
            return Err("cannot mix websocket and non-websocket URLs in server pool".to_string());
        }

        let new_servers = addrs
            .into_iter()
            .map(|addr| {
                if let Some(existing) = self.servers.iter().find(|s| s.addr == addr) {
                    ServerEntry {
                        addr,
                        failed_attempts: existing.failed_attempts,
                        did_connect: existing.did_connect,
                        is_discovered: false,
                        last_error: existing.last_error.clone(),
                    }
                } else {
                    ServerEntry::new(addr)
                }
            })
            .collect();
        self.servers = new_servers;
        self.attempts = 0;
        Ok(())
    }

    /// Returns a snapshot of the current server pool.
    pub(crate) fn server_pool(&self) -> Vec<Server> {
        self.servers.iter().map(|s| s.to_server()).collect()
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
            let pool_snapshot: Vec<Server> = self.servers.iter().map(|s| s.to_server()).collect();
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
            servers.shuffle(&mut thread_rng());
            // sort_by is stable, meaning it will retain the order for equal elements.
            servers.sort_by(|a, b| a.failed_attempts.cmp(&b.failed_attempts));
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
        let mut connection = match server_addr.scheme() {
            #[cfg(feature = "websockets")]
            "ws" => {
                let ws = tokio_websockets::client::Builder::new()
                    .uri(server_addr.as_url_str())
                    .map_err(|err| {
                        ConnectError::with_source(crate::ConnectErrorKind::ServerParse, err)
                    })?
                    .connect()
                    .await
                    .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Io, err))?;

                let con = WebSocketAdapter::new(ws.0);
                Connection::new(Box::new(con), 0, self.connect_stats.clone())
            }
            #[cfg(feature = "websockets")]
            "wss" => {
                let tls_config =
                    Arc::new(tls::config_tls(&self.options).await.map_err(|err| {
                        ConnectError::with_source(crate::ConnectErrorKind::Tls, err)
                    })?);
                let tls_connector = tokio_rustls::TlsConnector::from(tls_config);
                let ws = tokio_websockets::client::Builder::new()
                    .connector(&tokio_websockets::Connector::Rustls(tls_connector))
                    .uri(server_addr.as_url_str())
                    .map_err(|err| {
                        ConnectError::with_source(crate::ConnectErrorKind::ServerParse, err)
                    })?
                    .connect()
                    .await
                    .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Io, err))?;
                let con = WebSocketAdapter::new(ws.0);
                Connection::new(Box::new(con), 0, self.connect_stats.clone())
            }
            _ => {
                let tcp_stream = if let Some(local_addr) = self.options.local_address {
                    let socket = if local_addr.is_ipv4() {
                        TcpSocket::new_v4()?
                    } else {
                        TcpSocket::new_v6()?
                    };
                    socket.bind(local_addr).map_err(|err| {
                        ConnectError::with_source(crate::ConnectErrorKind::Io, err)
                    })?;
                    socket.connect(*socket_addr).await?
                } else {
                    TcpStream::connect(socket_addr).await?
                };
                tcp_stream.set_nodelay(true)?;

                Connection::new(
                    Box::new(tcp_stream),
                    self.options.read_buffer_capacity.into(),
                    self.connect_stats.clone(),
                )
            }
        };

        let tls_connection = |connection: Connection| async {
            tracing::debug!("upgrading connection to TLS");
            let tls_config = Arc::new(
                tls::config_tls(&self.options)
                    .await
                    .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?,
            );
            let tls_connector = tokio_rustls::TlsConnector::from(tls_config);

            let domain = crate::rustls::pki_types::ServerName::try_from(server_addr.host())
                .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?;

            let tls_stream = tls_connector
                .connect(domain.to_owned(), connection.stream)
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
            && (self.options.tls_required || info.tls_required || tls_required)
        {
            connection = tls_connection(connection).await?;
        };

        // Discover servers from INFO.
        if !self.options.ignore_discovered_servers {
            for url in &info.connect_urls {
                let discovered_addr = url.parse::<ServerAddr>().map_err(|err| {
                    ConnectError::with_source(crate::ConnectErrorKind::ServerParse, err)
                })?;
                if !self.servers.iter().any(|s| s.addr == discovered_addr) {
                    tracing::debug!(
                        discovered_url = %url,
                        "adding discovered server"
                    );
                    self.servers
                        .push(ServerEntry::new_implicit(discovered_addr));
                }
            }
        }

        // Build CONNECT message with auth info.
        let tls_required = self.options.tls_required || server_addr.tls_required();
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
