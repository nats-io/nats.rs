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

use lazy_static::__Deref;
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::prelude::*;
use std::io::{self, BufReader, Error, ErrorKind};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use url::{Host, Url};

use crate::auth_utils;
use crate::proto::{self, ClientOp, ServerOp};
use crate::rustls::{ClientConfig, ClientConnection};
use crate::secure_wipe::SecureString;
use crate::{connect::ConnectInfo, inject_io_failure, AuthStyle, Options, ServerInfo};

/// Maintains a list of servers and establishes connections.
///
/// Clients use this helper to hold a list of known servers discovered through
/// INFO messages, reconnect when the connection is lost, and do exponential
/// backoff after failed connect attempts.
pub(crate) struct Connector {
    /// A map of servers and number of connect attempts.
    attempts: HashMap<ServerAddress, usize>,

    /// Configured options for establishing connections.
    options: Arc<Options>,

    /// TLS config.
    tls_config: Arc<ClientConfig>,
}

fn configure_tls(options: &Arc<Options>) -> Result<ClientConfig, Error> {
    let mut root_store = rustls::RootCertStore::empty();

    // load native system certs only if user did not specify them
    if options.tls_client_config.is_some() || options.certificates.is_empty() {
        let native_certs = rustls_native_certs::load_native_certs().map_err(|err| {
            io::Error::new(
                ErrorKind::Other,
                format!("could not load platform certs: {err}"),
            )
        })?;

        root_store.add_parsable_certificates(native_certs);
    }

    if let Some(config) = &options.tls_client_config {
        Ok(config.to_owned())
    } else {
        // Include user-provided certificates
        for path in &options.certificates {
            let mut pem = BufReader::new(std::fs::File::open(path)?);
            let certs = rustls_pemfile::certs(&mut pem).collect::<io::Result<Vec<_>>>()?;
            let trust_anchors = certs
                .into_iter()
                .map(|cert| webpki::anchor_from_trusted_cert(&cert).map(|ta| ta.to_owned()))
                .collect::<Result<Vec<_>, webpki::Error>>()
                .map_err(|err| {
                    io::Error::new(
                        ErrorKind::InvalidInput,
                        format!("could not load certs: {err}"),
                    )
                })?;
            root_store.extend(trust_anchors);
        }

        let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

        if let Some(cert) = &options.client_cert {
            if let Some(key) = &options.client_key {
                let cert = auth_utils::load_certs(cert)?;
                let key = auth_utils::load_key(key)?;

                return builder.with_client_auth_cert(cert, key).map_err(|_| {
                    io::Error::new(ErrorKind::Other, "could not add certificate or key")
                });
            } else {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    "found certificate, but no key",
                ));
            }
        }

        // if there are no client certs provided, connect with just TLS.
        Ok(builder.with_no_client_auth())
    }
}

impl Connector {
    /// Creates a new connector with the URLs and options.
    pub(crate) fn new(urls: Vec<ServerAddress>, options: Arc<Options>) -> io::Result<Connector> {
        let tls_config = configure_tls(&options)?;

        let connector = Connector {
            attempts: urls.into_iter().map(|url| (url, 0)).collect(),
            options,
            tls_config: Arc::new(tls_config),
        };

        Ok(connector)
    }

    /// Adds an URL to the list of servers.
    pub(crate) fn add_server(&mut self, url: ServerAddress) {
        self.attempts.insert(url, 0);
    }

    pub(crate) fn get_options(&self) -> Arc<Options> {
        self.options.clone()
    }

    /// Get the list of servers with enough reconnection attempts left
    fn get_servers(&mut self) -> io::Result<Vec<ServerAddress>> {
        let servers: Vec<_> = self
            .attempts
            .iter()
            .filter_map(
                |(server, reconnects)| match self.options.max_reconnects.as_ref() {
                    None => Some(server),
                    Some(max) if reconnects < max => Some(server),
                    Some(_) => None,
                },
            )
            .cloned()
            .collect();

        if servers.is_empty() {
            Err(Error::new(
                ErrorKind::NotFound,
                "no servers remaining to connect to",
            ))
        } else {
            Ok(servers)
        }
    }

    /// Creates a new connection to one of the known URLs.
    ///
    /// If `use_backoff` is `true`, this method will try connecting in a loop
    /// and will back off after failed connect attempts.
    pub(crate) fn connect(&mut self, use_backoff: bool) -> io::Result<(ServerInfo, NatsStream)> {
        // The last seen error, which gets returned if all connect attempts
        // fail.
        let mut last_err = Error::new(ErrorKind::AddrNotAvailable, "no socket addresses");

        loop {
            // Shuffle the list of servers.
            let mut servers = self.get_servers()?;
            fastrand::shuffle(&mut servers);

            // Iterate over the server list in random order.
            for server in &servers {
                // Calculate sleep duration for exponential backoff and bump the
                // reconnect counter.
                let reconnects = self.attempts.get_mut(server).unwrap();
                let sleep_duration = self.options.reconnect_delay_callback.call(*reconnects);
                *reconnects += 1;

                let lookup_res = server.socket_addrs();

                let mut addrs = match lookup_res {
                    Ok(addrs) => addrs.collect::<Vec<_>>(),
                    Err(err) => {
                        last_err = err;
                        continue;
                    }
                };

                // Shuffle the resolved socket addresses.
                fastrand::shuffle(&mut addrs);

                for addr in addrs {
                    // Sleep for some time if this is not the first connection
                    // attempt for this server.
                    thread::sleep(sleep_duration);

                    // Try connecting to this address.
                    let res = self.connect_addr(addr, server);

                    // Check if connecting worked out.
                    let (server_info, stream) = match res {
                        Ok(val) => val,
                        Err(err) => {
                            last_err = err;
                            continue;
                        }
                    };

                    // Add URLs discovered through the INFO message.
                    for url in &server_info.connect_urls {
                        self.add_server(url.parse()?);
                    }

                    *self.attempts.get_mut(server).unwrap() = 0;
                    return Ok((server_info, stream));
                }
            }

            if !use_backoff {
                // All connect attempts have failed.
                return Err(last_err);
            }
        }
    }

    /// Attempts to establish a connection to a single socket address.
    fn connect_addr(
        &self,
        addr: SocketAddr,
        server: &ServerAddress,
    ) -> io::Result<(ServerInfo, NatsStream)> {
        // Inject random I/O failures when testing.
        inject_io_failure()?;

        // Connect to the remote socket.
        let mut stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;

        // Expect an INFO message.
        let mut line = crate::SecureVec::with_capacity(1024);
        while !line.ends_with(b"\r\n") {
            let byte = &mut [0];
            stream.read_exact(byte)?;
            line.push(byte[0]);
        }
        let server_info = match proto::decode(&line[..])? {
            Some(ServerOp::Info(server_info)) => server_info,
            Some(op) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("expected INFO, received: {op:?}"),
                ));
            }
            None => {
                return Err(Error::new(ErrorKind::UnexpectedEof, "connection closed"));
            }
        };

        // Check if TLS authentication is required:
        // - Has `self.options.tls_required(true)` been set?
        // - Was the server address prefixed with `tls://`?
        // - Does the INFO line contain `tls_required: true`?
        let tls_required =
            self.options.tls_required || server.tls_required() || server_info.tls_required;

        // Upgrade to TLS if required.
        let session = if tls_required {
            // Inject random I/O failures when testing.
            inject_io_failure()?;

            // Connect using TLS.
            let server_name = webpki::types::ServerName::try_from(server.host().to_string())
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot determine hostname for TLS connection",
                    )
                })?;

            Some(
                ClientConnection::new(self.tls_config.clone(), server_name)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
            )
        } else {
            None
        };
        let mut stream = NatsStream::new(stream, session)?;

        // Data that will be formatted as a CONNECT message.
        let mut connect_info = ConnectInfo {
            tls_required,
            name: self.options.name.clone().map(SecureString::from),
            pedantic: false,
            verbose: false,
            lang: crate::LANG.to_string(),
            version: crate::VERSION.to_string(),
            protocol: crate::connect::Protocol::Dynamic,
            user: None,
            pass: None,
            auth_token: None,
            user_jwt: None,
            nkey: None,
            signature: None,
            echo: !self.options.no_echo,
            headers: true,
            no_responders: true,
        };

        let server_auth = server.auth();
        let auth = if let AuthStyle::NoAuth = server_auth {
            &self.options.auth
        } else {
            &server_auth
        };

        // Fill in the info that authenticates the client.
        match auth {
            AuthStyle::NoAuth => {}
            AuthStyle::UserPass(user, pass) => {
                connect_info.user = Some(SecureString::from(user.to_string()));
                connect_info.pass = Some(SecureString::from(pass.to_string()));
            }
            AuthStyle::Token(token) => {
                connect_info.auth_token = Some(token.to_string().into());
            }
            AuthStyle::Credentials { jwt_cb, sig_cb } => {
                let jwt = jwt_cb()?;
                let sig = sig_cb(server_info.nonce.as_bytes())?;
                connect_info.user_jwt = Some(jwt);
                connect_info.signature = Some(sig);
            }
            AuthStyle::NKey { nkey_cb, sig_cb } => {
                let nkey = nkey_cb()?;
                let sig = sig_cb(server_info.nonce.as_bytes())?;
                connect_info.nkey = Some(nkey);
                connect_info.signature = Some(sig);
            }
        }

        // Send CONNECT and PING messages.
        proto::encode(&mut stream, ClientOp::Connect(&connect_info))?;
        proto::encode(&mut stream, ClientOp::Ping)?;
        stream.flush()?;

        let mut reader = BufReader::new(stream.clone());

        // Wait for a PONG.
        loop {
            match proto::decode(&mut reader)? {
                // If we get PONG, the server is happy and we're done
                // connecting.
                Some(ServerOp::Pong) => break,

                // Respond to a PING with a PONG.
                Some(ServerOp::Ping) => {
                    proto::encode(&mut stream, ClientOp::Pong)?;
                    stream.flush()?;
                }

                // No other operations should arrive at this time.
                Some(op) => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected line while connecting: {op:?}"),
                    ));
                }

                // Error if the connection was closed.
                None => {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "connection closed while waiting for the first PONG",
                    ));
                }
            }
        }

        Ok((server_info, stream))
    }
}

/// A raw NATS stream of bytes.
///
/// The stream uses the TCP protocol, optionally secured by TLS.
#[derive(Clone)]
pub(crate) struct NatsStream {
    flavor: Arc<Flavor>,
}

enum Flavor {
    Tcp(TcpStream),
    Tls(Box<Mutex<TlsStream>>),
}

struct TlsStream {
    tcp: TcpStream,
    session: ClientConnection,
}

impl NatsStream {
    /// Creates a NATS stream from a TCP stream and an optional TLS session.
    fn new(tcp: TcpStream, session: Option<ClientConnection>) -> io::Result<NatsStream> {
        let flavor = match session {
            None => Flavor::Tcp(tcp),
            Some(session) => {
                tcp.set_nonblocking(true)?;
                Flavor::Tls(Box::new(Mutex::new(TlsStream { tcp, session })))
            }
        };
        let flavor = Arc::new(flavor);
        Ok(NatsStream { flavor })
    }

    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match &*self.flavor {
            Flavor::Tcp(tcp) => tcp.set_write_timeout(timeout),
            Flavor::Tls(tls) => tls.lock().tcp.set_write_timeout(timeout),
        }
    }

    /// Will attempt to shutdown the underlying stream.
    pub(crate) fn shutdown(&self) {
        match &*self.flavor {
            Flavor::Tcp(tcp) => tcp.shutdown(Shutdown::Both),
            Flavor::Tls(tls) => tls.lock().tcp.shutdown(Shutdown::Both),
        }
        .ok();
    }
}

impl Read for NatsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        <&NatsStream as Read>::read(&mut &*self, buf)
    }
}

impl Read for &NatsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &*self.flavor {
            Flavor::Tcp(tcp) => (tcp.deref()).read(buf),
            Flavor::Tls(tls) => tls_op(tls, |session, eof| match session.reader().read(buf) {
                Ok(0) if !eof => Err(io::ErrorKind::WouldBlock.into()),
                res => res,
            }),
        }
    }
}

impl Write for NatsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        <&NatsStream as Write>::write(&mut &*self, buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        <&NatsStream as Write>::flush(&mut &*self)
    }
}

impl Write for &NatsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &*self.flavor {
            Flavor::Tcp(tcp) => (tcp.deref()).write(buf),
            Flavor::Tls(tls) => tls_op(tls, |session, _| session.writer().write(buf)),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &*self.flavor {
            Flavor::Tcp(tcp) => (tcp.deref()).flush(),
            Flavor::Tls(tls) => tls_op(tls, |session, _| session.writer().flush()),
        }
    }
}

/// Performs a blocking operation on a TLS stream.
///
/// However, note that the inner TCP stream is in non-blocking mode.
fn tls_op<T: std::fmt::Debug>(
    tls: &Mutex<TlsStream>,
    mut op: impl FnMut(&mut ClientConnection, bool) -> io::Result<T>,
) -> io::Result<T> {
    loop {
        let mut tls = tls.lock();
        let TlsStream { tcp, session } = &mut *tls;
        let mut eof = false;

        // If necessary, read TLS messages.
        if session.wants_read() {
            match session.read_tls(tcp) {
                Ok(0) => eof = true,
                Ok(_) => {
                    session
                        .process_new_packets()
                        .map_err(|err| Error::new(ErrorKind::Other, format!("TLS error: {err}")))?;
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                Err(err) => return Err(err),
            }
        }

        // If necessary, write TLS messages.
        if session.wants_write() {
            match session.write_tls(tcp) {
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                Err(err) => return Err(err),
            }
        }

        // Try the non-blocking read/write/flush operation.
        match op(session, eof) {
            Err(err) if err.kind() == ErrorKind::WouldBlock => {}
            res => return res,
        }

        tls_wait(tls)?;
    }
}

/// Waits until the TLS stream becomes ready.
fn tls_wait(mut tls: MutexGuard<'_, TlsStream>) -> io::Result<()> {
    #[cfg(unix)]
    use {
        libc::{self as sys, poll, pollfd},
        std::os::unix::io::AsRawFd,
    };
    #[cfg(windows)]
    use {
        std::os::windows::io::AsRawSocket,
        winapi::um::winsock2::{self as sys, WSAPoll as poll, WSAPOLLFD as pollfd},
    };

    let TlsStream { tcp, session } = &mut *tls;

    // Initialize a pollfd object with readiness events we're looking for.
    #[allow(trivial_numeric_casts)]
    let mut pollfd = pollfd {
        #[cfg(unix)]
        fd: tcp.as_raw_fd() as _,
        #[cfg(windows)]
        fd: tcp.as_raw_socket() as _,
        #[cfg(unix)]
        events: sys::POLLERR,
        #[cfg(windows)]
        events: 0,
        revents: 0,
    };
    if session.wants_read() {
        pollfd.events |= sys::POLLIN;
    }
    if session.wants_write() {
        pollfd.events |= sys::POLLOUT;
    }

    // Make sure to drop the lock before blocking on `poll()`!
    // This way concurrent operations on the TLS stream won't block each other.
    drop(tls);

    // Wait until the TCP stream becomes ready.
    #[allow(unsafe_code)]
    while unsafe { poll(&mut pollfd, 1, -1) } == -1 {
        let err = Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }

    Ok(())
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
    type Err = Error;

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
            Error::new(
                ErrorKind::InvalidInput,
                format!("NATS server URL is invalid: {e}"),
            )
        })?;

        Self::from_url(url)
    }
}

impl ServerAddress {
    /// Check if the URL is a valid NATS server address.
    pub fn from_url(url: Url) -> io::Result<Self> {
        if url.scheme() != "nats" && url.scheme() != "tls" {
            return Err(Error::new(
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

    pub(crate) fn auth(&self) -> AuthStyle {
        if let Some(password) = self.0.password() {
            if self.0.username() == "" {
                AuthStyle::NoAuth
            } else {
                AuthStyle::UserPass(self.0.username().to_string(), password.to_string())
            }
        } else if "" != self.0.username() {
            AuthStyle::Token(self.0.username().to_string())
        } else {
            AuthStyle::NoAuth
        }
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
    pub fn username(&self) -> Option<SecureString> {
        let user = self.0.username();
        if user.is_empty() {
            None
        } else {
            Some(SecureString::from(user.to_string()))
        }
    }

    /// Returns the optional password in the url.
    pub fn password(&self) -> Option<SecureString> {
        self.0
            .password()
            .map(|password| SecureString::from(password.to_string()))
    }

    /// Return the sockets from resolving the server address.
    ///
    /// # Fault injection
    ///
    /// If compiled with the `"fault_injection"` feature this method might fail artificially.
    pub fn socket_addrs(&self) -> io::Result<impl Iterator<Item = SocketAddr>> {
        inject_io_failure().and_then(|_| (self.host(), self.port()).to_socket_addrs())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_address_ipv6() {
        let address = ServerAddress::from_str("nats://[::]").unwrap();
        assert_eq!(address.host(), "::")
    }

    #[test]
    fn server_address_ipv4() {
        let address = ServerAddress::from_str("nats://127.0.0.1").unwrap();
        assert_eq!(address.host(), "127.0.0.1")
    }

    #[test]
    fn server_address_domain() {
        let address = ServerAddress::from_str("nats://example.com").unwrap();
        assert_eq!(address.host(), "example.com")
    }

    #[test]
    fn server_address_no_auth() {
        let address = ServerAddress::from_str("nats://localhost").unwrap();
        assert!(matches!(address.auth(), AuthStyle::NoAuth));
    }

    #[test]
    fn server_address_token_auth() {
        let address = ServerAddress::from_str("nats://mytoken@localhost").unwrap();
        assert!(matches!(address.auth(), AuthStyle::Token(token) if &token == "mytoken"));
    }

    #[test]
    fn server_address_user_auth() {
        let address = ServerAddress::from_str("nats://myuser:mypass@localhost").unwrap();
        assert!(
            matches!(address.auth(), AuthStyle::UserPass(username, password) if &username == "myuser" && &password == "mypass")
        );
    }
}
