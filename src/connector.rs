use std::collections::HashMap;
use std::io::prelude::*;
use std::io::{self, BufReader, Error, ErrorKind};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use parking_lot::{Mutex, MutexGuard};
use rustls::{ClientConfig, ClientSession, Session};
use webpki::DNSNameRef;

use crate::auth_utils;
use crate::proto::{self, ClientOp, ServerOp};
use crate::secure_wipe::SecureString;
use crate::{
    connect::ConnectInfo, inject_io_failure, AuthStyle, Options, ServerInfo,
};

/// Maintains a list of servers and establishes connections.
///
/// Clients use this helper to hold a list of known servers discovered through
/// INFO messages, reconnect when the connection is lost, and do exponential
/// backoff after failed connect attempts.
pub(crate) struct Connector {
    /// A map of servers and number of connect attempts.
    attempts: HashMap<Server, usize>,

    /// Configured options for establishing connections.
    options: Arc<Options>,

    /// TLS config.
    tls_config: Arc<ClientConfig>,
}

impl Connector {
    /// Creates a new connector with the URLs and options.
    pub(crate) fn new(
        url: &str,
        options: Arc<Options>,
    ) -> io::Result<Connector> {
        let mut tls_config = ClientConfig::new();

        // Include system root certificates.
        //
        // On Windows, some certificates cannot be loaded by rustls
        // for whatever reason, so we simply skip them.
        // See https://github.com/ctz/rustls-native-certs/issues/5
        let roots = match rustls_native_certs::load_native_certs() {
            Ok(store) | Err((Some(store), _)) => store.roots,
            Err((None, _)) => Vec::new(),
        };
        for root in roots {
            tls_config.root_store.roots.push(root);
        }

        // Include user-provided certificates.
        for path in &options.certificates {
            let contents = std::fs::read(path)?;
            let mut cursor = std::io::Cursor::new(contents);

            tls_config
                .root_store
                .add_pem_file(&mut cursor)
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid certificate file",
                    )
                })?;
        }

        if let Some(cert) = &options.client_cert {
            if let Some(key) = &options.client_key {
                tls_config
                    .set_single_client_cert(
                        auth_utils::load_certs(cert)?,
                        auth_utils::load_key(key)?,
                    )
                    .map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "invalid client certificate and key pair: {}",
                                err
                            ),
                        )
                    })?;
            }
        }

        let mut connector = Connector {
            attempts: HashMap::new(),
            options,
            tls_config: Arc::new(tls_config),
        };

        // Add all URLs in the comma-separated list.
        for url in url.split(',') {
            connector.add_url(url)?;
        }

        Ok(connector)
    }

    /// Adds an URL to the list of servers.
    pub(crate) fn add_url(&mut self, url: &str) -> io::Result<()> {
        let server = Server::new(url)?;
        self.attempts.insert(server, 0);
        Ok(())
    }

    pub(crate) fn get_options(&self) -> Arc<Options> {
        self.options.clone()
    }

    /// Get the list of servers with enough reconnection attempts left
    fn get_servers(&mut self) -> io::Result<Vec<Server>> {
        let mut servers: Vec<Server> = self.attempts.keys().cloned().collect();

        servers = servers
            .into_iter()
            .filter(|server| {
                let reconnects = self.attempts.get_mut(server).unwrap();
                match self.options.max_reconnects.as_ref() {
                    Some(max) => max > reconnects,
                    None => true,
                }
            })
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
    pub(crate) fn connect(
        &mut self,
        use_backoff: bool,
    ) -> io::Result<(ServerInfo, NatsStream)> {
        // The last seen error, which gets returned if all connect attempts
        // fail.
        let mut last_err =
            Error::new(ErrorKind::AddrNotAvailable, "no socket addresses");

        loop {
            // Shuffle the list of servers.
            let mut servers: Vec<Server> = self.get_servers()?;
            fastrand::shuffle(&mut servers);

            // Iterate over the server list in random order.
            for server in &servers {
                // Calculate sleep duration for exponential backoff and bump the
                // reconnect counter.
                let reconnects = self.attempts.get_mut(server).unwrap();
                let sleep_duration =
                    self.options.reconnect_delay_callback.call(*reconnects);
                *reconnects += 1;

                // Resolve the server URL to socket addresses.
                let host = server.host.clone();
                let port = server.port;

                // Inject random I/O failures when testing.
                let fault_injection = inject_io_failure();

                let lookup_res = fault_injection
                    .and_then(|_| (host.as_str(), port).to_socket_addrs());

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
                        self.add_url(url)?;
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
        server: &Server,
    ) -> io::Result<(ServerInfo, NatsStream)> {
        // Inject random I/O failures when testing.
        inject_io_failure()?;

        // Connect to the remote socket.
        let mut stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;

        // Expect an INFO message.
        let mut line = crate::SecureVec::with_capacity(512);
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
                    format!("expected INFO, received: {:?}", op),
                ));
            }
            None => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
        };

        // Check if TLS authentication is required:
        // - Has `self.options.tls_required(true)` been set?
        // - Was the server address prefixed with `tls://`?
        // - Does the INFO line contain `tls_required: true`?
        let tls_required = self.options.tls_required
            || server.tls_required
            || server_info.tls_required;

        // Upgrade to TLS if required.
        let session = if tls_required {
            // Inject random I/O failures when testing.
            inject_io_failure()?;

            // Connect using TLS.
            let dns_name = DNSNameRef::try_from_ascii_str(&server_info.host)
                .or_else(|_| {
                    DNSNameRef::try_from_ascii_str(server.host.as_str())
                })
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot determine hostname for TLS connection",
                    )
                })?;
            Some(ClientSession::new(&self.tls_config, dns_name))
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
            user: None,
            pass: None,
            auth_token: None,
            user_jwt: None,
            nkey: None,
            signature: None,
            echo: !self.options.no_echo,
            headers: true,
        };

        // Fill in the info that authenticates the client.
        match &self.options.auth {
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
                        format!("unexpected line while connecting: {:?}", op),
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

/// A parsed URL.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Server {
    host: String,
    port: u16,
    tls_required: bool,
}

impl Server {
    /// Creates a new server from an URL.
    ///
    /// Returns an error if the URL cannot be parsed.
    fn new(url: &str) -> io::Result<Server> {
        // Make sure this isn't a comma-separated URL list.
        if url.contains(',') {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "only one server URL should be passed to Server::new",
            ));
        }

        // Check if the URL starts with "tls://".
        let tls_required = if let Some("tls") = url.split("://").next() {
            true
        } else {
            false
        };

        // Extract the part after "://".
        let scheme_separator = "://";
        let host_port = if let Some(idx) = url.find(&scheme_separator) {
            &url[idx + scheme_separator.len()..]
        } else {
            url
        };

        // Extract the host.
        let (host, port) = if host_port.starts_with('[') {
            // ipv6
            let close_idx = if let Some(ci) = host_port.find(']') {
                ci
            } else {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("invalid URL provided: {}", url),
                ));
            };

            let host = host_port[1..close_idx].to_string();

            let port = if host_port.len() == close_idx + 1 {
                4222
            } else if let Ok(port) = host_port[close_idx + 2..].parse() {
                port
            } else {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("unable to parse port number in URL: {}", url),
                ));
            };

            (host, port)
        } else {
            let mut host_port_splits = host_port.split(':');
            let host_opt = host_port_splits.next();
            if host_opt.map_or(true, str::is_empty) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("invalid URL provided: {}", url),
                ));
            };

            let host = host_opt.unwrap().to_string();

            let port_opt = host_port_splits
                .next()
                .and_then(|port_str| port_str.parse().ok());
            let port = port_opt.unwrap_or(4222);

            if host_port_splits.next().is_some() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "unable to parse port number in URL: {}. \
                        If you are trying to use ipv6, please wrap \
                        the address in square brackets: [{}] etc... \
                        with an optional colon and port after the \
                        closing bracket.",
                        url, url
                    ),
                ));
            }

            (host, port)
        };

        // Extract the port.

        Ok(Server {
            host,
            port,
            tls_required,
        })
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
    session: ClientSession,
}

impl NatsStream {
    /// Creates a NATS stream from a TCP stream and an optional TLS session.
    fn new(
        tcp: TcpStream,
        session: Option<ClientSession>,
    ) -> io::Result<NatsStream> {
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

    pub(crate) fn set_write_timeout(
        &self,
        timeout: Option<Duration>,
    ) -> io::Result<()> {
        match &*self.flavor {
            Flavor::Tcp(tcp) => tcp.set_write_timeout(timeout),
            Flavor::Tls(tls) => tls.lock().tcp.set_write_timeout(timeout),
        }
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
            Flavor::Tcp(tcp) => (&*tcp).read(buf),
            Flavor::Tls(tls) => {
                tls_op(tls, |session, eof| match session.read(buf) {
                    Ok(0) if !eof => Err(io::ErrorKind::WouldBlock.into()),
                    res => res,
                })
            }
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
            Flavor::Tcp(tcp) => (&*tcp).write(buf),
            Flavor::Tls(tls) => tls_op(tls, |session, _| session.write(buf)),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &*self.flavor {
            Flavor::Tcp(tcp) => (&*tcp).flush(),
            Flavor::Tls(tls) => tls_op(tls, |session, _| session.flush()),
        }
    }
}

/// Performs a blocking operation on a TLS stream.
///
/// However, note that the inner TCP stream is in non-blocking mode.
fn tls_op<T: std::fmt::Debug>(
    tls: &Mutex<TlsStream>,
    mut op: impl FnMut(&mut ClientSession, bool) -> io::Result<T>,
) -> io::Result<T> {
    loop {
        let mut tls = tls.lock();
        let TlsStream { tcp, session } = &mut *tls;
        let mut eof = false;

        // If necessary, read TLS messages.
        if session.wants_read() {
            match session.read_tls(tcp) {
                Ok(0) => eof = true,
                Ok(_) => session.process_new_packets().map_err(|err| {
                    Error::new(ErrorKind::Other, format!("TLS error: {}", err))
                })?,
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
        winapi::um::winsock2::{
            self as sys, WSAPoll as poll, WSAPOLLFD as pollfd,
        },
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
