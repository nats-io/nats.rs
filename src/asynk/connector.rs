use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::sync::Arc;

use async_rustls::rustls::ClientConfig;
use async_rustls::webpki::DNSNameRef;
use async_rustls::TlsConnector;

use crate::asynk::proto::{self, ClientOp, ServerOp};
use crate::auth_utils;
use crate::secure_wipe::SecureString;
use crate::smol::io::{self, BufReader};
use crate::smol::{net, prelude::*, Timer};
use crate::{connect::ConnectInfo, inject_io_failure, AuthStyle, Options, ServerInfo};

/// Maintains a list of servers and establishes connections.
///
/// Clients use this helper to hold a list of known servers discovered through INFO messages,
/// reconnect when the connection is lost, and do exponential backoff after failed connect
/// attempts.
pub(crate) struct Connector {
    /// A map of servers and number of connect attempts.
    attempts: HashMap<Server, usize>,

    /// Configured options for establishing connections.
    options: Arc<Options>,

    /// TLS connector.
    tls_connector: TlsConnector,
}

impl Connector {
    /// Creates a new connector with the URLs and options.
    pub(crate) fn new(url: &str, options: Arc<Options>) -> io::Result<Connector> {
        let mut tls_config = ClientConfig::new();

        // Include system root certificates.
        for root in rustls_native_certs::load_native_certs()
            .expect("could not load system certs")
            .roots
        {
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
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid certificate file")
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
                            format!("invalid client certificate and key pair: {}", err),
                        )
                    })?;
            }
        }

        let mut connector = Connector {
            attempts: HashMap::new(),
            options,
            tls_connector: TlsConnector::from(Arc::new(tls_config)),
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
    /// If `use_backoff` is `true`, this method will try connecting in a loop and will back off
    /// after failed connect attempts.
    pub(crate) async fn connect(
        &mut self,
        use_backoff: bool,
    ) -> io::Result<(
        ServerInfo,
        Pin<Box<dyn AsyncRead + Send>>,
        Pin<Box<dyn AsyncWrite + Send>>,
    )> {
        // The last seen error, which gets returned if all connect attempts fail.
        let mut last_err = Error::new(ErrorKind::AddrNotAvailable, "no socket addresses");

        loop {
            // Shuffle the list of servers.
            let mut servers: Vec<Server> = self.get_servers()?;
            fastrand::shuffle(&mut servers);

            // Iterate over the server list in random order.
            for server in &servers {
                // Calculate sleep duration for exponential backoff and bump the reconnect counter.
                let reconnects = self.attempts.get_mut(server).unwrap();
                let sleep_duration = self.options.reconnect_delay_callback.call(*reconnects);
                *reconnects += 1;

                // Resolve the server URL to socket addresses.
                let host = server.host.clone();
                let port = server.port;
                let mut addrs = match net::resolve((host.as_str(), port)).await {
                    Ok(addrs) => addrs,
                    Err(err) => {
                        last_err = err;
                        continue;
                    }
                };

                // Shuffle the resolved socket addresses.
                fastrand::shuffle(&mut addrs);

                for addr in addrs {
                    // Sleep for some time if this is not the first connection attempt for this
                    // server.
                    Timer::after(sleep_duration).await;

                    // Try connecting to this address.
                    let res = self.connect_addr(addr, server).await;

                    // Check if connecting worked out.
                    let (server_info, reader, writer) = match res {
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
                    return Ok((server_info, reader, writer));
                }
            }

            if !use_backoff {
                // All connect attempts have failed.
                return Err(last_err);
            }
        }
    }

    /// Attempts to establish a connection to a single socket address.
    async fn connect_addr(
        &self,
        addr: net::SocketAddr,
        server: &Server,
    ) -> io::Result<(
        ServerInfo,
        Pin<Box<dyn AsyncRead + Send>>,
        Pin<Box<dyn AsyncWrite + Send>>,
    )> {
        // Inject random I/O failures when testing.
        inject_io_failure()?;

        // Connect to the remote socket.
        let mut stream = net::TcpStream::connect(addr).await?;

        // Expect an INFO message.
        let mut line = crate::SecureVec::with_capacity(512);
        while !line.ends_with(b"\r\n") {
            let byte = &mut [0];
            stream.read_exact(byte).await?;
            line.push(byte[0]);
        }
        let server_info = match proto::decode(&line[..]).await? {
            Some(ServerOp::Info(server_info)) => server_info,
            Some(op) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("expected INFO, received: {:?}", op),
                ));
            }
            None => return Err(Error::new(ErrorKind::UnexpectedEof, "connection closed")),
        };

        // Check if TLS authentication is required:
        // - Has `self.options.tls_required(true)` been set?
        // - Was the server address prefixed with `tls://`?
        // - Does the INFO line contain `tls_required: true`?
        let tls_required =
            self.options.tls_required || server.tls_required || server_info.tls_required;

        // Upgrade to TLS if required.
        #[allow(clippy::type_complexity)]
        let (reader, mut writer): (
            Pin<Box<dyn AsyncRead + Send>>,
            Pin<Box<dyn AsyncWrite + Send>>,
        ) = if tls_required {
            // Inject random I/O failures when testing.
            inject_io_failure()?;

            // Connect using TLS.
            let res = DNSNameRef::try_from_ascii_str(server.host.as_str());
            let name = res.map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "the server name is not ASCII")
            })?;
            let stream = self.tls_connector.connect(name, stream).await?;

            // Split the TLS stream into a reader and a writer.
            let (r, w) = io::split(stream);
            (r.boxed_reader(), w.boxed_writer())
        } else {
            // Split the TCP stream into a reader and a writer.
            let (r, w) = io::split(stream);
            (r.boxed_reader(), w.boxed_writer())
        };

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

        // Send CONNECT and INFO messages.
        proto::encode(&mut writer, ClientOp::Connect(connect_info)).await?;
        proto::encode(&mut writer, ClientOp::Ping).await?;
        writer.flush().await?;

        let mut reader = BufReader::new(reader);

        // Wait for a PONG.
        loop {
            match proto::decode(&mut reader).await? {
                // If we get PONG, the server is happy and we're done connecting.
                Some(ServerOp::Pong) => break,

                // Respond to a PING with a PONG.
                Some(ServerOp::Ping) => {
                    proto::encode(&mut writer, ClientOp::Pong).await?;
                    writer.flush().await?;
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

        Ok((server_info, reader.into_inner(), writer))
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
        let mut host_port_splits = host_port.split(':');
        let host_opt = host_port_splits.next();
        if host_opt.map_or(true, str::is_empty) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid URL provided: {}", url),
            ));
        };
        let host = host_opt.unwrap().to_string();

        // Extract the port.
        let port_opt = host_port_splits
            .next()
            .and_then(|port_str| port_str.parse().ok());
        let port = port_opt.unwrap_or(4222);

        Ok(Server {
            host,
            port,
            tls_required,
        })
    }
}
