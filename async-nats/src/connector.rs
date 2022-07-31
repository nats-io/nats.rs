use crate::connection::Connection;
use crate::Authorization;
use crate::ClientOp;
use crate::Protocol;
use crate::ServerAddr;
use crate::ServerInfo;
use crate::SocketAddr;
use crate::ToServerAddrs;
use crate::VERSION;
use bytes::BytesMut;
use std::cmp;
use std::collections::HashMap;
use std::io;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::io::ErrorKind;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_rustls::rustls::{self};

pub(crate) struct ConnectorOptions {
    pub(crate) tls_required: bool,
    pub(crate) certificates: Vec<PathBuf>,
    pub(crate) client_cert: Option<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) tls_client_config: Option<rustls::ClientConfig>,
    pub(crate) auth: Authorization,
    pub(crate) no_echo: bool,
}

/// Maintains a list of servers and establishes connections.
pub(crate) struct Connector {
    /// A map of servers and number of connect attempts.
    servers: HashMap<ServerAddr, usize>,
    options: ConnectorOptions,
}

impl Connector {
    pub(crate) fn new<A: ToServerAddrs>(
        addrs: A,
        options: ConnectorOptions,
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
                    Ok((server_info, mut connection)) => {
                        for url in &server_info.connect_urls {
                            let server_addr = url.parse::<ServerAddr>()?;
                            self.servers.entry(server_addr).or_insert(0);
                        }

                        let server_attempts = self.servers.get_mut(&server_addr).unwrap();
                        *server_attempts = 0;

                        let tls_required = self.options.tls_required || server_addr.tls_required();
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
                            echo: !self.options.no_echo,
                            headers: true,
                            no_responders: true,
                        };

                        match &self.options.auth {
                            Authorization::None => {
                                return Ok((server_info, connection));
                            }
                            Authorization::Token(token) => {
                                connect_info.auth_token = Some(token.clone())
                            }
                            Authorization::UserAndPassword(user, pass) => {
                                connect_info.user = Some(user.clone());
                                connect_info.pass = Some(pass.clone());
                            }
                            Authorization::Jwt(jwt, sign_fn) => {
                                match sign_fn.call(server_info.nonce.clone()).await {
                                    Ok(sig) => {
                                        connect_info.user_jwt = Some(jwt.clone());
                                        connect_info.signature = Some(sig);
                                    }
                                    Err(e) => {
                                        panic!(
                    "JWT auth is disabled. sign error: {} (possibly invalid key or corrupt cred file?)",
                    e
                );
                                    }
                                }
                            }
                        }

                        connection.write_op(ClientOp::Connect(connect_info)).await?;
                        connection.flush().await?;

                        match connection.read_op().await? {
                            Some(ServerOp::Pong) => {
                                return Ok((server_info, connection));
                            }
                            Some(ServerOp::Error(err)) => {
                                return Err(io::Error::new(
                                    ErrorKind::InvalidInput,
                                    err.to_string(),
                                ));
                            }
                            None => {
                                return Err(io::Error::new(
                                    ErrorKind::BrokenPipe,
                                    "connection aborted",
                                ))
                            }
                        }
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

            // Use the server-advertised hostname to validate if given as a hostname, not an IP address
            let domain = if let Ok(server_hostname @ rustls::ServerName::DnsName(_)) =
                rustls::ServerName::try_from(info.host.as_str())
            {
                server_hostname
            } else if let Ok(tls_hostname @ rustls::ServerName::DnsName(_)) =
                rustls::ServerName::try_from(tls_host)
            {
                tls_hostname
            } else {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "cannot determine hostname for TLS connection",
                ));
            };

            connection = Connection {
                stream: Box::new(tls_connector.connect(domain, connection.stream).await?),
                buffer: BytesMut::new(),
            };
        };

        Ok((*info, connection))
    }
}
