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

use crate::connection::Connection;
use crate::connection::State;
use crate::tls;
use crate::Authorization;
use crate::ClientError;
use crate::ClientOp;
use crate::ConnectError;
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
use bytes::BytesMut;
use std::cmp;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::BufWriter;
use tokio::io::ErrorKind;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_rustls::rustls;

pub(crate) struct ConnectorOptions {
    pub(crate) tls_required: bool,
    pub(crate) certificates: Vec<PathBuf>,
    pub(crate) client_cert: Option<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) tls_client_config: Option<rustls::ClientConfig>,
    pub(crate) auth: Authorization,
    pub(crate) no_echo: bool,
    pub(crate) connection_timeout: Duration,
    pub(crate) name: Option<String>,
    pub(crate) ignore_discovered_servers: bool,
}

/// Maintains a list of servers and establishes connections.
pub(crate) struct Connector {
    /// A map of servers and number of connect attempts.
    servers: HashMap<ServerAddr, usize>,
    options: ConnectorOptions,
    pub(crate) events_tx: tokio::sync::mpsc::Sender<Event>,
    pub(crate) state_tx: tokio::sync::watch::Sender<State>,
}

impl Connector {
    pub(crate) fn new<A: ToServerAddrs>(
        addrs: A,
        options: ConnectorOptions,
        events_tx: tokio::sync::mpsc::Sender<Event>,
        state_tx: tokio::sync::watch::Sender<State>,
    ) -> Result<Connector, io::Error> {
        let servers = addrs.to_server_addrs()?.map(|addr| (addr, 0)).collect();

        Ok(Connector {
            servers,
            options,
            events_tx,
            state_tx,
        })
    }

    pub(crate) async fn connect(&mut self) -> Result<(ServerInfo, Connection), io::Error> {
        loop {
            match self.try_connect().await {
                Ok(inner) => return Ok(inner),
                Err(error) => {
                    self.events_tx
                        .send(Event::ClientError(ClientError::Other(error.to_string())))
                        .await
                        .ok();
                }
            }
        }
    }

    pub(crate) async fn try_connect(&mut self) -> Result<(ServerInfo, Connection), ConnectError> {
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

            let socket_addrs = server_addr
                .socket_addrs()
                .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Dns, err))?;
            for socket_addr in socket_addrs {
                match self
                    .try_connect_to(&socket_addr, server_addr.tls_required(), server_addr.host())
                    .await
                {
                    Ok((server_info, mut connection)) => {
                        if !self.options.ignore_discovered_servers {
                            for url in &server_info.connect_urls {
                                let server_addr = url.parse::<ServerAddr>().map_err(|err| {
                                    ConnectError::with_source(
                                        crate::ConnectErrorKind::ServerParse,
                                        err,
                                    )
                                })?;
                                self.servers.entry(server_addr).or_insert(0);
                            }
                        }

                        let server_attempts = self.servers.get_mut(&server_addr).unwrap();
                        *server_attempts = 0;

                        let tls_required = self.options.tls_required || server_addr.tls_required();
                        let mut connect_info = ConnectInfo {
                            tls_required,
                            name: self.options.name.clone(),
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
                            // We don't want to early return here,
                            // as server might require auth that we did not provide.
                            Authorization::None => {}
                            Authorization::Token(token) => {
                                connect_info.auth_token = Some(token.clone())
                            }
                            Authorization::UserAndPassword(user, pass) => {
                                connect_info.user = Some(user.clone());
                                connect_info.pass = Some(pass.clone());
                            }
                            Authorization::NKey(ref seed) => {
                                match nkeys::KeyPair::from_seed(seed.as_str()) {
                                    Ok(key_pair) => {
                                        let nonce = server_info.nonce.clone();
                                        match key_pair.sign(nonce.as_bytes()) {
                                            Ok(signed) => {
                                                connect_info.nkey = Some(key_pair.public_key());
                                                connect_info.signature =
                                                    Some(base64_url::encode(&signed));
                                            }
                                            Err(_) => {
                                                return Err(ConnectError::new(
                                                    crate::ConnectErrorKind::Authentication,
                                                ))
                                            }
                                        };
                                    }
                                    Err(_) => {
                                        return Err(ConnectError::new(
                                            crate::ConnectErrorKind::Authentication,
                                        ))
                                    }
                                }
                            }
                            Authorization::Jwt(jwt, sign_fn) => {
                                match sign_fn.call(server_info.nonce.clone()).await {
                                    Ok(sig) => {
                                        connect_info.user_jwt = Some(jwt.clone());
                                        connect_info.signature = Some(sig);
                                    }
                                    Err(_) => {
                                        return Err(ConnectError::new(
                                            crate::ConnectErrorKind::Authentication,
                                        ))
                                    }
                                }
                            }
                        }

                        connection.write_op(ClientOp::Connect(connect_info)).await?;
                        connection.write_op(ClientOp::Ping).await?;
                        connection.flush().await?;

                        match connection.read_op().await? {
                            Some(ServerOp::Error(err)) => match err {
                                ServerError::AuthorizationViolation => {
                                    return Err(ConnectError::with_source(
                                        crate::ConnectErrorKind::AuthorizationViolation,
                                        err,
                                    ));
                                }
                                err => {
                                    return Err(ConnectError::with_source(
                                        crate::ConnectErrorKind::Io,
                                        err,
                                    ));
                                }
                            },
                            Some(_) => {
                                self.events_tx.send(Event::Connected).await.ok();
                                self.state_tx.send(State::Connected).ok();
                                return Ok((server_info, connection));
                            }
                            None => {
                                return Err(ConnectError::with_source(
                                    crate::ConnectErrorKind::Io,
                                    "broken pipe",
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
    ) -> Result<(ServerInfo, Connection), ConnectError> {
        let tcp_stream = tokio::time::timeout(
            self.options.connection_timeout,
            TcpStream::connect(socket_addr),
        )
        .await
        .map_err(|_| ConnectError::new(crate::ConnectErrorKind::TimedOut))??;

        tcp_stream.set_nodelay(true)?;

        let mut connection = Connection {
            stream: Box::new(BufWriter::new(tcp_stream)),
            buffer: BytesMut::new(),
        };

        let op = connection.read_op().await?;
        let info = match op {
            Some(ServerOp::Info(info)) => info,
            Some(op) => {
                return Err(ConnectError::with_source(
                    crate::ConnectErrorKind::Io,
                    format!("expected INFO, got {:?}", op),
                ))
            }
            None => {
                return Err(ConnectError::with_source(
                    crate::ConnectErrorKind::Io,
                    "expected INFO, got nothing",
                ))
            }
        };

        if self.options.tls_required || info.tls_required || tls_required {
            let tls_config = Arc::new(
                tls::config_tls(&self.options)
                    .await
                    .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?,
            );
            let tls_connector = tokio_rustls::TlsConnector::try_from(tls_config)
                .map_err(|err| {
                    io::Error::new(
                        ErrorKind::Other,
                        format!("failed to create TLS connector from TLS config: {err}"),
                    )
                })
                .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?;

            let domain = rustls::ServerName::try_from(tls_host)
                .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?;

            connection = Connection {
                stream: Box::new(tls_connector.connect(domain, connection.stream).await?),
                buffer: BytesMut::new(),
            };
        };

        Ok((*info, connection))
    }
}
