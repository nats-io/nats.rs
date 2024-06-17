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
use crate::connection::Connection;
use crate::connection::State;
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
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::engine::Engine;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::cmp;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_rustls::rustls;

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
    pub(crate) reconnect_delay_callback: Box<dyn Fn(usize) -> Duration + Send + Sync + 'static>,
    pub(crate) auth_callback: Option<CallbackArg1<Vec<u8>, Result<Auth, AuthError>>>,
    pub(crate) max_reconnects: Option<usize>,
}

/// Maintains a list of servers and establishes connections.
pub(crate) struct Connector {
    /// A map of servers and number of connect attempts.
    servers: Vec<(ServerAddr, usize)>,
    options: ConnectorOptions,
    attempts: usize,
    pub(crate) events_tx: tokio::sync::mpsc::Sender<Event>,
    pub(crate) state_tx: tokio::sync::watch::Sender<State>,
    pub(crate) max_payload: Arc<AtomicUsize>,
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
    ) -> Result<Connector, io::Error> {
        let servers = addrs.to_server_addrs()?.map(|addr| (addr, 0)).collect();

        Ok(Connector {
            attempts: 0,
            servers,
            options,
            events_tx,
            state_tx,
            max_payload,
        })
    }

    pub(crate) async fn connect(&mut self) -> Result<(ServerInfo, Connection), ConnectError> {
        loop {
            match self.try_connect().await {
                Ok(inner) => return Ok(inner),
                Err(error) => match error.kind() {
                    ConnectErrorKind::MaxReconnects => {
                        return Err(ConnectError::with_source(
                            crate::ConnectErrorKind::MaxReconnects,
                            error,
                        ))
                    }
                    other => {
                        self.events_tx
                            .send(Event::ClientError(ClientError::Other(other.to_string())))
                            .await
                            .ok();
                    }
                },
            }
        }
    }

    pub(crate) async fn try_connect(&mut self) -> Result<(ServerInfo, Connection), ConnectError> {
        tracing::debug!("connecting");
        let mut error = None;

        let mut servers = self.servers.clone();
        if !self.options.retain_servers_order {
            servers.shuffle(&mut thread_rng());
            // sort_by is stable, meaning it will retain the order for equal elements.
            servers.sort_by(|a, b| a.1.cmp(&b.1));
        }

        for (server_addr, _) in servers {
            self.attempts += 1;
            if let Some(max_reconnects) = self.options.max_reconnects {
                if self.attempts > max_reconnects {
                    self.events_tx
                        .send(Event::ClientError(ClientError::MaxReconnects))
                        .await
                        .ok();
                    return Err(ConnectError::new(crate::ConnectErrorKind::MaxReconnects));
                }
            }

            let duration = (self.options.reconnect_delay_callback)(self.attempts);

            sleep(duration).await;

            let socket_addrs = server_addr
                .socket_addrs()
                .await
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
                                if !self.servers.iter().any(|(addr, _)| addr == &server_addr) {
                                    self.servers.push((server_addr, 0));
                                }
                            }
                        }

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

                        if let Some(nkey) = self.options.auth.nkey.as_ref() {
                            match nkeys::KeyPair::from_seed(nkey.as_str()) {
                                Ok(key_pair) => {
                                    let nonce = server_info.nonce.clone();
                                    match key_pair.sign(nonce.as_bytes()) {
                                        Ok(signed) => {
                                            connect_info.nkey = Some(key_pair.public_key());
                                            connect_info.signature =
                                                Some(URL_SAFE_NO_PAD.encode(signed));
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

                        if let Some(jwt) = self.options.auth.jwt.as_ref() {
                            if let Some(sign_fn) = self.options.auth.signature_callback.as_ref() {
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

                        if let Some(callback) = self.options.auth_callback.as_ref() {
                            let auth = callback
                                .call(server_info.nonce.as_bytes().to_vec())
                                .await
                                .map_err(|err| {
                                ConnectError::with_source(
                                    crate::ConnectErrorKind::Authentication,
                                    err,
                                )
                            })?;
                            connect_info.user = auth.username;
                            connect_info.pass = auth.password;
                            connect_info.user_jwt = auth.jwt;
                            connect_info.signature = auth
                                .signature
                                .map(|signature| URL_SAFE_NO_PAD.encode(signature));
                            connect_info.auth_token = auth.token;
                            connect_info.nkey = auth.nkey;
                        }

                        connection
                            .easy_write_and_flush(
                                [ClientOp::Connect(connect_info), ClientOp::Ping].iter(),
                            )
                            .await?;

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
                                tracing::debug!("connected to {}", server_info.port);
                                self.attempts = 0;
                                self.events_tx.send(Event::Connected).await.ok();
                                self.state_tx.send(State::Connected).ok();
                                self.max_payload.store(
                                    server_info.max_payload,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
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

        let mut connection = Connection::new(
            Box::new(tcp_stream),
            self.options.read_buffer_capacity.into(),
        );

        let tls_connection = |connection: Connection| async {
            let tls_config = Arc::new(
                tls::config_tls(&self.options)
                    .await
                    .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?,
            );
            let tls_connector = tokio_rustls::TlsConnector::from(tls_config);

            let domain = webpki::types::ServerName::try_from(tls_host)
                .map_err(|err| ConnectError::with_source(crate::ConnectErrorKind::Tls, err))?;

            let tls_stream = tls_connector
                .connect(domain.to_owned(), connection.stream)
                .await?;

            Ok::<Connection, ConnectError>(Connection::new(Box::new(tls_stream), 0))
        };

        // If `tls_first` was set, establish TLS connection before getting INFO.
        // There is no point in  checking if tls is required, because
        // the connection has to be be upgraded to TLS anyway as it's different flow.
        if self.options.tls_first {
            connection = tls_connection(connection).await?;
        }

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

        // If `tls_first` was not set, establish TLS connection if it is required.
        if !self.options.tls_first
            && (self.options.tls_required || info.tls_required || tls_required)
        {
            connection = tls_connection(connection).await?;
        };

        Ok((*info, connection))
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
