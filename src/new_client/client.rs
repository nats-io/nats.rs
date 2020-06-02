use std::cmp;
use std::collections::HashMap;
use std::io::{self, Error, ErrorKind};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::pin::Pin;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use async_mutex::Mutex;
use futures::{
    channel::{mpsc, oneshot},
    io::{BufReader, BufWriter},
    prelude::*,
};
use rand::{seq::SliceRandom, thread_rng};
use smol::{Async, Timer};

use crate::new_client::decoder::{decode, ServerOp};
use crate::new_client::encoder::{encode, ClientOp};
use crate::new_client::message::Message;
use crate::new_client::options::{AuthStyle, Options};
use crate::new_client::server::Server;
use crate::new_client::writer::Writer;
use crate::secure_wipe::SecureString;
use crate::{connect::ConnectInfo, inject_delay, inject_io_failure, ServerInfo};

/// An operation enqueued by the user of this crate.
///
/// These operations are enqueued by `Connection` or `Subscription` and are then handled by the
/// client thread.
#[derive(Debug)]
pub(crate) enum UserOp {
    /// Subscribe to a subject.
    Sub {
        subject: String,
        queue_group: Option<String>,
        sid: usize,
        messages: mpsc::UnboundedSender<Message>,
    },

    /// Unsubscribe from a subject.
    Unsub { sid: usize, max_msgs: Option<u64> },
}

/// Spawns a client thread.
pub(crate) fn spawn(
    url: &str,
    options: Options,
    user_ops: mpsc::UnboundedReceiver<UserOp>,
    writer: Arc<Mutex<Writer>>,
) -> (JoinHandle<io::Result<()>>, oneshot::Sender<()>) {
    let url = url.to_string();
    let (s, r) = oneshot::channel();
    let t = thread::spawn(move || {
        // TODO(stjepang): Make panics in the client thread louder.
        smol::run(async move {
            loop {
                futures::select! {
                    res = connect_loop(&url, options, user_ops, writer).fuse() => break res,
                    _ = r.fuse() => break Ok(()),
                }
            }
        })
    });
    (t, s)
}

/// Runs the loop that connects and reconnects the client.
async fn connect_loop(
    urls: &str,
    options: Options,
    mut user_ops: mpsc::UnboundedReceiver<UserOp>,
    writer: Arc<Mutex<Writer>>,
) -> io::Result<()> {
    // Current subscriptions in the form `(subject, sid, messages)`.
    let mut subscriptions: Vec<(String, usize, mpsc::UnboundedSender<Message>)> = Vec::new();

    // Server table in the form `(server, reconnects)`.
    let mut server_reconnects: HashMap<Server, usize> = HashMap::new();

    // Populate the server table with the initial URLs.
    for url in urls.split(',') {
        server_reconnects.insert(url.parse()?, 0);
    }

    'start: loop {
        // The last seen error, which gets returned if all connect attempts fail.
        let mut last_err = Error::new(ErrorKind::AddrNotAvailable, "no socket addresses");

        // Shuffle the list of servers.
        let mut servers: Vec<Server> = server_reconnects.keys().cloned().collect();
        servers.shuffle(&mut thread_rng());

        // Iterate over the server table in random order.
        for server in &servers {
            // Calculate sleep duration for exponential backoff and bump the reconnect counter.
            let reconnects = server_reconnects.get_mut(server).unwrap();
            let sleep_duration = backoff(*reconnects);
            *reconnects += 1;

            // Resolve the server URL into socket addresses.
            let mut addrs = match (server.host.as_str(), server.port).to_socket_addrs() {
                Ok(addrs) => addrs.collect::<Vec<SocketAddr>>(),
                Err(err) => {
                    last_err = err;
                    continue;
                }
            };

            // Shuffle the resolved socket addresses.
            addrs.shuffle(&mut thread_rng());

            for addr in addrs {
                // Sleep for some time if this is not the first connection attempt for this server.
                Timer::after(sleep_duration).await;

                // Try connecting to this address.
                let res = try_connect(addr, &server, &options).await;

                // Check if connecting worked out.
                let (mut server_info, server_ops, w) = match res {
                    Ok(val) => val,
                    Err(err) => {
                        last_err = err;
                        continue;
                    }
                };

                let res = async {
                    // Inject random delays when testing.
                    inject_delay();

                    writer.lock().await.reconnect(w).await
                }
                .and_then(|()| {
                    // Connected! Now run the client loop.
                    client_loop(
                        writer.clone(),
                        server_ops,
                        &mut user_ops,
                        &mut subscriptions,
                        &mut server_info,
                    )
                })
                .await;

                // If the client stopped gracefully, all is good. Return to stop the client thread.
                if res.is_ok() {
                    return Ok(());
                }

                // Go over the latest list of server URLs and add them to the table.
                for url in server_info.connect_urls {
                    if let Ok(server) = url.parse() {
                        server_reconnects.entry(server).or_insert(0);
                    }
                }

                // Go to the beginning of the connect loop.
                continue 'start;
            }
        }

        // If this is the first connect attempt, fail fast.
        // TODO(stjepang): Find a better way to check this.
        if server_reconnects.values().sum::<usize>() == 1 {
            // All connect attempts have failed.
            return Err(last_err);
        }
    }
}

/// Calculates how long to sleep for before connecting to a server.
fn backoff(reconnects: usize) -> Duration {
    // 0ms, 1ms, 2ms, 4ms, 8ms, 16ms, ..., 4sec
    if reconnects == 0 {
        Duration::from_millis(0)
    } else {
        cmp::min(
            Duration::from_millis(2u64.saturating_pow(reconnects as u32 - 1)),
            Duration::from_secs(4),
        )
    }
}

/// Attempt to establish a connection to a single socket address.
async fn try_connect(
    addr: SocketAddr,
    server: &Server,
    options: &Options,
) -> io::Result<(
    ServerInfo,
    Pin<Box<dyn Stream<Item = io::Result<ServerOp>> + Send>>,
    BufWriter<Pin<Box<dyn AsyncWrite + Send>>>,
)> {
    // Inject random I/O failures when testing.
    inject_io_failure()?;

    // Connect to the remote socket.
    let stream = Async::<TcpStream>::connect(addr).await?;

    // Expect an INFO message.
    let mut reader = BufReader::new(stream);
    let server_info = match decode(&mut reader).await? {
        Some(ServerOp::Info(server_info)) => server_info,
        Some(op) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("expected INFO, received: {:?}", op),
            ))
        }
        None => return Err(Error::new(ErrorKind::UnexpectedEof, "connection closed")),
    };
    let stream = reader.into_inner();

    // Check if TLS authentication is required:
    // - Has `options.tls_required(true)` been set?
    // - Was the server address prefixed with `tls://`?
    // - Does the INFO line contain `tls_required: true`?
    let tls_required = options.tls_required || server.tls_required || server_info.tls_required;

    // Upgrade to TLS if required.
    let (reader, writer): (
        Pin<Box<dyn AsyncRead + Send>>,
        Pin<Box<dyn AsyncWrite + Send>>,
    ) = if tls_required {
        let conn = async move {
            // Inject random I/O failures when testing.
            inject_io_failure()?;

            // TODO(stjepang): Allow specifying a custom TLS connector.
            async_native_tls::connect(&server.host, stream)
                .await
                .map_err(|err| io::Error::new(io::ErrorKind::PermissionDenied, err))
        };

        // Connect using TLS.
        let stream = conn.await?;

        // Split the TLS stream into a reader and a writer.
        let stream = piper::Arc::new(piper::Mutex::new(stream));
        (Box::pin(stream.clone()), Box::pin(stream))
    } else {
        // Split the TCP stream into a reader and a writer.
        let stream = piper::Arc::new(stream);
        (Box::pin(stream.clone()), Box::pin(stream))
    };

    // Data that will be formatted as a CONNECT message.
    let mut connect_info = ConnectInfo {
        tls_required,
        name: options.name.clone().map(SecureString::from),
        pedantic: false,
        verbose: false,
        lang: crate::LANG.to_string(),
        version: crate::VERSION.to_string(),
        user: None,
        pass: None,
        auth_token: None,
        user_jwt: None,
        signature: None,
        echo: !options.no_echo,
    };

    // Fill in the info that authenticates the client.
    match &options.auth {
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
    }

    // Send CONNECT and INFO messages.
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, writer);
    encode(&mut writer, ClientOp::Connect(connect_info)).await?;
    encode(&mut writer, ClientOp::Ping).await?;
    writer.flush().await?;

    // Create an endless stream parsing operations from the server.
    let mut server_ops = stream::try_unfold(BufReader::new(reader), |mut stream| async {
        // Decode a single operation.
        match decode(&mut stream).await? {
            None => io::Result::Ok(None),
            Some(op) => io::Result::Ok(Some((op, stream))),
        }
    })
    .boxed();

    // Wait for a PONG.
    loop {
        match server_ops.try_next().await? {
            // If we get PONG, the server is happy and we're done connecting.
            Some(ServerOp::Pong) => break,

            // Respond to a PING with a PONG.
            Some(ServerOp::Ping) => {
                encode(&mut writer, ClientOp::Pong).await?;
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

    Ok((server_info, server_ops, writer))
}

/// Runs the main loop for a connected client.
async fn client_loop<'a>(
    writer: Arc<Mutex<Writer>>,
    mut server_ops: impl Stream<Item = io::Result<ServerOp>> + Unpin + 'a,
    user_ops: &'a mut mpsc::UnboundedReceiver<UserOp>,
    subscriptions: &'a mut Vec<(String, usize, mpsc::UnboundedSender<Message>)>,
    server_info: &'a mut ServerInfo,
) -> io::Result<()> {
    // TODO(stjepang): Make this option configurable.
    let flush_timeout = Duration::from_millis(100);

    // Bytes written to the server are buffered and periodically flushed.
    let mut next_flush = Instant::now() + flush_timeout;

    // Restart subscriptions that existed before the last reconnect.
    for (subject, sid, _messages) in subscriptions.iter() {
        // Inject random delays when testing.
        inject_delay();

        // Send a SUB operation to the server.
        encode(
            &mut *writer.lock().await,
            ClientOp::Sub {
                subject: subject.clone(),
                queue_group: None, // TODO(stjepang): Use actual queue group here.
                sid: *sid,
            },
        )
        .await?;
    }

    // Handle events in a loop.
    loop {
        futures::select! {
            // An operation was received from the server.
            res = server_ops.try_next().fuse() => {
                // Extract the next operation or return an error if the connection is closed.
                let op = match res? {
                    None => return Err(ErrorKind::ConnectionReset.into()),
                    Some(op) => op,
                };

                match op {
                    ServerOp::Info(new_server_info) => {
                        *server_info = new_server_info;
                    }

                    ServerOp::Ping => {
                        // Inject random delays when testing.
                        inject_delay();

                        // Send a PONG operation to the server.
                        encode(
                            &mut *writer.lock().await,
                            ClientOp::Pong,
                        ).await?;
                    }

                    ServerOp::Pong => {
                        // Inject random delays when testing.
                        inject_delay();

                        let mut writer = writer.lock().await;
                        writer.pong();
                    }

                    ServerOp::Msg { subject, sid, reply_to, payload } => {
                        // Send the message to matching subscriptions.
                        for (_, _, messages) in
                            subscriptions.iter().filter(|(_, s, _)| *s == sid)
                        {
                            let _ = messages.unbounded_send(Message {
                                subject: subject.clone(),
                                reply: reply_to.clone(),
                                data: payload.clone(),
                                writer: None,
                            });
                        }
                    }

                    ServerOp::Err(msg) => {
                        return Err(Error::new(ErrorKind::Other, msg));
                    }

                    ServerOp::Unknown(line) => {
                        log::warn!("unknown op: {}", line);
                    }
                }
            }

            // The user has enqueued an operation.
            msg = user_ops.next().fuse() => {
                match msg.expect("user_ops disconnected") {
                    UserOp::Sub { subject, queue_group, sid, messages } => {
                        // Add the subscription to the list.
                        subscriptions.push((subject.clone(), sid, messages));

                        // Inject random delays when testing.
                        inject_delay();

                        // Send a SUB operation to the server.
                        let mut writer = writer.lock().await;
                        encode(
                            &mut *writer,
                            ClientOp::Sub {
                                subject,
                                queue_group,
                                sid,
                            },
                        )
                        .await?;
                    }

                    UserOp::Unsub { sid, max_msgs } => {
                        // Remove the subscription from the list.
                        subscriptions.retain(|(_, s, _)| *s != sid);

                        // Inject random delays when testing.
                        inject_delay();

                        // Send an UNSUB operation to the server.
                        let mut writer = writer.lock().await;
                        encode(
                            &mut *writer,
                            ClientOp::Unsub {
                                sid,
                                max_msgs,
                            },
                        )
                        .await?;
                    }
                }
            }

            // Periodically flush writes to the server.
            _ = Timer::at(next_flush).fuse() => {
                // Inject random delays when testing.
                inject_delay();

                let mut writer = writer.lock().await;
                writer.flush().await?;
                next_flush = Instant::now() + flush_timeout;
            }
        }
    }
}
