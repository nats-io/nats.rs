use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Error, ErrorKind};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::pin::Pin;
use std::thread;
use std::time::{Duration, Instant};

use futures::{
    channel::{mpsc, oneshot},
    io::{BufReader, BufWriter},
    prelude::*,
};
use piper::{Arc, Mutex};
use rand::{seq::SliceRandom, thread_rng};
use smol::{Async, Timer};

use crate::new_client::decoder::{decode, ServerOp};
use crate::new_client::encoder::{encode, ClientOp};
use crate::new_client::options::{AuthStyle, Options};
use crate::new_client::server::Server;
use crate::secure_wipe::SecureString;
use crate::{connect::ConnectInfo, inject_io_failure, Message, ServerInfo};

/// An operation enqueued by the user of this crate.
///
/// These operations are enqueued by `Connection` or `Subscription` and are then handled by the
/// client thread.
pub(crate) enum UserOp {
    /// Publish a message.
    Pub {
        subject: String,
        reply_to: Option<String>,
        payload: Vec<u8>,
    },

    /// Subscribe to a subject.
    Sub {
        subject: String,
        queue_group: Option<String>,
        sid: usize,
        messages: mpsc::UnboundedSender<Message>,
    },

    /// Unsubscribe from a subject.
    Unsub { sid: usize, max_msgs: Option<u64> },

    /// Send a ping and wait for a pong.
    Ping { pong: oneshot::Sender<()> },

    /// Close the connection.
    Close,
}

/// Spawns a client thread.
pub(crate) fn spawn(
    url: &str,
    options: Options,
    user_ops: mpsc::UnboundedReceiver<UserOp>,
) -> thread::JoinHandle<io::Result<()>> {
    let url = url.to_string();
    thread::spawn(move || smol::run(connect_loop(&url, options, user_ops)))
}

/// Runs the loop that connects and reconnects the client.
async fn connect_loop(
    urls: &str,
    options: Options,
    mut user_ops: mpsc::UnboundedReceiver<UserOp>,
) -> io::Result<()> {
    // Current subscriptions in the form `(subject, sid, messages)`.
    let mut subscriptions: Vec<(String, usize, mpsc::UnboundedSender<Message>)> = Vec::new();

    // Expected pongs and their notification channels.
    let mut pongs: VecDeque<oneshot::Sender<()>> = VecDeque::new();

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
                let (mut server_info, server_ops, writer) = match res {
                    Ok(val) => val,
                    Err(err) => {
                        last_err = err;
                        continue;
                    }
                };

                // Connected! Now run the client loop.
                let res = client_loop(
                    writer,
                    server_ops,
                    &mut user_ops,
                    &mut subscriptions,
                    &mut pongs,
                    &mut server_info,
                )
                .await;

                // If the client stopped gracefully, all is good. Return to stop the client thread.
                if res.is_ok() {
                    return Ok(());
                }

                // Clear the pending PONG list, dropping all senders and thus failing operations
                // waiting on PONGs.
                pongs.clear();

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

        // All connect attempts have failed.
        return Err(last_err);
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
    Pin<Box<dyn AsyncWrite + Send>>,
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
        // TODO(stjepang): Allow specifying a custom TLS connector.
        match async_native_tls::connect(&server.host, stream).await {
            Ok(stream) => {
                // Split the TLS stream into a reader and a writer.
                let stream = Arc::new(Mutex::new(stream));
                (Box::pin(stream.clone()), Box::pin(stream))
            }
            Err(err) => return Err(io::Error::new(io::ErrorKind::PermissionDenied, err)),
        }
    } else {
        // Split the TCP stream into a reader and a writer.
        let stream = Arc::new(stream);
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
    let mut writer = BufWriter::new(writer);
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

    Ok((server_info, server_ops, writer.into_inner()))
}

/// Runs the main loop for a connected client.
async fn client_loop(
    stream: impl AsyncWrite + Unpin,
    mut server_ops: impl Stream<Item = io::Result<ServerOp>> + Unpin,
    user_ops: &mut mpsc::UnboundedReceiver<UserOp>,
    subscriptions: &mut Vec<(String, usize, mpsc::UnboundedSender<Message>)>,
    pongs: &mut VecDeque<oneshot::Sender<()>>,
    server_info: &mut ServerInfo,
) -> io::Result<()> {
    // TODO(stjepang): Make this option configurable.
    let flush_timeout = Duration::from_millis(100);

    // Bytes written to the server are buffered and periodically flushed.
    let mut next_flush = Instant::now() + flush_timeout;
    let mut writer = BufWriter::new(stream);

    // Restart subscriptions that existed before the last reconnect.
    for (subject, sid, _messages) in subscriptions.iter() {
        // Send a SUB operation to the server.
        encode(
            &mut writer,
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
                        // Send a PONG operation to the server.
                        encode(&mut writer, ClientOp::Pong).await?;
                    }

                    ServerOp::Pong => {
                        // Take the next expected pong from the queue.
                        let pong = pongs.pop_front().expect("unexpected pong");

                        // Complete the pong by sending a message into the channel.
                        let _ = pong.send(());
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
                                responder: None,
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
                    UserOp::Pub { subject, reply_to, payload } => {
                        // Send a PUB operation to the server.
                        encode(
                            &mut writer,
                            ClientOp::Pub {
                                subject,
                                reply_to: None,
                                payload
                            },
                        )
                        .await?;
                    }

                    UserOp::Sub { subject, queue_group, sid, messages } => {
                        // Add the subscription to the list.
                        subscriptions.push((subject.clone(), sid, messages));

                        // Send a SUB operation to the server.
                        encode(
                            &mut writer,
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

                        // Send an UNSUB operation to the server.
                        encode(
                            &mut writer,
                            ClientOp::Unsub {
                                sid,
                                max_msgs,
                            },
                        )
                        .await?;
                    }

                    UserOp::Ping { pong } => {
                        // Send a PING operation to the server.
                        encode(&mut writer, ClientOp::Ping).await?;

                        // Flush to make sure the PING goes through.
                        writer.flush().await?;

                        // Record that we're expecting a PONG.
                        pongs.push_back(pong);
                    }

                    UserOp::Close => {
                        // TODO(stjepang): Perhaps we should flush before closing abruptly.
                        return Ok(());
                    }
                }
            }

            // Periodically flush writes to the server.
            _ = Timer::at(next_flush).fuse() => {
                writer.flush().await?;
                next_flush = Instant::now() + flush_timeout;
            }
        }
    }
}
