use std::io::{self, Error, ErrorKind};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

use futures::{
    channel::mpsc,
    io::{BufReader, BufWriter},
    prelude::*,
};
use piper::Arc;
use smol::{Async, Timer};

use crate::connect::ConnectInfo;
use crate::new_client::decoder::{decode, ServerOp};
use crate::new_client::encoder::{encode, ClientOp};
use crate::Message;

/// An operation requested by the user of this crate.
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

    /// Close the connection.
    Close,
}

/// Spawns a client thread.
pub(crate) fn spawn(
    url: &str,
    user_ops: mpsc::UnboundedReceiver<UserOp>,
) -> thread::JoinHandle<io::Result<()>> {
    let url = url.to_string();

    thread::spawn(move || {
        // TODO(stjepang): Report errors from `run()` in a better place.
        dbg!(smol::run(client(&url, user_ops)))
    })
}

/// The main loop for a NATS client.
async fn client(url: &str, mut user_ops: mpsc::UnboundedReceiver<UserOp>) -> io::Result<()> {
    let stream = Arc::new(Async::<TcpStream>::connect(url).await?);

    // Bytes written to the server are buffered and periodically flushed.
    let flush_timeout = Duration::from_millis(100); // TODO(stjepang): Make this configurable.
    let mut next_flush = Instant::now() + flush_timeout;
    let mut writer = BufWriter::new(stream.clone());

    // Create an endless stream parsing operations from the server.
    let mut server_ops = stream::try_unfold(BufReader::new(stream), |mut stream| async {
        // Decode a single operation.
        let op = decode(&mut stream).await?;
        io::Result::Ok(Some((op, stream)))
    })
    .boxed();

    // Expect an INFO message.
    let mut server_info = match server_ops
        .try_next()
        .await?
        .expect("end of what should be an infinite stream")
    {
        ServerOp::Info(server_info) => server_info,
        _ => return Err(Error::new(ErrorKind::Other, "expected an INFO message")),
    };

    // Send a CONNECT operation to the server.
    encode(
        &mut writer,
        ClientOp::Connect(ConnectInfo {
            tls_required: false,
            name: None,
            pedantic: false,
            verbose: false,
            lang: crate::LANG.to_string(),
            version: crate::VERSION.to_string(),
            user: None,
            pass: None,
            auth_token: None,
            user_jwt: None,
            signature: None,
            echo: true,
        }),
    )
    .await?;

    // Current subscriptions in the form `(subject, sid, messages)`.
    let mut subscriptions: Vec<(String, usize, mpsc::UnboundedSender<Message>)> = Vec::new();

    // Handle events in a loop.
    loop {
        futures::select! {
            // An operation was received from the server.
            res = server_ops.try_next().fuse() => {
                let op = res?.expect("end of what should be an infinite stream");

                match op {
                    ServerOp::Info(new_server_info) => {
                        server_info = new_server_info;
                    }

                    ServerOp::Ping => {
                        // Send a PONG operation to the server.
                        encode(&mut writer, ClientOp::Pong).await?;
                    }

                    ServerOp::Pong => {
                        // TODO(stjepang): Do something with these pongs.
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
                        // TODO(stjepang): Display the error?
                    }

                    ServerOp::Unknown(line) => {
                        // TODO(stjepang): Log the unknown operation?
                    }
                }
            }

            // The user has enqueued an operation.
            msg = user_ops.next().fuse() => {
                match msg.expect("user_ops disconnected") {
                    UserOp::Pub { subject, reply_to, payload } => {
                        // Send a PUB operation to the server.
                        encode(&mut writer, ClientOp::Pub {
                            subject,
                            reply_to: None,
                            payload
                        })
                        .await?;
                    }

                    UserOp::Sub { subject, queue_group, sid, messages } => {
                        // Add the subscription to the list.
                        subscriptions.push((subject.clone(), sid, messages));

                        // Send a SUB operation to the server.
                        encode(&mut writer, ClientOp::Sub {
                            subject,
                            queue_group,
                            sid,
                        })
                        .await?;
                    }

                    UserOp::Unsub { sid, max_msgs } => {
                        // Remove the subscription from the list.
                        subscriptions.retain(|(_, s, _)| *s != sid);

                        // Send an UNSUB operation to the server.
                        encode(&mut writer, ClientOp::Unsub {
                            sid,
                            max_msgs,
                        })
                        .await?;
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
