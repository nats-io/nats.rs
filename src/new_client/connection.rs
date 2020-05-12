use std::io::{self, Error, ErrorKind};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use crate::new_client::subscription::{Subscription, SubscriptionOp};
use crate::Message;

/// A NATS client connection.
pub struct Connection {
    /// Enqueues PUB operations.
    ///
    /// Items are of the form `(subject, message)`.
    pub_ops: mpsc::UnboundedSender<(String, Vec<u8>)>,

    /// Enqueues SUB and UNSUB operations.
    sub_ops: mpsc::UnboundedSender<SubscriptionOp>,

    /// Subscription ID generator.
    sid_gen: AtomicUsize,

    /// Thread running the main future.
    thread: thread::JoinHandle<io::Result<()>>,
}

impl Connection {
    /// Connects a NATS client.
    pub fn connect(url: &str) -> io::Result<Connection> {
        let url = url.to_string();
        let (pub_sender, pub_receiver) = mpsc::unbounded();
        let (sub_sender, sub_receiver) = mpsc::unbounded();

        Ok(Connection {
            thread: thread::spawn(move || {
                // TODO(stjepang): Report errors from `run()` in a better place.
                dbg!(smol::run(client(&url, pub_receiver, sub_receiver)))
            }),
            sub_ops: sub_sender,
            pub_ops: pub_sender,
            sid_gen: AtomicUsize::new(1),
        })
    }

    /// Publishes a message.
    pub fn publish(&mut self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        let subject = subject.to_string();
        let msg = msg.as_ref().to_vec();

        // Enqueue a PUB operation.
        self.pub_ops
            .send((subject, msg))
            .now_or_never()
            .expect("future can't be pending because the publish channel is unbounded")
            .expect("the publish channel shouldn't be disconnected");

        Ok(())
    }

    /// Creates a new subscriber.
    pub fn subscribe(&mut self, subject: &str) -> Subscription {
        let sid = self.sid_gen.fetch_add(1, Ordering::SeqCst);
        Subscription::new(subject, sid, self.sub_ops.clone())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // TODO(stjepang): Wait for `self.thread` to stop.
    }
}

/// The main loop for a NATS client.
async fn client(
    url: &str,
    mut pub_ops: mpsc::UnboundedReceiver<(String, Vec<u8>)>,
    mut sub_ops: mpsc::UnboundedReceiver<SubscriptionOp>,
) -> io::Result<()> {
    let stream = Arc::new(Async::<TcpStream>::connect(url).await?);

    // Bytes written to the server are buffered and periodically flushed.
    let flush_timeout = Duration::from_millis(100); // TODO(stjepang): Make this configurable.
    let mut next_flush = Instant::now() + flush_timeout;
    let mut writer = BufWriter::new(stream.clone());

    // Create an endless stream parsing operations from the server.
    let mut reader = stream::try_unfold(BufReader::new(stream), |mut stream| async {
        // Decode a single operation.
        let op = decode(&mut stream).await?;
        io::Result::Ok(Some((op, stream)))
    })
    .boxed();

    // Expect an INFO message.
    let server_info = match reader
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
            res = reader.try_next().fuse() => {
                let op = res?.expect("end of what should be an infinite stream");

                match op {
                    ServerOp::Info(server_info) => {
                        // TODO(stjepang): Do something with this info.
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

            // A PUB operation was enqueued.
            msg = pub_ops.next().fuse() => {
                let (subject, payload) = msg.expect("disconnected");

                // Send a PUB operation to the server.
                encode(&mut writer, ClientOp::Pub {
                    subject,
                    reply_to: None,
                    payload
                })
                .await?;
            }

            // A SUB or UNSUB operation was enqueued.
            msg = sub_ops.next().fuse() => {
                match msg.expect("disconnected") {
                    SubscriptionOp::Sub { subject, queue_group, sid, messages } => {
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

                    SubscriptionOp::Unsub { sid, max_msgs } => {
                        // Remove the subscription from the list.
                        subscriptions.retain(|(_, s, _)| *s != sid);

                        // Send an UNSUB operation to the server.
                        encode(&mut writer, ClientOp::Unsub {
                            sid,
                            max_msgs,
                        })
                        .await?;
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
