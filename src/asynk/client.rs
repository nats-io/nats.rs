use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::io::{self, Error, ErrorKind, Write};
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use futures_channel::{mpsc, oneshot};
use smol::io::{AssertAsync, BufReader, BufWriter};
use smol::{future, prelude::*, stream, Task, Timer};

use crate::asynk::connector::Connector;
use crate::asynk::message::Message;
use crate::asynk::proto::{self, ClientOp, ServerOp};
use crate::{inject_delay, inject_io_failure, Headers, Options, ServerInfo};

/// Client state.
struct State {
    /// Buffered writer with an active connection.
    ///
    /// When `None`, the client is either reconnecting or closed.
    writer: Option<Pin<Box<dyn AsyncWrite + Send>>>,

    /// Signals to the client thread that the writer needs a flush.
    flush_kicker: mpsc::Sender<()>,

    /// The reconnect buffer.
    ///
    /// When the client is reconnecting, PUB messages get buffered here. When the connection is
    /// re-established, contents of the buffer are flushed to the server.
    buffer: Buffer,

    /// Next subscription ID.
    next_sid: u64,

    /// Current subscriptions.
    subscriptions: HashMap<u64, Subscription>,

    /// Expected pongs and their notification channels.
    pongs: VecDeque<oneshot::Sender<()>>,

    /// A channel for coordinating shutdown.
    ///
    /// When `None`, that means the client is closed or in the process of closing.
    ///
    /// To initiate shutdown, we create a channel and send its sender. The client thread receives
    /// this sender and sends `()` when it stops.
    shutdown: Option<oneshot::Sender<oneshot::Sender<()>>>,
}

/// A registered subscription.
struct Subscription {
    subject: String,
    queue_group: Option<String>,
    messages: async_channel::Sender<Message>,
}

/// A NATS client.
#[derive(Clone)]
pub(crate) struct Client {
    /// Shared client state.
    state: Arc<async_mutex::Mutex<State>>,

    /// Server info provided by the last INFO message.
    server_info: Arc<Mutex<Option<ServerInfo>>>,

    /// Subscriptions that have logically unsubscribed but haven't sent UNSUB yet.
    unsubscribed: Arc<Mutex<HashSet<u64>>>,
}

impl Client {
    /// Creates a new client that will begin connecting in the background.
    pub(crate) async fn connect(url: &str, options: Options) -> io::Result<Client> {
        // A channel for coordinating shutdown.
        let (shutdown, stop) = oneshot::channel();

        // A channel for coordinating flushes.
        let (flush_kicker, mut dirty) = mpsc::channel(1);

        // Channels for coordinating initial connect.
        let (run_sender, run_receiver) = oneshot::channel();
        let (pong_sender, pong_receiver) = oneshot::channel();

        // The client state.
        let client = Client {
            state: Arc::new(async_mutex::Mutex::new(State {
                writer: None,
                flush_kicker,
                buffer: Buffer::new(options.reconnect_buffer_size),
                next_sid: 1,
                subscriptions: HashMap::new(),
                pongs: VecDeque::from(vec![pong_sender]),
                shutdown: Some(shutdown),
            })),
            server_info: Arc::new(Mutex::new(None)),
            unsubscribed: Arc::new(Mutex::new(HashSet::new())),
        };

        // Connector for creating the initial connection and reconnecting when it is broken.
        let connector = Connector::new(url, options)?;

        // Spawn the client thread responsible for:
        // - Maintaining a connection to the server and reconnecting when it is broken.
        // - Reading messages from the server and processing them.
        // - Forwarding MSG operations to subscribers.
        thread::spawn({
            let client = client.clone();
            move || {
                smol::run(async move {
                    // Spawn a task that periodically flushes buffered messages.
                    let flusher = Task::local({
                        let client = client.clone();
                        async move {
                            // Wait until at least one message is buffered.
                            while dirty.next().await.is_some() {
                                {
                                    // Flush the writer.
                                    let mut state = client.state.lock().await;
                                    if let Some(writer) = state.writer.as_mut() {
                                        let _ = writer.flush().await;
                                    }
                                }
                                // Wait a little bit before flushing again.
                                Timer::new(Duration::from_millis(1)).await;
                            }
                        }
                    });

                    // Spawn the main task that processes messages from the server.
                    let runner = Task::local(async move {
                        let res = client.run(connector).await;
                        let _ = run_sender.send(res);
                    });

                    // Wait until the client is closed.
                    let res = stop.await;

                    // Cancel spawned tasks.
                    flusher.cancel().await;
                    runner.cancel().await;

                    // Signal to the shutdown initiator that it is now complete.
                    if let Ok(s) = res {
                        let _ = s.send(());
                    }
                })
            }
        });

        future::race(
            async {
                // Wait for `run()` to error.
                run_receiver.await.expect("client has panicked")?;
                panic!("client has stopped unexpectedly");
            },
            async {
                // Wait for the connection to get established.
                let _ = pong_receiver.await;
                Ok(client)
            },
        )
        .await
    }

    /// Retrieves server info as received by the most recent connection.
    pub(crate) fn server_info(&self) -> Option<ServerInfo> {
        self.server_info.lock().unwrap().clone()
    }

    /// Makes a round trip to the server to ensure buffered messages reach it.
    pub(crate) async fn flush(&self) -> io::Result<()> {
        let pong = {
            // Inject random delays when testing.
            inject_delay().await;

            let mut state = self.state.lock().await;

            // Check if the client is closed.
            if state.shutdown.is_none() {
                return Err(Error::new(ErrorKind::NotConnected, "the client is closed"));
            }

            let (sender, receiver) = oneshot::channel();

            // If connected, send a PING.
            match state.writer.as_mut() {
                None => {}
                Some(mut writer) => {
                    proto::encode(&mut writer, ClientOp::Ping).await?;
                    writer.flush().await?;
                }
            }

            // Enqueue an expected PONG.
            state.pongs.push_back(sender);
            receiver
        };

        // Wait until the PONG operation is received.
        match pong.await {
            Ok(()) => Ok(()),
            Err(_) => Err(Error::new(ErrorKind::ConnectionReset, "flush failed")),
        }
    }

    /// Closes the client.
    pub(crate) async fn close(&self) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay().await;

        let mut state = self.state.lock().await;

        // Initiate shutdown process.
        if let Some(shutdown) = state.shutdown.take() {
            // Unsubscribe all subscriptions.
            for sid in state.subscriptions.keys() {
                self.unsubscribe(*sid);
            }
            self.cleanup_subscriptions(&mut state).await;

            // Flush the writer in case there are buffered messages.
            if let Some(writer) = state.writer.as_mut() {
                let _ = writer.flush().await;
            }

            // Wake up all pending flushes.
            state.pongs.clear();
            drop(state);

            let (s, r) = oneshot::channel();
            // Signal the thread to stop.
            let _ = shutdown.send(s);
            // Wait for the thread to stop.
            let _ = r.await;
        }

        Ok(())
    }

    /// Subscribes to a subject.
    pub(crate) async fn subscribe(
        &self,
        subject: &str,
        queue_group: Option<&str>,
    ) -> io::Result<(u64, async_channel::Receiver<Message>)> {
        // Inject random delays when testing.
        inject_delay().await;

        let mut state = self.state.lock().await;

        // Check if the client is closed.
        if state.shutdown.is_none() {
            return Err(Error::new(ErrorKind::NotConnected, "the client is closed"));
        }

        // Clean up dead subscriptions.
        self.cleanup_subscriptions(&mut state).await;

        // Generate a subject ID.
        let sid = state.next_sid;
        state.next_sid += 1;

        // If connected, send a SUB operation.
        if let Some(writer) = state.writer.as_mut() {
            let op = ClientOp::Sub {
                subject,
                queue_group,
                sid,
            };
            let _ = proto::encode(writer, op).await;
            let _ = state.flush_kicker.try_send(());
        }

        // Register the subscription in the hash map.
        let (sender, receiver) = async_channel::unbounded();
        state.subscriptions.insert(
            sid,
            Subscription {
                subject: subject.to_string(),
                queue_group: queue_group.map(ToString::to_string),
                messages: sender,
            },
        );
        Ok((sid, receiver))
    }

    /// Unsubscribes from a subject.
    pub(crate) fn unsubscribe(&self, sid: u64) {
        self.unsubscribed.lock().unwrap().insert(sid);
    }

    /// Publishes a message with optional reply subject and headers.
    pub(crate) async fn publish(
        &self,
        subject: &str,
        reply_to: Option<&str>,
        headers: Option<&Headers>,
        msg: &[u8],
    ) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay().await;

        let mut state = self.state.lock().await;

        // Check if the client is closed.
        if state.shutdown.is_none() {
            return Err(Error::new(ErrorKind::NotConnected, "the client is closed"));
        }

        let op = if let Some(headers) = headers {
            ClientOp::Hpub {
                subject,
                reply_to,
                payload: msg,
                headers,
            }
        } else {
            ClientOp::Pub {
                subject,
                reply_to,
                payload: msg,
            }
        };

        match state.writer.as_mut() {
            None => {
                // If reconnecting, write into the buffer.
                proto::encode(AssertAsync::new(&mut state.buffer), op).await?;
                state.buffer.flush()?;
                Ok(())
            }
            Some(mut writer) => {
                // If connected, write into the writer.
                let res = proto::encode(&mut writer, op).await;
                let _ = state.flush_kicker.try_send(());

                // If writing fails, disconnect.
                if res.is_err() {
                    state.writer = None;
                    state.pongs.clear();
                }
                res
            }
        }
    }

    /// Runs the loop that connects and reconnects the client.
    async fn run(&self, mut connector: Connector) -> io::Result<()> {
        // Don't use backoff on first connect.
        let mut use_backoff = false;

        loop {
            // Make a connection to the server.
            let (server_info, reader, writer) = connector.connect(use_backoff).await?;
            use_backoff = true;

            let reader = BufReader::with_capacity(128 * 1024, reader);
            let writer = BufWriter::with_capacity(128 * 1024, writer);

            // Create an endless stream parsing operations from the server.
            let server_ops = stream::try_unfold(reader, |mut stream| async {
                // Decode a single operation.
                match proto::decode(&mut stream).await? {
                    None => io::Result::Ok(None),
                    Some(op) => io::Result::Ok(Some((op, stream))),
                }
            })
            .boxed();

            // Set up the new connection for this client.
            if self.reconnect(server_info, writer).await.is_ok() {
                // Connected! Now dispatch MSG operations.
                if self.dispatch(server_ops, &mut connector).await.is_ok() {
                    // If the client stopped gracefully, return.
                    return Ok(());
                }
            }

            // Inject random delays when testing.
            inject_delay().await;

            let state = self.state.lock().await;

            // Check if the client is closed.
            if state.shutdown.is_none() {
                return Ok(());
            }
        }
    }

    /// Puts the client back into connected state with the given writer.
    async fn reconnect(
        &self,
        server_info: ServerInfo,
        writer: impl AsyncWrite + Send + 'static,
    ) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay().await;

        let mut state = self.state.lock().await;

        // Check if the client is closed.
        if state.shutdown.is_none() {
            return Err(Error::new(ErrorKind::NotConnected, "the client is closed"));
        }

        // Drop the current writer, if there is one.
        state.writer = None;

        // Pin the new writer on the heap.
        let mut writer = Box::pin(writer);

        // Inject random I/O failures when testing.
        inject_io_failure()?;

        // Clean up dead subscriptions.
        self.cleanup_subscriptions(&mut state).await;

        // Restart subscriptions that existed before the last reconnect.
        for (sid, subscription) in &state.subscriptions {
            // Send a SUB operation to the server.
            proto::encode(
                &mut writer,
                ClientOp::Sub {
                    subject: subscription.subject.as_str(),
                    queue_group: subscription.queue_group.as_deref(),
                    sid: *sid,
                },
            )
            .await?;
        }

        // Take out expected PONGs.
        let pongs = mem::replace(&mut state.pongs, VecDeque::new());

        // Take out buffered operations.
        let buffered = state.buffer.clear();

        // Write buffered PUB operations into the new writer.
        writer.write_all(buffered).await?;
        writer.flush().await?;

        // All good, continue with this connection.
        *self.server_info.lock().unwrap() = Some(server_info);
        state.writer = Some(writer);

        // Complete PONGs because the connection is healthy.
        for p in pongs {
            let _ = p.send(());
        }

        Ok(())
    }

    /// Reads messages from the server and dispatches them to subscribers.
    async fn dispatch(
        &self,
        mut server_ops: impl Stream<Item = io::Result<ServerOp>> + Unpin,
        connector: &mut Connector,
    ) -> io::Result<()> {
        // Handle operations received from the server.
        while let Some(op) = server_ops.next().await {
            let op = op?;

            // Inject random delays when testing.
            inject_delay().await;

            let mut state = self.state.lock().await;

            match op {
                ServerOp::Info(server_info) => {
                    for url in &server_info.connect_urls {
                        let _ = connector.add_url(url);
                    }
                    *self.server_info.lock().unwrap() = Some(server_info);
                }

                ServerOp::Ping => {
                    // Respond with a PONG if connected.
                    if let Some(w) = state.writer.as_mut() {
                        proto::encode(w, ClientOp::Pong).await?;
                        let _ = state.flush_kicker.try_send(());
                    }
                }

                ServerOp::Pong => {
                    // If a PONG is received while disconnected, it came from a connection that isn't
                    // alive anymore and therefore doesn't correspond to the next expected PONG.
                    if state.writer.is_some() {
                        // Take the next expected PONG and complete it by sending a message.
                        if let Some(pong) = state.pongs.pop_front() {
                            let _ = pong.send(());
                        }
                    }
                }

                ServerOp::Msg {
                    subject,
                    sid,
                    reply_to,
                    payload,
                } => {
                    // Send the message to matching subscription.
                    if let Some(subscription) = state.subscriptions.get(&sid) {
                        let msg = Message {
                            subject,
                            reply: reply_to,
                            data: payload,
                            headers: None,
                            client: self.clone(),
                        };

                        // Send a message or drop it if the channel is disconnected or full.
                        let _ = subscription.messages.try_send(msg);
                    } else {
                        // If there is no matching subscription, clean up.
                        self.cleanup_subscriptions(&mut state).await;
                    }
                }

                ServerOp::Hmsg {
                    subject,
                    headers,
                    sid,
                    reply_to,
                    payload,
                } => {
                    // Send the message to matching subscription.
                    if let Some(subscription) = state.subscriptions.get(&sid) {
                        let msg = Message {
                            subject,
                            reply: reply_to,
                            data: payload,
                            headers: Some(headers),
                            client: self.clone(),
                        };

                        // Send a message or drop it if the channel is disconnected or full.
                        let _ = subscription.messages.try_send(msg);
                    } else {
                        // If there is no matching subscription, clean up.
                        self.cleanup_subscriptions(&mut state).await;
                    }
                }

                ServerOp::Err(msg) => return Err(Error::new(ErrorKind::Other, msg)),

                ServerOp::Unknown(line) => log::warn!("unknown op: {}", line),
            }
        }

        // The stream of operation is broken, meaning the connection was lost.
        Err(ErrorKind::ConnectionReset.into())
    }

    /// Sends UNSUB for dead subscriptions.
    async fn cleanup_subscriptions(&self, state: &mut async_mutex::MutexGuard<'_, State>) {
        // Keep unsubscribed list in separate variable so it won't be captured by for loop context.
        let unsubscribed = mem::replace(&mut *self.unsubscribed.lock().unwrap(), HashSet::new());
        
        for sid in unsubscribed {
            // Remove the subscription from the map.
            state.subscriptions.remove(&sid);

            // Send an UNSUB message and ignore errors.
            if let Some(writer) = state.writer.as_mut() {
                let max_msgs = None;
                let _ = proto::encode(writer, ClientOp::Unsub { sid, max_msgs }).await;
                let _ = state.flush_kicker.try_send(());
            }
        }
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("Client").finish()
    }
}

/// Reconnect buffer.
///
/// If the connection was broken and the client is currently reconnecting, PUB messages get stored
/// in this buffer of limited size. As soon as the connection is then re-established, buffered
/// messages will be sent to the server.
struct Buffer {
    /// Bytes in the buffer.
    ///
    /// There are three interesting ranges in this slice:
    ///
    /// - `..flushed` contains buffered PUB messages.
    /// - `flushed..written` contains a partial PUB message at the end.
    /// - `written..` is empty space in the buffer.
    bytes: Box<[u8]>,

    /// Number of written bytes.
    written: usize,

    /// Number of bytes marked as "flushed".
    flushed: usize,
}

impl Buffer {
    /// Creates a new buffer with the given size.
    fn new(size: usize) -> Buffer {
        Buffer {
            bytes: vec![0_u8; size].into_boxed_slice(),
            written: 0,
            flushed: 0,
        }
    }

    /// Clears the buffer and returns buffered bytes.
    fn clear(&mut self) -> &[u8] {
        let buffered = &self.bytes[..self.flushed];
        self.written = 0;
        self.flushed = 0;
        buffered
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = buf.len();

        // Check if `buf` will fit into this `Buffer`.
        if self.bytes.len() - self.written < n {
            // Fill the buffer to prevent subsequent smaller writes.
            self.written = self.bytes.len();

            Err(Error::new(
                ErrorKind::Other,
                "the disconnect buffer is full",
            ))
        } else {
            // Append `buf` into the buffer.
            let range = self.written..self.written + n;
            self.bytes[range].copy_from_slice(&buf[..n]);
            self.written += n;
            Ok(n)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushed = self.written;
        Ok(())
    }
}
