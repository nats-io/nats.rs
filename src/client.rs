use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter, Error, ErrorKind};
use std::mem;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel as channel;
use parking_lot::Mutex;

use crate::connector::{Connector, NatsStream};
use crate::message::Message;
use crate::proto::{self, ClientOp, ServerOp};
use crate::{inject_delay, inject_io_failure, Headers, Options, ServerInfo};

/// Client state.
struct State {
    /// Buffered writer with an active connection.
    ///
    /// When `None`, the client is either reconnecting or closed.
    writer: Option<BufWriter<NatsStream>>,

    /// Signals to the client thread that the writer needs a flush.
    flush_kicker: channel::Sender<()>,

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
    pongs: VecDeque<channel::Sender<()>>,
}

/// A registered subscription.
struct Subscription {
    subject: String,
    queue_group: Option<String>,
    messages: channel::Sender<Message>,
}

/// A NATS client.
///
#[derive(Clone)]
pub struct Client {
    /// Shared client state.
    state: Arc<Mutex<State>>,

    /// Server info provided by the last INFO message.
    server_info: Arc<Mutex<Option<ServerInfo>>>,

    /// Set to `true` if shutdown has been requested.
    shutdown: Arc<Mutex<bool>>,
}

impl Client {
    /// Creates a new client that will begin connecting in the background.
    pub(crate) fn connect(url: &str, options: Options) -> io::Result<Client> {
        // A channel for coordinating flushes.
        let (flush_kicker, dirty) = channel::bounded(1);

        // Channels for coordinating initial connect.
        let (run_sender, run_receiver) = channel::bounded(1);
        let (pong_sender, pong_receiver) = channel::bounded::<()>(1);

        // The client state.
        let client = Client {
            state: Arc::new(Mutex::new(State {
                writer: None,
                flush_kicker,
                buffer: Buffer::new(options.reconnect_buffer_size),
                next_sid: 1,
                subscriptions: HashMap::new(),
                pongs: VecDeque::from(vec![pong_sender]),
            })),
            server_info: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(Mutex::new(false)),
        };

        let options = Arc::new(options);
        // Connector for creating the initial connection and reconnecting when it is broken.
        let connector = Connector::new(url, options.clone())?;

        // Spawn the client thread responsible for:
        // - Maintaining a connection to the server and reconnecting when it is broken.
        // - Reading messages from the server and processing them.
        // - Forwarding MSG operations to subscribers.
        thread::spawn({
            let client = client.clone();
            move || {
                let res = client.run(connector);
                run_sender.send(res).ok();

                // One final flush before shutting down.
                // This way we make sure buffered published messages reach the server.
                {
                    let mut state = client.state.lock();
                    if let Some(writer) = state.writer.as_mut() {
                        writer.flush().ok();
                    }
                }

                options.close_callback.call();
            }
        });

        channel::select! {
            recv(run_receiver) -> res => {
                res.expect("client thread has panicked")?;
                unreachable!()
            }
            recv(pong_receiver) -> _ => {}
        }

        // Spawn a thread that periodically flushes buffered messages.
        thread::spawn({
            let client = client.clone();
            move || {
                // Wait until at least one message is buffered.
                while dirty.recv().is_ok() {
                    let start = Instant::now();
                    {
                        // Flush the writer.
                        let mut state = client.state.lock();
                        if let Some(writer) = state.writer.as_mut() {
                            let res = writer.flush();

                            // If flushing fails, disconnect.
                            if res.is_err() {
                                state.writer = None;
                                state.pongs.clear();
                            }
                        }
                    }
                    // Wait a little bit before flushing again.
                    thread::sleep(start.elapsed() * 9);
                }
            }
        });

        Ok(client)
    }

    /// Retrieves server info as received by the most recent connection.
    pub(crate) fn server_info(&self) -> Option<ServerInfo> {
        self.server_info.lock().clone()
    }

    /// Makes a round trip to the server to ensure buffered messages reach it.
    pub(crate) fn flush(&self, timeout: Duration) -> io::Result<()> {
        let pong = {
            // Inject random delays when testing.
            inject_delay();

            let mut state = self.state.lock();

            // Check if the client is closed.
            if *self.shutdown.lock() {
                return Err(Error::new(ErrorKind::NotConnected, "the client is closed"));
            }

            let (sender, receiver) = channel::bounded(1);

            // If connected, send a PING.
            match state.writer.as_mut() {
                None => {}
                Some(mut writer) => {
                    // TODO(stjepang): We probably want to set the deadline rather than the timeout
                    // because right now the timeout applies to each write syscall individually.
                    writer.get_ref().set_write_timeout(Some(timeout))?;
                    proto::encode(&mut writer, ClientOp::Ping)?;
                    writer.flush()?;
                    writer.get_ref().set_write_timeout(None)?;
                }
            }

            // Enqueue an expected PONG.
            state.pongs.push_back(sender);
            receiver
        };

        // Wait until the PONG operation is received.
        match pong.recv() {
            Ok(()) => Ok(()),
            Err(_) => Err(Error::new(ErrorKind::ConnectionReset, "flush failed")),
        }
    }

    /// Closes the client.
    pub(crate) fn close(&self) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay();

        let mut state = self.state.lock();

        // Initiate shutdown process.
        if !mem::replace(&mut *self.shutdown.lock(), true) {
            // Clear all subscriptions.
            let old_subscriptions = mem::replace(&mut state.subscriptions, HashMap::new());
            for (sid, _) in old_subscriptions {
                // Send an UNSUB message and ignore errors.
                if let Some(writer) = state.writer.as_mut() {
                    let max_msgs = None;
                    proto::encode(writer, ClientOp::Unsub { sid, max_msgs }).ok();
                    state.flush_kicker.try_send(()).ok();
                }
            }
            state.subscriptions.clear();

            // Flush the writer in case there are buffered messages.
            if let Some(writer) = state.writer.as_mut() {
                writer.flush().ok();
            }

            // Wake up all pending flushes.
            state.pongs.clear();
            drop(state);
        }

        Ok(())
    }

    /// Kicks off the shutdown process, but doesn't wait for its completion.
    pub(crate) fn shutdown(&self) {
        *self.shutdown.lock() = true;
    }

    /// Subscribes to a subject.
    pub(crate) fn subscribe(
        &self,
        subject: &str,
        queue_group: Option<&str>,
    ) -> io::Result<(u64, channel::Receiver<Message>)> {
        // Inject random delays when testing.
        inject_delay();

        let mut state = self.state.lock();

        // Check if the client is closed.
        if *self.shutdown.lock() {
            return Err(Error::new(ErrorKind::NotConnected, "the client is closed"));
        }

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
            proto::encode(writer, op).ok();
            state.flush_kicker.try_send(()).ok();
        }

        // Register the subscription in the hash map.
        let (sender, receiver) = channel::unbounded();
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
    pub(crate) fn unsubscribe(&self, sid: u64) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay();

        let mut state = self.state.lock();

        // Remove the subscription from the map.
        state.subscriptions.remove(&sid);

        // Send an UNSUB message.
        if let Some(writer) = state.writer.as_mut() {
            let max_msgs = None;
            proto::encode(writer, ClientOp::Unsub { sid, max_msgs })?;
            state.flush_kicker.try_send(()).ok();
        }

        Ok(())
    }

    /// Publishes a message with optional reply subject and headers.
    pub fn publish(
        &self,
        subject: &str,
        reply_to: Option<&str>,
        headers: Option<&Headers>,
        msg: &[u8],
    ) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay();

        let mut state = self.state.lock();

        // Check if the client is closed.
        if *self.shutdown.lock() {
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
                proto::encode(&mut state.buffer, op)?;
                state.buffer.flush()?;
                Ok(())
            }
            Some(mut writer) => {
                // If connected, write into the writer.
                let res = proto::encode(&mut writer, op);
                state.flush_kicker.try_send(()).ok();

                // If writing fails, disconnect.
                if res.is_err() {
                    state.writer = None;
                    state.pongs.clear();
                }
                res
            }
        }
    }

    /// Attempts to publish a message without blocking.
    ///
    /// This only works when the write buffer has enough space to encode the whole message.
    pub fn try_publish(
        &self,
        subject: &str,
        reply_to: Option<&str>,
        headers: Option<&Headers>,
        msg: &[u8],
    ) -> Option<io::Result<()>> {
        let mut state = self.state.try_lock()?;

        // Check if the client is closed.
        if *self.shutdown.lock() {
            return Some(Err(Error::new(ErrorKind::NotConnected, "the client is closed")));
        }

        // Estimate how many bytes the message will consume when written into the stream.
        // We must make a conservative guess: it's okay to overestimate but not to underestimate.
        let mut estimate = 1024 + subject.len() + reply_to.map_or(0, str::len) + msg.len();
        if let Some(headers) = headers {
            estimate += headers.iter().map(|(k, v)| k.len() + v.len() + 3).sum::<usize>();
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
                let res = proto::encode(&mut state.buffer, op).and_then(|_| state.buffer.flush());
                Some(res)
            }
            Some(mut writer) => {
                // Check if there's enough space in the buffer to encode the whole message.
                if writer.capacity() - writer.buffer().len() < estimate {
                    return None;
                }

                // If connected, write into the writer.
                // This is not going to block because there's enough space in the buffer.
                let res = proto::encode(&mut writer, op);
                state.flush_kicker.try_send(()).ok();

                // If writing fails, disconnect.
                if res.is_err() {
                    state.writer = None;
                    state.pongs.clear();
                }
                Some(res)
            }
        }
    }

    /// Runs the loop that connects and reconnects the client.
    fn run(&self, mut connector: Connector) -> io::Result<()> {
        let mut first_connect = true;

        loop {
            // Don't use backoff on first connect.
            let use_backoff = !first_connect;
            // Make a connection to the server.
            let (server_info, stream) = connector.connect(use_backoff)?;

            let reader = BufReader::with_capacity(128 * 1024, stream.clone());
            let writer = BufWriter::with_capacity(128 * 1024, stream);

            // Set up the new connection for this client.
            if self.reconnect(server_info, writer).is_ok() {
                // Connected! Now dispatch MSG operations.
                if !first_connect {
                    connector.get_options().reconnect_callback.call();
                }
                if self.dispatch(reader, &mut connector).is_ok() {
                    // If the client stopped gracefully, return.
                    return Ok(());
                } else {
                    connector.get_options().disconnect_callback.call();
                }
            }

            // Inject random delays when testing.
            inject_delay();

            // Check if the client is closed.
            if *self.shutdown.lock() {
                return Ok(());
            }
            first_connect = false;
        }
    }

    /// Puts the client back into connected state with the given writer.
    fn reconnect(
        &self,
        server_info: ServerInfo,
        mut writer: BufWriter<NatsStream>,
    ) -> io::Result<()> {
        // Inject random delays when testing.
        inject_delay();

        let mut state = self.state.lock();

        // Check if the client is closed.
        if *self.shutdown.lock() {
            return Err(Error::new(ErrorKind::NotConnected, "the client is closed"));
        }

        // Drop the current writer, if there is one.
        state.writer = None;

        // Inject random I/O failures when testing.
        inject_io_failure()?;

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
            )?;
        }

        // Take out expected PONGs.
        let pongs = mem::replace(&mut state.pongs, VecDeque::new());

        // Take out buffered operations.
        let buffered = state.buffer.clear();

        // Write buffered PUB operations into the new writer.
        writer.write_all(buffered)?;
        writer.flush()?;

        // All good, continue with this connection.
        *self.server_info.lock() = Some(server_info);
        state.writer = Some(writer);

        // Complete PONGs because the connection is healthy.
        for p in pongs {
            p.try_send(()).ok();
        }

        Ok(())
    }

    /// Reads messages from the server and dispatches them to subscribers.
    fn dispatch(&self, mut reader: impl BufRead, connector: &mut Connector) -> io::Result<()> {
        // Handle operations received from the server.
        while let Some(op) = proto::decode(&mut reader)? {
            // Inject random delays when testing.
            inject_delay();

            if *self.shutdown.lock() {
                break;
            }

            let mut state = self.state.lock();

            match op {
                ServerOp::Info(server_info) => {
                    for url in &server_info.connect_urls {
                        connector.add_url(url).ok();
                    }
                    *self.server_info.lock() = Some(server_info);
                }

                ServerOp::Ping => {
                    // Respond with a PONG if connected.
                    if let Some(w) = state.writer.as_mut() {
                        proto::encode(w, ClientOp::Pong)?;
                        state.flush_kicker.try_send(()).ok();
                    }
                }

                ServerOp::Pong => {
                    // If a PONG is received while disconnected, it came from a connection that isn't
                    // alive anymore and therefore doesn't correspond to the next expected PONG.
                    if state.writer.is_some() {
                        // Take the next expected PONG and complete it by sending a message.
                        if let Some(pong) = state.pongs.pop_front() {
                            pong.try_send(()).ok();
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
                        subscription.messages.try_send(msg).ok();
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
                        subscription.messages.try_send(msg).ok();
                    }
                }

                ServerOp::Err(msg) => return Err(Error::new(ErrorKind::Other, msg)),

                ServerOp::Unknown(line) => log::warn!("unknown op: {}", line),
            }
        }

        // The stream of operation is broken, meaning the connection was lost.
        Err(ErrorKind::ConnectionReset.into())
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
