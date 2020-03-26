//! A Rust client for the NATS.io ecosystem.
//!
//! `git clone https://github.com/nats-io/nats.rs`
//!
//! NATS.io is a simple, secure and high performance open source messaging system for cloud native applications,
//! `IoT` messaging, and microservices architectures.
//!
//! For more information see [https://nats.io/].
//!
//! [https://nats.io/]: https://nats.io/
//!
#![cfg_attr(test, deny(warnings))]
#![deny(
    missing_docs,
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_qualifications
)]
#![deny(
    // over time, consider enabling the following commented-out lints:
    // clippy::else_if_without_else,
    // clippy::indexing_slicing,
    // clippy::multiple_crate_versions,
    // clippy::multiple_inherent_impl,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::checked_conversions,
    clippy::decimal_literal_representation,
    clippy::doc_markdown,
    clippy::empty_enum,
    clippy::explicit_into_iter_loop,
    clippy::explicit_iter_loop,
    clippy::expl_impl_clone_on_copy,
    clippy::fallible_impl_from,
    clippy::filter_map,
    clippy::filter_map_next,
    clippy::find_map,
    clippy::float_arithmetic,
    clippy::get_unwrap,
    clippy::if_not_else,
    clippy::inline_always,
    clippy::invalid_upcast_comparisons,
    clippy::items_after_statements,
    clippy::map_flatten,
    clippy::match_same_arms,
    clippy::maybe_infinite_iter,
    clippy::mem_forget,
    clippy::missing_const_for_fn,
    clippy::module_name_repetitions,
    clippy::mut_mut,
    clippy::needless_borrow,
    clippy::needless_continue,
    clippy::needless_pass_by_value,
    clippy::non_ascii_literal,
    clippy::option_map_unwrap_or,
    clippy::option_map_unwrap_or_else,
    clippy::path_buf_push_overwrite,
    clippy::print_stdout,
    clippy::pub_enum_variant_names,
    clippy::redundant_closure_for_method_calls,
    clippy::replace_consts,
    clippy::result_map_unwrap_or_else,
    clippy::shadow_reuse,
    clippy::shadow_same,
    clippy::shadow_unrelated,
    clippy::single_match_else,
    clippy::string_add,
    clippy::string_add_assign,
    clippy::type_repetition_in_bounds,
    clippy::unicode_not_nfc,
    clippy::unimplemented,
    clippy::unseparated_literal_suffix,
    clippy::used_underscore_binding,
    clippy::wildcard_dependencies,
    clippy::wildcard_enum_match_arm,
    clippy::wrong_pub_self_convention,
)]

mod parser;
mod server_manager;
mod shared_state;

/// Functionality relating to subscribing to a
/// subject.
pub mod subscription;

use std::{
    io::{self, BufWriter, Error, ErrorKind, Write},
    marker::PhantomData,
    net::{Shutdown, SocketAddr, TcpStream},
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
    {fmt, str, thread},
};

use serde::Serialize;

pub use subscription::Subscription;

use {server_manager::ServerManager, shared_state::SharedState};

const VERSION: &str = "0.0.1";
const LANG: &str = "rust";

fn parse_server_addresses<S: AsRef<str>>(
    connect_str: S,
) -> std::io::Result<Vec<server_manager::Server>> {
    fn check_port(nats_url: &str) -> String {
        match nats_url.parse::<SocketAddr>() {
            Ok(_) => nats_url.to_string(),
            Err(_) => {
                if nats_url.find(':').is_some() {
                    nats_url.to_string()
                } else {
                    format!("{}:4222", nats_url)
                }
            }
        }
    }

    let server_strings = connect_str.as_ref().split(',');

    let mut ret = vec![];

    for server_string in server_strings {
        let addr = if server_string.starts_with("nats://") {
            check_port(&server_string["nats://".len()..])
        } else {
            check_port(server_string)
        };
        ret.push(server_manager::Server {
            addr: addr.to_string(),
            retries: 0,
        });
    }

    if ret.is_empty() {
        Err(Error::new(ErrorKind::InvalidInput, "No configured servers"))
    } else {
        Ok(ret)
    }
}

#[doc(hidden)]
pub mod options_typestate {
    /// `ConnectionOptions` typestate indicating
    /// that there has not yet been
    /// any auth-related configuration
    /// provided yet.
    #[derive(Debug, Copy, Clone, Default)]
    pub struct NoAuth;

    /// `ConnectionOptions` typestate indicating
    /// that auth-related configuration
    /// has been provided, and may not
    /// be provided again.
    #[derive(Debug, Copy, Clone)]
    pub struct Authenticated;

    /// `ConnectionOptions` typestate indicating that
    /// this `ConnectionOptions` has been used to create
    /// a `Connection` and may not be changed.
    #[derive(Debug, Copy, Clone)]
    pub struct Finalized;
}

type FinalizedOptions = ConnectionOptions<crate::options_typestate::Finalized>;

/// A configuration object for a NATS connection.
#[derive(Clone, Debug, Default)]
pub struct ConnectionOptions<TypeState> {
    auth: AuthStyle,
    name: Option<String>,
    no_echo: bool,
    typestate: PhantomData<TypeState>,
}

impl ConnectionOptions<options_typestate::NoAuth> {
    /// `ConnectionOptions` for establishing a new NATS `Connection`.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let options = nats::ConnectionOptions::new();
    /// let nc = options.connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new() -> ConnectionOptions<options_typestate::NoAuth> {
        ConnectionOptions::default()
    }

    /// Authenticate with NATS using a token.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::ConnectionOptions::new()
    ///     .with_token("t0k3n!")
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_token(self, token: &str) -> ConnectionOptions<options_typestate::Authenticated> {
        ConnectionOptions {
            auth: AuthStyle::Token(token.to_string()),
            typestate: PhantomData,
            no_echo: self.no_echo,
            name: self.name,
        }
    }

    /// Authenticate with NATS using a username and password.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::ConnectionOptions::new()
    ///     .with_user_pass("derek", "s3cr3t!")
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_user_pass(
        self,
        user: &str,
        password: &str,
    ) -> ConnectionOptions<options_typestate::Authenticated> {
        ConnectionOptions {
            auth: AuthStyle::UserPass(user.to_string(), password.to_string()),
            typestate: PhantomData,
            no_echo: self.no_echo,
            name: self.name,
        }
    }
}

impl<TypeState> ConnectionOptions<TypeState> {
    /// Add a name option to this configuration.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::ConnectionOptions::new()
    ///     .with_name("My App")
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_name(mut self, name: &str) -> ConnectionOptions<TypeState> {
        self.name = Some(name.to_string());
        self
    }

    /// Select option to not deliver messages that we have published.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::ConnectionOptions::new()
    ///     .no_echo()
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub const fn no_echo(mut self) -> ConnectionOptions<TypeState> {
        self.no_echo = true;
        self
    }

    /// Establish a `Connection` with a NATS server.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let options = nats::ConnectionOptions::new();
    /// let nc = options.connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect(self, nats_url: &str) -> io::Result<Connection> {
        let servers = parse_server_addresses(nats_url)?;

        let options = ConnectionOptions {
            auth: self.auth,
            no_echo: self.no_echo,
            name: self.name,
            // move options into the Finalized state by setting
            // `typestate` to `PhantomData<Finalized>`
            typestate: PhantomData,
        };

        // Setup the server manager we will move to the its own thread.
        let mut server_manager = ServerManager::connect(options, servers)?;

        let mut conn = Connection {
            sid: AtomicUsize::new(1),
            shared_state: server_manager.shared_state.clone(),
            reader: None,
        };

        let read_loop = thread::spawn(move || {
            // FIXME(dlc) - Capture?
            if let Err(error) = server_manager.read_loop() {
                eprintln!("error encountered in the parser read_loop: {:?}", error);
                return;
            }
        });
        conn.reader = Some(read_loop);

        let usec = Duration::from_micros(1);
        let wait: [Duration; 5] = [10 * usec, 100 * usec, 500 * usec, 1000 * usec, 5000 * usec];

        let shared_state = conn.shared_state.clone();
        let flusher_loop = thread::spawn(move || loop {
            thread::park();
            let start_len = start_flush_cycle(&shared_state.writer);
            thread::yield_now();
            let mut cur_len = shared_state.writer.lock().unwrap().writer.buffer().len();
            if cur_len != start_len {
                for d in &wait {
                    thread::sleep(*d);
                    {
                        let w = shared_state.writer.lock().unwrap();
                        cur_len = w.writer.buffer().len();
                        if cur_len == 0 || cur_len == start_len || w.closed {
                            break;
                        }
                    }
                }
            }
            let mut w = shared_state.writer.lock().unwrap();
            w.in_flush = false;
            if cur_len > 0 {
                if let Err(error) = w.writer.flush() {
                    eprintln!("Flusher thread failed to flush: {:?}", error);
                    break;
                }
            }
            if w.closed {
                break;
            }
        });

        conn.shared_state.writer.lock().unwrap().flusher = Some(flusher_loop);

        Ok(conn)
    }
}

#[derive(Debug)]
pub(crate) struct Outbound {
    writer: BufWriter<TcpStream>,
    flusher: Option<thread::JoinHandle<()>>,
    should_flush: bool,
    in_flush: bool,
    closed: bool,
}

impl Outbound {
    fn write_response(&mut self, subj: &str, msgb: &[u8]) -> io::Result<()> {
        write!(self.writer, "PUB {} {}\r\n", subj, msgb.len())?;
        self.writer.write_all(msgb)?;
        self.writer.write_all(b"\r\n")?;
        if self.should_flush && !self.in_flush {
            self.kick_flusher();
        }
        Ok(())
    }

    fn kick_flusher(&self) {
        if let Some(flusher) = &self.flusher {
            flusher.thread().unpark();
        }
    }
}

/// A NATS connection.
#[derive(Debug)]
pub struct Connection {
    shared_state: Arc<SharedState>,
    sid: AtomicUsize,
    reader: Option<thread::JoinHandle<()>>,
}

#[derive(Serialize, Clone, Debug)]
enum AuthStyle {
    //    Credentials(String, String),
    Token(String),
    UserPass(String, String),
    None,
}

impl Default for AuthStyle {
    fn default() -> AuthStyle {
        AuthStyle::None
    }
}

/// Connect to a NATS server at the given url.
///
/// # Example
/// ```
/// # fn main() -> std::io::Result<()> {
/// let nc = nats::connect("demo.nats.io")?;
/// # Ok(())
/// # }
/// ```
pub fn connect(nats_url: &str) -> io::Result<Connection> {
    ConnectionOptions::new().connect(nats_url)
}

fn start_flush_cycle(wbuf: &Mutex<Outbound>) -> usize {
    let mut w = wbuf.lock().unwrap();
    w.in_flush = true;
    w.writer.buffer().len()
}

/// A `Message` that has been published to a NATS `Subject`.
#[derive(Debug)]
pub struct Message {
    /// The NATS `Subject` that this `Message` has been published to.
    pub subject: String,
    /// The optional reply `Subject` that may be used for sending
    /// responses when using the request/reply pattern.
    pub reply: Option<String>,
    /// The `Message` contents.
    pub data: Vec<u8>,
    pub(crate) responder: Option<Arc<SharedState>>,
}

impl Message {
    /// Respond to a request message.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.subscribe("help.request")?.with_handler(move |m| {
    ///     m.respond("ans=42")?; Ok(())
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn respond(&self, msg: impl AsRef<[u8]>) -> io::Result<()> {
        if let Some(shared_state) = &self.responder {
            if let Some(reply) = &self.reply {
                shared_state
                    .writer
                    .lock()
                    .unwrap()
                    .write_response(reply, msg.as_ref())?;
            }
        } else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "No reply subject available",
            ));
        }
        Ok(())
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut body = format!("[{} bytes]", self.data.len());
        if let Ok(str) = str::from_utf8(&self.data) {
            body = str.to_string();
        }
        if let Some(reply) = &self.reply {
            write!(
                f,
                "Message {{\n  subject: \"{}\",\n  reply: \"{}\",\n  data: \"{}\"\n}}",
                self.subject, reply, body
            )
        } else {
            write!(
                f,
                "Message {{\n  subject: \"{}\",\n  data: \"{}\"\n}}",
                self.subject, body
            )
        }
    }
}

impl Connection {
    /// Create a subscription for the given NATS connection.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe(&self, subject: &str) -> io::Result<Subscription> {
        self.do_subscribe(subject, None)
    }

    /// Create a queue subscription for the given NATS connection.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.queue_subscribe("foo", "production")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn queue_subscribe(&self, subject: &str, queue: &str) -> io::Result<Subscription> {
        self.do_subscribe(subject, Some(queue))
    }

    fn do_subscribe(&self, subject: &str, queue: Option<&str>) -> io::Result<Subscription> {
        let sid = self.sid.fetch_add(1, Ordering::Relaxed);
        {
            let w = &mut self.shared_state.writer.lock().unwrap();
            match queue {
                Some(q) => write!(w.writer, "SUB {} {} {}\r\n", subject, q, sid)?,
                None => write!(w.writer, "SUB {} {}\r\n", subject, sid)?,
            }
            if w.should_flush && !w.in_flush {
                w.kick_flusher();
            }
        }
        let (sub, recv) = crossbeam_channel::unbounded();
        {
            let mut subs = self.shared_state.subs.write().unwrap();
            subs.insert(
                sid,
                (subject.to_string(), queue.map(ToString::to_string), sub),
            );
        }
        // TODO(dlc) - Should we do a flush and check errors?
        Ok(Subscription {
            subject: subject.to_string(),
            sid,
            recv,
            shared_state: self.shared_state.clone(),
            do_unsub: true,
        })
    }

    #[doc(hidden)]
    pub fn batch(&self) {
        self.shared_state.writer.lock().unwrap().should_flush = false;
    }

    #[doc(hidden)]
    pub fn unbatch(&self) {
        self.shared_state.writer.lock().unwrap().should_flush = true;
    }

    #[inline]
    fn write_pub_msg(&self, subj: &str, reply: Option<&str>, msgb: &[u8]) -> io::Result<()> {
        let mut w = self.shared_state.writer.lock().unwrap();
        if let Some(reply) = reply {
            write!(w.writer, "PUB {} {} {}\r\n", subj, reply, msgb.len())?;
        } else {
            write!(w.writer, "PUB {} {}\r\n", subj, msgb.len())?;
        }
        w.writer.write_all(msgb)?;
        w.writer.write_all(b"\r\n")?;
        if w.should_flush && !w.in_flush {
            w.kick_flusher();
        }
        Ok(())
    }

    /// Publish a message on the given subject.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.publish("foo", "Hello World!")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        self.write_pub_msg(subject, None, msg.as_ref())
    }

    /// Publish a message on the given subject with a reply subject for responses.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let reply = nc.new_inbox();
    /// let rsub = nc.subscribe(&reply)?;
    /// nc.publish_request("foo", &reply, "Help me!")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish_request(
        &self,
        subject: &str,
        reply: &str,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        self.write_pub_msg(subject, Some(reply), msg.as_ref())
    }

    /// Create a new globally unique inbox which can be used for replies.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let reply = nc.new_inbox();
    /// let rsub = nc.subscribe(&reply)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_inbox(&self) -> String {
        format!("_INBOX.{}.{}", self.shared_state.id, nuid::next())
    }

    /// Publish a message on the given subject as a request and receive the response.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// let resp = nc.request("foo", "Help me?")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Message> {
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply)?;
        self.publish_request(subject, &reply, msg)?;
        match sub.next() {
            Some(msg) => Ok(msg),
            None => Err(Error::new(ErrorKind::NotConnected, "No response")),
        }
    }

    /// Publish a message on the given subject as a request and receive the response.
    /// This call will return after the timeout duration if no response is received.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// let resp = nc.request_timeout("foo", "Help me?", std::time::Duration::from_secs(2))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_timeout(
        &self,
        subject: &str,
        msg: impl AsRef<[u8]>,
        timeout: Duration,
    ) -> io::Result<Message> {
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply)?;
        self.publish_request(subject, &reply, msg)?;
        match sub.next_timeout(timeout) {
            Ok(msg) => Ok(msg),
            Err(_) => Err(Error::new(ErrorKind::TimedOut, "No response")),
        }
    }

    /// Publish a message on the given subject as a request and allow multiple responses.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// for msg in nc.request_multi("foo", "Help")?.iter().take(1) {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_multi(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Subscription> {
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply)?;
        self.publish_request(subject, &reply, msg)?;
        Ok(sub)
    }

    /// Flush a NATS connection by sending a `PING` protocol and waiting for the responding `PONG`.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.flush()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush(&self) -> io::Result<()> {
        // TODO(dlc) - bounded or oneshot?
        self.unbatch();
        let (s, r) = crossbeam_channel::unbounded();
        {
            let mut pongs = self.shared_state.pongs.lock().unwrap();
            pongs.push_back(s);
        }
        self.send_ping()?;
        r.recv().unwrap();
        Ok(())
    }

    fn send_ping(&self) -> io::Result<()> {
        let w = &mut self.shared_state.writer.lock().unwrap().writer;
        w.write_all(b"PING\r\n")?;
        // Flush in place on pings.
        w.flush()?;
        Ok(())
    }

    /// Close a NATS connection.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(self) -> io::Result<()> {
        drop(self);
        Ok(())
    }

    /// Set a callback to be executed during disconnection.
    /// Returns `true` if this is the first time a callback
    /// has been set.
    pub fn set_disconnect_callback<F>(&self, cb: F) -> bool
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut shared_cb = self.shared_state.disconnect_callback.write().unwrap();
        let ret = shared_cb.is_none();
        *shared_cb = Some(Box::new(cb));
        ret
    }

    /// Set a callback to be executed during reconnection,
    /// Returns `true` if this is the first time a callback
    /// has been set.
    pub fn set_reconnect_callback<F>(&self, cb: F) -> bool
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut shared_cb = self.shared_state.reconnect_callback.write().unwrap();
        let ret = shared_cb.is_none();
        *shared_cb = Some(Box::new(cb));
        ret
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.shared_state.writer.lock().unwrap().closed = true;
        let flusher = self.shared_state.writer.lock().unwrap().flusher.take();
        if let Some(ft) = flusher {
            ft.thread().unpark();
            if let Err(error) = ft.join() {
                eprintln!("error encountered in flusher thread: {:?}", error);
            }
        }
        self.shared_state
            .shutting_down
            .store(true, Ordering::SeqCst);
        // Shutdown socket.
        let ret = self
            .shared_state
            .writer
            .lock()
            .unwrap()
            .writer
            .get_mut()
            .shutdown(Shutdown::Both);
        if let Some(rt) = self.reader.take() {
            if let Err(error) = rt.join() {
                eprintln!("error encountered in reader thread: {:?}", error);
            }
        }
        if let Err(error) = ret {
            eprintln!("error closing Connected during Drop: {:?}", error);
        }
    }
}
