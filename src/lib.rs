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

mod inbound;
mod outbound;
mod parser;
mod shared_state;

/// Functionality relating to TLS configuration
pub mod tls;

/// Functionality relating to subscribing to a
/// subject.
pub mod subscription;

use std::{
    fmt,
    io::{self, Error, ErrorKind},
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};

pub use subscription::Subscription;

use {
    inbound::{Inbound, Reader},
    outbound::{Outbound, Writer},
    shared_state::{parse_server_addresses, Server, SharedState, SubscriptionState},
    tls::{split_tls, TlsReader, TlsWriter},
};

const VERSION: &str = "0.0.1";
const LANG: &str = "rust";

/// Information sent by the server back to this client
/// during initial connection, and possibly again later.
#[derive(Deserialize, Debug)]
pub struct ServerInfo {
    /// The unique identifier of the NATS server.
    pub server_id: String,
    /// Generated Server Name.
    #[serde(default)]
    pub server_name: String,
    /// The host specified in the cluster parameter/options.
    pub host: String,
    /// The port number specified in the cluster parameter/options.
    pub port: i16,
    /// The version of the NATS server.
    pub version: String,
    /// If this is set, then the server should try to authenticate upon connect.
    #[serde(default)]
    pub auth_required: bool,
    /// If this is set, then the server must authenticate using TLS.
    #[serde(default)]
    pub tls_required: bool,
    /// Maximum payload size that the server will accept.
    pub max_payload: i32,
    /// The protocol version in use.
    pub proto: i8,
    /// The server-assigned client ID. This may change during reconnection.
    pub client_id: u64,
    /// The version of golang the NATS server was built with.
    pub go: String,
    #[serde(default)]
    /// The nonce used for nkeys.
    pub nonce: String,
    /// A list of server urls that a client can connect to.
    #[serde(default)]
    pub connect_urls: Vec<String>,
}

impl ServerInfo {
    pub(crate) fn learned_servers(&self) -> Vec<Server> {
        parse_server_addresses(&self.connect_urls)
    }
}

/// A `ConnectionStatus` describes the current sub-status of a `Connection`.
#[derive(Debug, PartialEq, Clone, Copy)]
enum ConnectionStatus {
    /// An established connection.
    Connected,
    /// A permanently closed connection.
    Closed,
    /// A connection that has lost connectivity, but may reestablish connectivity.
    Disconnected,
    /// A connection in the process of reestablishing connectivity after a disconnect.
    Reconnecting,
}

#[derive(Default, Clone)]
pub(crate) struct Callback(Option<Arc<dyn Fn(&ServerInfo) + Send + Sync + 'static>>);

impl fmt::Debug for Callback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(
                &"callback",
                if self.0.is_some() { &"set" } else { &"unset" },
            )
            .finish()
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

type FinalizedOptions = ConnectionOptions<options_typestate::Finalized>;

/// A configuration object for a NATS connection.
pub struct ConnectionOptions<TypeState> {
    typestate: PhantomData<TypeState>,
    auth: AuthStyle,
    name: Option<String>,
    no_echo: bool,
    max_reconnects: Option<usize>,
    reconnect_buffer_size: usize,
    disconnect_callback: Callback,
    reconnect_callback: Callback,
    tls_connector: Option<tls::TlsConnector>,
    tls_required: bool,
}

impl Default for ConnectionOptions<options_typestate::NoAuth> {
    fn default() -> ConnectionOptions<options_typestate::NoAuth> {
        ConnectionOptions {
            typestate: PhantomData,
            auth: AuthStyle::None,
            name: None,
            no_echo: false,
            reconnect_buffer_size: 8 * 1024 * 1024,
            max_reconnects: Some(60),
            disconnect_callback: Callback(None),
            reconnect_callback: Callback(None),
            tls_connector: None,
            tls_required: false,
        }
    }
}

impl<T> fmt::Debug for ConnectionOptions<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(&"auth", &self.auth)
            .entry(&"name", &self.name)
            .entry(&"no_echo", &self.no_echo)
            .entry(&"reconnect_buffer_size", &self.reconnect_buffer_size)
            .entry(&"max_reconnects", &self.max_reconnects)
            .entry(&"disconnect_callback", &self.disconnect_callback)
            .entry(&"reconnect_callback", &self.reconnect_callback)
            .entry(
                &"tls_connector",
                if self.tls_connector.is_some() {
                    &"set"
                } else {
                    &"unset"
                },
            )
            .entry(&"tls_required", &self.tls_required)
            .finish()
    }
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
            disconnect_callback: self.disconnect_callback,
            reconnect_callback: self.reconnect_callback,
            reconnect_buffer_size: self.reconnect_buffer_size,
            max_reconnects: self.max_reconnects,
            tls_connector: self.tls_connector,
            tls_required: self.tls_required,
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
            reconnect_buffer_size: self.reconnect_buffer_size,
            disconnect_callback: self.disconnect_callback,
            reconnect_callback: self.reconnect_callback,
            max_reconnects: self.max_reconnects,
            tls_connector: self.tls_connector,
            tls_required: self.tls_required,
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

    /// Set the maximum number of reconnect attempts.
    /// If no servers remain that are under this threshold,
    /// all servers will still be attempted.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::ConnectionOptions::new()
    ///     .max_reconnects(Some(3))
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub const fn max_reconnects(
        mut self,
        max_reconnects: Option<usize>,
    ) -> ConnectionOptions<TypeState> {
        self.max_reconnects = max_reconnects;
        self
    }

    /// Set the maximum amount of bytes to buffer
    /// when accepting outgoing traffic in disconnected
    /// mode.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::ConnectionOptions::new()
    ///     .reconnect_buffer_size(64 * 1024)
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub const fn reconnect_buffer_size(
        mut self,
        reconnect_buffer_size: usize,
    ) -> ConnectionOptions<TypeState> {
        self.reconnect_buffer_size = reconnect_buffer_size;
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
        let options = ConnectionOptions {
            auth: self.auth,
            no_echo: self.no_echo,
            name: self.name,
            reconnect_buffer_size: self.reconnect_buffer_size,
            max_reconnects: self.max_reconnects,
            disconnect_callback: self.disconnect_callback,
            reconnect_callback: self.reconnect_callback,
            tls_connector: self.tls_connector,
            tls_required: self.tls_required,
            // move options into the Finalized state by setting
            // `typestate` to `PhantomData<Finalized>`
            typestate: PhantomData,
        };

        let shared_state = SharedState::connect(options, nats_url)?;

        let conn = Connection {
            sid: Arc::new(AtomicUsize::new(1)),
            shutdown_dropper: Arc::new(ShutdownDropper {
                shared_state: shared_state.clone(),
            }),
            shared_state,
        };
        Ok(conn)
    }

    /// Set a callback to be executed when connectivity to
    /// a server has been lost.
    pub fn set_disconnect_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(&ServerInfo) + Send + Sync + 'static,
    {
        self.disconnect_callback = Callback(Some(Arc::new(cb)));
        self
    }

    /// Set a callback to be executed when connectivity to a
    /// server has been established.
    pub fn set_reconnect_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(&ServerInfo) + Send + Sync + 'static,
    {
        self.disconnect_callback = Callback(Some(Arc::new(cb)));
        self
    }

    /// Setting this requires that TLS be set for all server connections.
    ///
    /// If you only want to use TLS for some server connections, you may
    /// declare them separately in the connect string by prefixing them
    /// with `tls://host:port` instead of `nats://host:port`.
    ///
    /// If you want to use a particular TLS configuration, see
    /// the `nats::tls::tls_connector` method and the
    /// `nats::ConnectionOptions::tls_connector` method below
    /// to apply the desired configuration to all server connections.
    ///
    /// # Examples
    /// ```
    /// # fn main() -> std::io::Result<()> {
    ///
    /// let nc = nats::ConnectionOptions::new()
    ///     .tls_required(true)
    ///     .connect("tls://demo.nats.io:4443")?;
    /// # Ok(())
    /// # }
    /// ```
    pub const fn tls_required(mut self, tls_required: bool) -> Self {
        self.tls_required = tls_required;
        self
    }

    /// Allows a particular TLS configuration to be set
    /// for upgrading TCP connections to TLS connections.
    ///
    /// Note that this also enforces that TLS will be
    /// enabled for all connections to all servers.
    ///
    /// # Examples
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let mut tls_connector = nats::tls::builder()
    ///     .identity(nats::tls::Identity::from_pkcs12(b"der_bytes", "my_password").unwrap())
    ///     .add_root_certificate(nats::tls::Certificate::from_pem(b"my_pem_bytes").unwrap())
    ///     .build()
    ///     .unwrap();
    ///
    /// let nc = nats::ConnectionOptions::new()
    ///     .tls_connector(tls_connector)
    ///     .connect("tls://demo.nats.io:4443")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls_connector(mut self, connector: tls::TlsConnector) -> Self {
        self.tls_connector = Some(connector);
        self.tls_required = true;
        self
    }
}

// This type exists to hold a reference count
// for all high-level user-controlled structures.
// Once all of the user structures drop,
// the background IO system should flush
// everything and shut down.
#[derive(Debug)]
pub(crate) struct ShutdownDropper {
    shared_state: Arc<SharedState>,
}

impl Drop for ShutdownDropper {
    fn drop(&mut self) {
        self.shared_state.shut_down();
    }
}

/// A NATS connection.
#[derive(Debug, Clone)]
pub struct Connection {
    shared_state: Arc<SharedState>,
    shutdown_dropper: Arc<ShutdownDropper>,
    sid: Arc<AtomicUsize>,
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
                shared_state.outbound.send_response(reply, msg.as_ref())?;
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
        if let Ok(str) = std::str::from_utf8(&self.data) {
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
        self.shared_state
            .outbound
            .send_sub_msg(subject, queue, sid)?;
        let (sub, recv) = crossbeam_channel::unbounded();
        {
            let mut subs = self.shared_state.subs.write();
            subs.insert(
                sid,
                SubscriptionState {
                    subject: subject.to_string(),
                    queue: queue.map(ToString::to_string),
                    sender: sub,
                },
            );
        }
        // TODO(dlc) - Should we do a flush and check errors?
        Ok(Subscription {
            subject: subject.to_string(),
            sid,
            recv,
            shared_state: self.shared_state.clone(),
            shutdown_dropper: self.shutdown_dropper.clone(),
            do_unsub: true,
        })
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
        self.shared_state
            .outbound
            .send_pub_msg(subject, None, msg.as_ref())
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
        self.shared_state
            .outbound
            .send_pub_msg(subject, Some(reply), msg.as_ref())
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
        let (s, r) = crossbeam_channel::bounded(1);
        {
            let mut pongs = self.shared_state.pongs.lock();
            pongs.push_back(s);
        }
        self.shared_state.outbound.send_ping()?;
        r.recv().unwrap();
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
}
