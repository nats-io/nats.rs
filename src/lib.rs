//! A Rust client for the NATS.io ecosystem.
//!
//! `git clone https://github.com/nats-io/nats.rs`
//!
//! NATS.io is a simple, secure and high performance open source messaging system for cloud native
//! applications, `IoT` messaging, and microservices architectures.
//!
//! For more information see [https://nats.io/].
//!
//! [https://nats.io/]: https://nats.io/
//!
//! ## Examples
//!
//! `> cargo run --example nats-box -- -h`
//!
//! Basic connections, and those with options. The compiler will force these to be correct.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let nc = nats::connect("demo.nats.io")?;
//!
//! let nc2 = nats::Options::with_user_pass("derek", "s3cr3t!")
//!     .with_name("My Rust NATS App")
//!     .connect("127.0.0.1")?;
//!
//! let nc3 = nats::Options::with_credentials("path/to/my.creds")
//!     .connect("connect.ngs.global")?;
//!
//! let nc4 = nats::Options::new()
//!     .add_root_certificate("my-certs.pem")
//!     .connect("tls://demo.nats.io:4443")?;
//! # Ok(()) }
//! ```
//!
//! ### Publish
//!
//! ```
//! # fn main() -> std::io::Result<()> {
//! let nc = nats::connect("demo.nats.io")?;
//! nc.publish("my.subject", "Hello World!")?;
//!
//! nc.publish("my.subject", "my message")?;
//!
//! // Publish a request manually.
//! let reply = nc.new_inbox();
//! let rsub = nc.subscribe(&reply)?;
//! nc.publish_request("my.subject", &reply, "Help me!")?;
//! # Ok(()) }
//! ```
//!
//! ### Subscribe
//!
//! ```no_run
//! # fn main() -> std::io::Result<()> {
//! # use std::time::Duration;
//! let nc = nats::connect("demo.nats.io")?;
//! let sub = nc.subscribe("foo")?;
//! for msg in sub.messages() {}
//!
//! // Using next.
//! if let Some(msg) = sub.next() {}
//!
//! // Other iterators.
//! for msg in sub.try_iter() {}
//! for msg in sub.timeout_iter(Duration::from_secs(10)) {}
//!
//! // Using a threaded handler.
//! let sub = nc.subscribe("bar")?.with_handler(move |msg| {
//!     println!("Received {}", &msg);
//!     Ok(())
//! });
//!
//! // Queue subscription.
//! let qsub = nc.queue_subscribe("foo", "my_group")?;
//! # Ok(()) }
//! ```
//!
//! ### Request/Response
//!
//! ```no_run
//! # use std::time::Duration;
//! # fn main() -> std::io::Result<()> {
//! let nc = nats::connect("demo.nats.io")?;
//! let resp = nc.request("foo", "Help me?")?;
//!
//! // With a timeout.
//! let resp = nc.request_timeout("foo", "Help me?", Duration::from_secs(2))?;
//!
//! // With multiple responses.
//! for msg in nc.request_multi("foo", "Help")?.iter() {}
//!
//! // Publish a request manually.
//! let reply = nc.new_inbox();
//! let rsub = nc.subscribe(&reply)?;
//! nc.publish_request("foo", &reply, "Help me!")?;
//! let response = rsub.iter().take(1);
//! # Ok(()) }
//! ```

#![recursion_limit = "1024"]
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
    // clippy::missing_const_for_fn,
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
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::maybe_infinite_iter,
    clippy::mem_forget,
    clippy::module_name_repetitions,
    clippy::multiple_inherent_impl,
    clippy::needless_borrow,
    clippy::needless_continue,
    clippy::needless_pass_by_value,
    clippy::non_ascii_literal,
    clippy::path_buf_push_overwrite,
    clippy::print_stdout,
    clippy::pub_enum_variant_names,
    clippy::redundant_closure_for_method_calls,
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
    clippy::wildcard_dependencies,
    clippy::wildcard_enum_match_arm,
    clippy::wrong_pub_self_convention,
)]

use smol::{future, prelude::*, Timer};

use crate::asynk::client::Client;

pub mod asynk;
mod connect;
mod creds_utils;
mod headers;
mod options;
mod secure_wipe;

#[cfg(feature = "fault_injection")]
mod fault_injection;

#[cfg(feature = "fault_injection")]
use fault_injection::{inject_delay, inject_io_failure};

#[cfg(not(feature = "fault_injection"))]
async fn inject_delay() {}

#[cfg(not(feature = "fault_injection"))]
fn inject_io_failure() -> io::Result<()> {
    Ok(())
}

/// Functionality relating to subscribing to a
/// subject.
pub mod subscription;

#[doc(hidden)]
#[deprecated(since = "0.6.0", note = "this has been renamed to `Options`.")]
pub type ConnectionOptions = Options;

use std::{
    fmt,
    io::{self, Error, ErrorKind},
    sync::Arc,
    time::Duration,
};

use serde::Deserialize;

pub use {headers::Headers, options::Options, subscription::Subscription};

#[doc(hidden)]
pub use connect::ConnectInfo;

use secure_wipe::{SecureString, SecureVec};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LANG: &str = "rust";

/// Information sent by the server back to this client
/// during initial connection, and possibly again later.
#[derive(Deserialize, Debug, Clone)]
struct ServerInfo {
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
    /// The client IP as known by the server.
    #[serde(default)]
    pub client_ip: String,
}

use options::AuthStyle;

/// A NATS connection.
#[derive(Debug, Clone)]
pub struct Connection(asynk::Connection);

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
    Options::new().connect(nats_url)
}

/// A `Message` that has been published to a NATS `Subject`.
#[derive(Debug, Clone)]
pub struct Message {
    /// The NATS `Subject` that this `Message` has been published to.
    pub subject: String,
    /// The optional reply `Subject` that may be used for sending
    /// responses when using the request/reply pattern.
    pub reply: Option<String>,
    /// The `Message` contents.
    pub data: Vec<u8>,
    /// Client for publishing on the reply subject.
    pub(crate) client: Client,
    /// Optional headers associated with this `Message`.
    pub headers: Option<Headers>,
}

impl Message {
    pub(crate) fn from_async(msg: asynk::Message) -> Message {
        Message {
            subject: msg.subject,
            reply: msg.reply,
            data: msg.data,
            client: msg.client,
            headers: msg.headers,
        }
    }

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
        match self.reply.as_ref() {
            None => Err(Error::new(
                ErrorKind::InvalidInput,
                "no reply subject available",
            )),
            Some(reply) => future::block_on(self.client.publish(reply, None, None, msg.as_ref())),
        }
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
        future::block_on(self.0.subscribe(subject)).map(|s| Subscription(Arc::new(s.into())))
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
        future::block_on(self.0.queue_subscribe(subject, queue))
            .map(|s| Subscription(Arc::new(s.into())))
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
        future::block_on(self.0.publish(subject, msg))
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
        future::block_on(self.0.publish_request(subject, reply, msg))
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
        self.0.new_inbox()
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
        future::block_on(self.0.request(subject, msg)).map(Message::from_async)
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
        future::block_on(async {
            self.0
                .request(subject, msg)
                .or(async {
                    Timer::new(timeout).await;
                    Err(ErrorKind::TimedOut.into())
                })
                .await
                .map(Message::from_async)
        })
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
        future::block_on(self.0.request_multi(subject, msg))
            .map(|s| Subscription(Arc::new(s.into())))
    }

    /// Flush a NATS connection by sending a `PING` protocol and waiting for the responding `PONG`.
    /// Will fail with `TimedOut` if the server does not respond with in 10 seconds.
    /// Will fail with `NotConnected` if the server is not currently connected.
    /// Will fail with `BrokenPipe` if the connection to the server is lost.
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
        future::block_on(self.0.flush())
    }

    /// Flush a NATS connection by sending a `PING` protocol and waiting for the responding `PONG`.
    /// Will fail with `TimedOut` if the server takes longer than this duration to respond.
    /// Will fail with `NotConnected` if the server is not currently connected.
    /// Will fail with `BrokenPipe` if the connection to the server is lost.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.flush()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush_timeout(&self, duration: Duration) -> io::Result<()> {
        future::block_on(self.0.flush_timeout(duration))
    }

    /// Close a NATS connection. All clones of
    /// this `Connection` will also be closed,
    /// as the backing IO threads are shared.
    ///
    /// If the client is currently connected
    /// to a server, the outbound write buffer
    /// will be flushed in the process of
    /// shutting down.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.close();
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(self) {
        let _ = future::block_on(self.0.close());
    }

    /// Calculates the round trip time between this client and the server,
    /// if the server is currently connected. Fails with `TimedOut` if
    /// the server takes more than 10 seconds to respond.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// println!("server rtt: {:?}", nc.rtt());
    /// # Ok(())
    /// # }
    /// ```
    pub fn rtt(&self) -> io::Result<Duration> {
        future::block_on(self.0.rtt())
    }

    /// Returns the client IP as known by the server.
    /// Supported as of server version 2.1.6.
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// println!("ip: {:?}", nc.client_ip());
    /// # Ok(())
    /// # }
    /// ```
    pub fn client_ip(&self) -> io::Result<std::net::IpAddr> {
        self.0.client_ip()
    }

    /// Returns the client ID as known by the most recently connected server.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// println!("ip: {:?}", nc.client_id());
    /// # Ok(())
    /// # }
    /// ```
    pub fn client_id(&self) -> u64 {
        self.0.client_id()
    }

    /// Send an unsubscription for all subs then flush the connection, allowing any unprocessed
    /// messages to be handled by a handler function if one is configured.
    ///
    /// After the flush returns, we know that a round-trip to the server has happened after it
    /// received our unsubscription, so we shut down the subscriber afterwards.
    ///
    /// A similar method exists for the `Subscription` struct which will drain
    /// a single `Subscription` without shutting down the entire connection
    /// afterward.
    ///
    /// # Example
    /// ```
    /// # use std::sync::{Arc, atomic::{AtomicBool, Ordering::SeqCst}};
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let received = Arc::new(AtomicBool::new(false));
    /// let received_2 = received.clone();
    ///
    /// nc.subscribe("test.drain")?.with_handler(move |m| {
    ///     received_2.store(true, SeqCst);
    ///     Ok(())
    /// });
    ///
    /// nc.publish("test.drain", "message")?;
    /// nc.drain()?;
    ///
    /// # std::thread::sleep(std::time::Duration::from_secs(1));
    ///
    /// assert!(received.load(SeqCst));
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn drain(&self) -> io::Result<()> {
        future::block_on(self.0.drain())
    }

    /// Publish a message which may have a reply subject or headers set.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo.headers")?;
    /// let headers = [("header1", "value1"),
    ///                ("header2", "value2")].iter().collect();
    /// let reply_to = None;
    /// nc.publish_with_reply_or_headers("foo.headers", reply_to, Some(&headers), "Hello World!")?;
    /// nc.flush()?;
    /// let message = sub.next_timeout(std::time::Duration::from_secs(2)).unwrap();
    /// assert_eq!(message.headers.unwrap().len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish_with_reply_or_headers(
        &self,
        subject: &str,
        reply: Option<&str>,
        headers: Option<&Headers>,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        future::block_on(self.0.publish_with_reply_or_headers(
            subject,
            reply,
            headers,
            msg.as_ref(),
        ))
    }
}
