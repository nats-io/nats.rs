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

//! A Rust client for the NATS.io ecosystem.
//!
//! <div class="warning"> This is the old legacy client. It will not get new features or updates beyond critical security fixes.
//! Use <a href="https://crates.io/crates/async-nats">async-nats</a> instead. </div>
//!
//! `git clone https://github.com/nats-io/nats.rs`
//!
//! NATS.io is a simple, secure and high performance open source messaging
//! system for cloud native applications, `IoT` messaging, and microservices
//! architectures.
//!
//! For async API refer to the [`asynk`] module.
//!
//! For more information see [https://nats.io/].
//!
//! [https://nats.io/]: https://nats.io/
//!
//! ## Examples
//!
//! `> cargo run --example nats-box -- -h`
//!
//! Basic connections, and those with options. The compiler will force these to
//! be correct.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let nc = nats::connect("demo.nats.io")?;
//!
//! let nc2 = nats::Options::with_user_pass("derek", "s3cr3t!")
//!     .with_name("My Rust NATS App")
//!     .connect("127.0.0.1")?;
//!
//! let nc3 = nats::Options::with_credentials("path/to/my.creds").connect("connect.ngs.global")?;
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

#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(
    feature = "fault_injection",
    deny(
        future_incompatible,
        missing_copy_implementations,
        missing_docs,
        nonstandard_style,
        rust_2018_idioms,
        trivial_casts,
        trivial_numeric_casts,
        unsafe_code,
        unused,
        unused_qualifications
    )
)]
#![cfg_attr(feature = "fault_injection", deny(
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
    clippy::filter_map_next,
    clippy::float_arithmetic,
    clippy::get_unwrap,
    clippy::if_not_else,
    clippy::inline_always,
    clippy::invalid_upcast_comparisons,
    clippy::items_after_statements,
    clippy::manual_filter_map,
    clippy::manual_find_map,
    clippy::map_flatten,
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::maybe_infinite_iter,
    clippy::mem_forget,
    clippy::needless_borrow,
    clippy::needless_continue,
    clippy::needless_pass_by_value,
    clippy::non_ascii_literal,
    clippy::path_buf_push_overwrite,
    clippy::print_stdout,
    clippy::single_match_else,
    clippy::string_add,
    clippy::string_add_assign,
    clippy::type_repetition_in_bounds,
    clippy::unicode_not_nfc,
    clippy::unimplemented,
    clippy::unseparated_literal_suffix,
    clippy::wildcard_dependencies,
    clippy::wildcard_enum_match_arm,
))]
#![allow(
    clippy::match_like_matches_macro,
    clippy::await_holding_lock,
    clippy::shadow_reuse,
    clippy::shadow_same,
    clippy::shadow_unrelated,
    clippy::wildcard_enum_match_arm,
    clippy::module_name_repetitions
)]
// As this is a deprecated client, we don't want warnings from new lints to make CI red.
#![allow(clippy::all)]
#![allow(warnings)]
/// Async-enabled NATS client.
pub mod asynk;

mod auth_utils;
mod client;
mod connect;
mod connector;
mod message;
mod options;
mod proto;
mod secure_wipe;
mod subscription;

/// Header constants and types.
pub mod header;

/// `JetStream` stream management and consumers.
pub mod jetstream;

#[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
pub mod kv;

#[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
pub mod object_store;

#[cfg(feature = "fault_injection")]
mod fault_injection;

#[cfg(feature = "fault_injection")]
use fault_injection::{inject_delay, inject_io_failure};

#[cfg(not(feature = "fault_injection"))]
fn inject_delay() {}

#[cfg(not(feature = "fault_injection"))]
fn inject_io_failure() -> io::Result<()> {
    Ok(())
}

// comment out until we reach MSRV 1.54.0
// #[doc = include_str!("../docs/migration-guide-0.17.0.md")]
// #[derive(Copy, Clone)]
// pub struct Migration0170;

#[doc(hidden)]
#[deprecated(since = "0.6.0", note = "this has been renamed to `Options`.")]
pub type ConnectionOptions = Options;

#[doc(hidden)]
#[deprecated(since = "0.17.0", note = "this has been moved to `header::HeaderMap`.")]
pub type Headers = HeaderMap;

pub use header::HeaderMap;

use std::{
    io::{self, Error, ErrorKind},
    sync::Arc,
    time::{Duration, Instant},
};

use lazy_static::lazy_static;
use regex::Regex;

pub use connector::{IntoServerList, ServerAddress};
pub use jetstream::JetStreamOptions;
pub use message::Message;
pub use options::Options;
pub use subscription::{Handler, Subscription};

/// A re-export of the `rustls` crate used in this crate,
/// for use in cases where manual client configurations
/// must be provided using `Options::tls_client_config`.
pub use rustls;

#[doc(hidden)]
pub use connect::ConnectInfo;

use client::Client;
use options::AuthStyle;
use secure_wipe::{SecureString, SecureVec};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LANG: &str = "rust";
const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

lazy_static! {
    static ref VERSION_RE: Regex = Regex::new(r#"\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?"#).unwrap();
}

/// Information sent by the server back to this client
/// during initial connection, and possibly again later.
#[allow(unused)]
#[derive(Debug, Default, Clone)]
pub struct ServerInfo {
    /// The unique identifier of the NATS server.
    pub server_id: String,
    /// Generated Server Name.
    pub server_name: String,
    /// The host specified in the cluster parameter/options.
    pub host: String,
    /// The port number specified in the cluster parameter/options.
    pub port: u16,
    /// The version of the NATS server.
    pub version: String,
    /// If this is set, then the server should try to authenticate upon
    /// connect.
    pub auth_required: bool,
    /// If this is set, then the server must authenticate using TLS.
    pub tls_required: bool,
    /// Maximum payload size that the server will accept.
    pub max_payload: usize,
    /// The protocol version in use.
    pub proto: i8,
    /// The server-assigned client ID. This may change during reconnection.
    pub client_id: u64,
    /// The version of golang the NATS server was built with.
    pub go: String,
    /// The nonce used for nkeys.
    pub nonce: String,
    /// A list of server urls that a client can connect to.
    pub connect_urls: Vec<String>,
    /// The client IP as known by the server.
    pub client_ip: String,
    /// Whether the server supports headers.
    pub headers: bool,
    /// Whether server goes into lame duck mode.
    pub lame_duck_mode: bool,
}

impl ServerInfo {
    fn parse(s: &str) -> Option<ServerInfo> {
        let mut obj = json::parse(s).ok()?;
        Some(ServerInfo {
            server_id: obj["server_id"].take_string()?,
            server_name: obj["server_name"].take_string().unwrap_or_default(),
            host: obj["host"].take_string()?,
            port: obj["port"].as_u16()?,
            version: obj["version"].take_string()?,
            auth_required: obj["auth_required"].as_bool().unwrap_or(false),
            tls_required: obj["tls_required"].as_bool().unwrap_or(false),
            max_payload: obj["max_payload"].as_usize()?,
            proto: obj["proto"].as_i8()?,
            client_id: obj["client_id"].as_u64()?,
            go: obj["go"].take_string()?,
            nonce: obj["nonce"].take_string().unwrap_or_default(),
            connect_urls: obj["connect_urls"]
                .members_mut()
                .filter_map(|m| m.take_string())
                .collect(),
            client_ip: obj["client_ip"].take_string().unwrap_or_default(),
            headers: obj["headers"].as_bool().unwrap_or(false),
            lame_duck_mode: obj["ldm"].as_bool().unwrap_or(false),
        })
    }
}

/// A NATS connection.
#[derive(Clone, Debug)]
pub struct Connection(pub(crate) Arc<Inner>);

#[derive(Clone, Debug)]
struct Inner {
    client: Client,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.client.shutdown();
    }
}

/// Connect to one or more NATS servers at the given URLs.
///
/// The [`IntoServerList`] trait allows to pass URLs in various different formats. Furthermore, if
/// you need more control of the connection's parameters use [`Options::connect()`].
///
/// **Warning:** There are asynchronous errors that can happen during operation of NATS client.
/// To handle them, add handler for [`Options::error_callback()`].
///
/// # Examples
///
/// If no scheme is provided the `nats://` scheme is assumed. The default port is `4222`.
/// ```no_run
/// let nc = nats::connect("demo.nats.io")?;
/// # Ok::<(), std::io::Error>(())
/// ```
///
/// It is possible to provide several URLs as a comma separated list.
/// ```no_run
/// let nc = nats::connect("demo.nats.io,tls://demo.nats.io:4443")?;
/// # Ok::<(), std::io::Error>(())
/// ```
///
/// Alternatively, an array of strings can be passed.
/// ```no_run
/// # use nats::IntoServerList;
/// let nc = nats::connect(&["demo.nats.io", "tls://demo.nats.io:4443"])?;
/// # Ok::<(), std::io::Error>(())
/// ```
///
/// Instead of using strings, [`ServerAddress`]es can be used directly as well. This is handy for
/// validating user input.
/// ```no_run
/// use nats::ServerAddress;
/// use std::io;
/// use structopt::StructOpt;
///
/// #[derive(Debug, StructOpt)]
/// struct Config {
///     #[structopt(short, long = "server", default_value = "demo.nats.io")]
///     servers: Vec<ServerAddress>,
/// }
///
/// fn main() -> io::Result<()> {
///     let config = Config::from_args();
///     let nc = nats::connect(config.servers)?;
///     Ok(())
/// }
/// ```
pub fn connect<I: IntoServerList>(nats_urls: I) -> io::Result<Connection> {
    Options::new().connect(nats_urls)
}

impl Connection {
    /// Connects on one or more NATS servers with the given options.
    ///
    /// For more on how to use [`IntoServerList`] trait see [`crate::connect()`].
    pub(crate) fn connect_with_options<I>(urls: I, options: Options) -> io::Result<Connection>
    where
        I: IntoServerList,
    {
        let urls = urls.into_server_list()?;
        let client = Client::connect(urls, options)?;
        client.flush(DEFAULT_FLUSH_TIMEOUT)?;
        Ok(Connection(Arc::new(Inner { client })))
    }

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
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.queue_subscribe("foo", "production")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn queue_subscribe(&self, subject: &str, queue: &str) -> io::Result<Subscription> {
        self.do_subscribe(subject, Some(queue))
    }

    /// Publish a message on the given subject.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.publish("foo", "Hello World!")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        self.publish_with_reply_or_headers(subject, None, None, msg)
    }

    /// Publish a message on the given subject with a reply subject for
    /// responses.
    ///
    /// # Example
    /// ```no_run
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
        self.0
            .client
            .publish(subject, Some(reply), None, msg.as_ref())
    }

    /// Create a new globally unique inbox which can be used for replies.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let reply = nc.new_inbox();
    /// let rsub = nc.subscribe(&reply)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_inbox(&self) -> String {
        format!("_INBOX.{}", nuid::next())
    }

    /// Publish a message on the given subject as a request and receive the
    /// response.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// let resp = nc.request("foo", "Help me?")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Message> {
        self.request_with_headers_or_timeout(subject, None, None, msg)
    }

    /// Publish a message on the given subject as a request and receive the
    /// response. This call will return after the timeout duration if no
    /// response is received.
    ///
    /// # Example
    /// ```no_run
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
        self.request_with_headers_or_timeout(subject, None, Some(timeout), msg)
    }

    /// Publish a message with headers on the given subject as a request and receive the
    /// response.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// let mut headers = nats::HeaderMap::new();
    /// headers.insert("X-key", "value".to_string());
    /// let resp = nc.request_with_headers_or_timeout(
    ///     "foo",
    ///     Some(&headers),
    ///     Some(std::time::Duration::from_secs(2)),
    ///     "Help me?",
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_with_headers(
        &self,
        subject: &str,
        msg: impl AsRef<[u8]>,
        headers: &HeaderMap,
    ) -> io::Result<Message> {
        self.request_with_headers_or_timeout(subject, Some(headers), None, msg)
    }

    /// Publish a message on the given subject as a request and receive the
    /// response. This call will return after the timeout duration if it was set to `Some` if no
    /// response is received. It also allows passing headers.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// let mut headers = nats::HeaderMap::new();
    /// headers.insert("X-key", "value".to_string());
    /// let resp = nc.request_with_headers_or_timeout(
    ///     "foo",
    ///     Some(&headers),
    ///     Some(std::time::Duration::from_secs(2)),
    ///     "Help me?",
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_with_headers_or_timeout(
        &self,
        subject: &str,
        maybe_headers: Option<&HeaderMap>,
        maybe_timeout: Option<Duration>,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<Message> {
        // Publish a request.
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply)?;
        self.publish_with_reply_or_headers(subject, Some(reply.as_str()), maybe_headers, msg)?;

        // Wait for the response
        let result = if let Some(timeout) = maybe_timeout {
            sub.next_timeout(timeout)
        } else if let Some(msg) = sub.next() {
            Ok(msg)
        } else {
            Err(ErrorKind::ConnectionReset.into())
        };

        // Check for no responder status.
        if let Ok(msg) = result.as_ref() {
            if msg.is_no_responders() {
                return Err(Error::new(ErrorKind::NotFound, "no responders"));
            }
        }

        result
    }

    /// Publish a message on the given subject as a request and allow multiple
    /// responses.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # nc.subscribe("foo")?.with_handler(move |m| { m.respond("ans=42")?; Ok(()) });
    /// for msg in nc.request_multi("foo", "Help")?.iter().take(1) {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_multi(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Subscription> {
        // Publish a request.
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply)?;
        self.publish_with_reply_or_headers(subject, Some(reply.as_str()), None, msg)?;

        // Return the subscription.
        Ok(sub)
    }

    /// Flush a NATS connection by sending a `PING` protocol and waiting for the
    /// responding `PONG`. Will fail with `TimedOut` if the server does not
    /// respond with in 10 seconds. Will fail with `NotConnected` if the
    /// server is not currently connected. Will fail with `BrokenPipe` if
    /// the connection to the server is lost.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.flush()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush(&self) -> io::Result<()> {
        self.flush_timeout(DEFAULT_FLUSH_TIMEOUT)
    }

    /// Flush a NATS connection by sending a `PING` protocol and waiting for the
    /// responding `PONG`. Will fail with `TimedOut` if the server takes
    /// longer than this duration to respond. Will fail with `NotConnected`
    /// if the server is not currently connected. Will fail with
    /// `BrokenPipe` if the connection to the server is lost.
    ///
    /// # Example
    /// ```no_run
    /// # use std::time::Duration;
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.flush_timeout(Duration::from_secs(5))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush_timeout(&self, duration: Duration) -> io::Result<()> {
        self.0.client.flush(duration)
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
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.close();
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(self) {
        self.0.client.flush(DEFAULT_FLUSH_TIMEOUT).ok();
        self.0.client.close();
    }

    /// Calculates the round trip time between this client and the server,
    /// if the server is currently connected. Fails with `TimedOut` if
    /// the server takes more than 10 seconds to respond.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// println!("server rtt: {:?}", nc.rtt());
    /// # Ok(())
    /// # }
    /// ```
    pub fn rtt(&self) -> io::Result<Duration> {
        let start = Instant::now();
        self.flush()?;
        Ok(start.elapsed())
    }

    /// Returns true if the version is compatible with the version components.
    pub fn is_server_compatible_version(&self, major: i64, minor: i64, patch: i64) -> bool {
        let server_info = self.0.client.server_info();
        let server_version_captures = VERSION_RE.captures(&server_info.version).unwrap();
        let server_major = server_version_captures
            .get(1)
            .map(|m| m.as_str().parse::<i64>().unwrap())
            .unwrap();

        let server_minor = server_version_captures
            .get(2)
            .map(|m| m.as_str().parse::<i64>().unwrap())
            .unwrap();

        let server_patch = server_version_captures
            .get(3)
            .map(|m| m.as_str().parse::<i64>().unwrap())
            .unwrap();

        if server_major < major
            || (server_major == major && server_minor < minor)
            || (server_major == major && server_minor == minor && server_patch < patch)
        {
            return false;
        }

        true
    }

    /// Returns the client IP as known by the server.
    /// Supported as of server version 2.1.6.
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// println!("ip: {:?}", nc.client_ip());
    /// # Ok(())
    /// # }
    /// ```
    pub fn client_ip(&self) -> io::Result<std::net::IpAddr> {
        let info = self.0.client.server_info();

        match info.client_ip.as_str() {
            "" => Err(Error::new(
                ErrorKind::Other,
                &*format!(
                    "client_ip was not provided by the server. It is \
                     supported on servers above version 2.1.6. The server \
                     version is {}",
                    info.version
                ),
            )),
            ip => match ip.parse() {
                Ok(addr) => Ok(addr),
                Err(_) => Err(Error::new(
                    ErrorKind::InvalidData,
                    &*format!(
                        "client_ip provided by the server cannot be parsed. \
                         The server provided IP: {}",
                        info.client_ip
                    ),
                )),
            },
        }
    }

    /// Returns the client ID as known by the most recently connected server.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// println!("ip: {:?}", nc.client_id());
    /// # Ok(())
    /// # }
    /// ```
    pub fn client_id(&self) -> u64 {
        self.0.client.server_info().client_id
    }

    /// Send an unsubscription for all subs then flush the connection, allowing
    /// any unprocessed messages to be handled by a handler function if one
    /// is configured.
    ///
    /// After the flush returns, we know that a round-trip to the server has
    /// happened after it received our unsubscription, so we shut down the
    /// subscriber afterwards.
    ///
    /// A similar method exists for the `Subscription` struct which will drain
    /// a single `Subscription` without shutting down the entire connection
    /// afterward.
    ///
    /// # Example
    /// ```no_run
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
        self.0.client.flush(DEFAULT_FLUSH_TIMEOUT)?;
        self.0.client.close();
        Ok(())
    }

    /// Publish a message which may have a reply subject or headers set.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo.headers")?;
    /// let headers = [("header1", "value1"), ("header2", "value2")]
    ///     .iter()
    ///     .collect();
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
        headers: Option<&HeaderMap>,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        self.0.client.publish(subject, reply, headers, msg.as_ref())
    }

    /// Returns the maximum payload size the most recently
    /// connected server will accept.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::connect("demo.nats.io")?;
    /// println!("max payload: {:?}", nc.max_payload());
    /// # Ok(())
    /// # }
    pub fn max_payload(&self) -> usize {
        self.0.client.server_info.lock().max_payload
    }

    fn do_subscribe(&self, subject: &str, queue: Option<&str>) -> io::Result<Subscription> {
        let (sid, receiver) = self.0.client.subscribe(subject, queue)?;
        Ok(Subscription::new(
            sid,
            subject.to_string(),
            receiver,
            self.0.client.clone(),
        ))
    }

    /// Attempts to publish a message without blocking.
    #[doc(hidden)]
    pub fn try_publish_with_reply_or_headers(
        &self,
        subject: &str,
        reply: Option<&str>,
        headers: Option<&HeaderMap>,
        msg: impl AsRef<[u8]>,
    ) -> Option<io::Result<()>> {
        self.0
            .client
            .try_publish(subject, reply, headers, msg.as_ref())
    }
}
