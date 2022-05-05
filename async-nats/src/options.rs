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

use crate::{Authorization, Client, ServerError, ToServerAddrs};
use futures::Future;
use std::{fmt, path::PathBuf, pin::Pin, sync::Arc, time::Duration};
use tokio::io;
use tokio_rustls::rustls;

/// Connect options. Used to connect with NATS when custom config is needed.
/// # Examples
/// ```
/// # #[tokio::main]
/// # async fn options() -> std::io::Result<()> {
/// let mut options =
/// async_nats::ConnectOptions::new()
///     .require_tls(true)
///     .ping_interval(std::time::Duration::from_secs(10))
///     .connect("demo.nats.io").await?;
/// # Ok(())
/// # }
/// ```
pub struct ConnectOptions {
    // pub(crate) auth: AuthStyle,
    pub(crate) name: Option<String>,
    pub(crate) no_echo: bool,
    pub(crate) retry_on_failed_connect: bool,
    pub(crate) max_reconnects: Option<usize>,
    pub(crate) reconnect_buffer_size: usize,
    pub(crate) auth: Authorization,
    pub(crate) tls_required: bool,
    pub(crate) certificates: Vec<PathBuf>,
    pub(crate) client_cert: Option<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) tls_client_config: Option<rustls::ClientConfig>,
    pub(crate) flush_interval: Duration,
    pub(crate) ping_interval: Duration,
    pub(crate) reconnect_callback: Callback,
    pub(crate) disconnect_callback: Callback,
    pub(crate) error_callback: ErrorCallback,
}

impl fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(&"name", &self.name)
            .entry(&"no_echo", &self.no_echo)
            .entry(&"retry_on_failed_connect", &self.retry_on_failed_connect)
            .entry(&"reconnect_buffer_size", &self.reconnect_buffer_size)
            .entry(&"max_reconnects", &self.max_reconnects)
            .entry(&"tls_required", &self.tls_required)
            .entry(&"certificates", &self.certificates)
            .entry(&"client_cert", &self.client_cert)
            .entry(&"client_key", &self.client_key)
            .entry(&"tls_client_config", &"XXXXXXXX")
            .entry(&"flush_interval", &self.flush_interval)
            .entry(&"ping_interval", &self.ping_interval)
            .finish()
    }
}

impl Default for ConnectOptions {
    fn default() -> ConnectOptions {
        ConnectOptions {
            name: None,
            no_echo: false,
            retry_on_failed_connect: false,
            reconnect_buffer_size: 8 * 1024 * 1024,
            max_reconnects: Some(60),
            auth: Authorization::None,
            tls_required: false,
            certificates: Vec::new(),
            client_cert: None,
            client_key: None,
            tls_client_config: None,
            flush_interval: Duration::from_millis(100),
            ping_interval: Duration::from_secs(60),
            reconnect_callback: Callback(None),
            disconnect_callback: Callback(None),
            error_callback: ErrorCallback(None),
        }
    }
}

impl ConnectOptions {
    /// Enables customization of NATS connection.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn options() -> std::io::Result<()> {
    /// let mut options =
    /// async_nats::ConnectOptions::new()
    ///     .require_tls(true)
    ///     .ping_interval(std::time::Duration::from_secs(10))
    ///     .connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new() -> ConnectOptions {
        ConnectOptions::default()
    }

    /// Connect to the NATS Server leveraging all passed options.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let nc = async_nats::ConnectOptions::new().require_tls(true).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect<A: ToServerAddrs>(self, addrs: A) -> io::Result<Client> {
        crate::connect_with_options(addrs, self).await
    }

    /// Auth against NATS Server with provided token.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let nc =
    /// async_nats::ConnectOptions::with_token("t0k3n!".into()).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_token(token: String) -> Self {
        ConnectOptions {
            auth: Authorization::Token(token),
            ..Default::default()
        }
    }

    /// Auth against NATS Server with provided username and password.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let nc = async_nats::ConnectOptions::with_user_and_password("derek".into(), "s3cr3t!".into()).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_user_and_password(user: String, pass: String) -> Self {
        ConnectOptions {
            auth: Authorization::UserAndPassword(user, pass),
            ..Default::default()
        }
    }

    /// Authenticate with a JWT. Requires function to sign the server nonce.
    /// The signing function is synchronous and should not do any blocking io.
    ///
    /// # Example
    /// ```no_run
    /// let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    /// let kp = nkeys::KeyPair::from_seed(seed).unwrap();
    /// // load jwt from creds file or other secure source
    /// async fn load_jwt() -> std::io::Result<String> { todo!(); }
    /// let jwt = load_jwt().await?;
    ///
    /// let nc = async_nats::ConnectOptions::with_jwt(jwt, move |nonce| kp.sign(nonce).unwrap())
    ///     .connect("localhost")?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn with_jwt<S>(jwt: String, sig_cb: S) -> Self
    where
        //J: Fn() -> futures::future::BoxFuture<String> + Send + Sync + 'static,
        S: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        ConnectOptions {
            auth: Authorization::Jwt(
                jwt,
                Arc::new(move |nonce| Ok(base64_url::encode(&sig_cb(nonce)))),
            ),
            ..Default::default()
        }
    }

    /// Loads root certificates by providing the path to them.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let nc =
    /// async_nats::ConnectOptions::new().add_root_certificates("mycerts.pem".into()).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_root_certificates(mut self, path: PathBuf) -> ConnectOptions {
        self.certificates = vec![path];
        self
    }

    /// Loads client certificate by providing the path to it.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let nc =
    /// async_nats::ConnectOptions::new().add_client_certificate("cert.pem".into(), "key.pem".into()).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_client_certificate(mut self, cert: PathBuf, key: PathBuf) -> ConnectOptions {
        self.client_cert = Some(cert);
        self.client_key = Some(key);
        self
    }

    /// Sets or disables TLS requirement. If TLS connection is impossible while `options.require_tls(true)` connection will return error.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let nc =
    /// async_nats::ConnectOptions::new().require_tls(true).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn require_tls(mut self, is_required: bool) -> ConnectOptions {
        self.tls_required = is_required;
        self
    }

    /// Sets the interval for flushing. NATS connection will send buffered data to the NATS Server
    /// whenever buffer limit is reached, but it is also necessary to flush once in a while if
    /// client is sending rarely and small messages. Flush interval allows to modify that interval.
    ///
    /// # Examples
    /// ```
    /// # use tokio::time::Duration;
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().flush_interval(Duration::from_millis(100)).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush_interval(mut self, flush_interval: Duration) -> ConnectOptions {
        self.flush_interval = flush_interval;
        self
    }

    /// Sets how often Client sends PING message to the server.
    ///
    /// # Examples
    /// ```
    /// # use tokio::time::Duration;
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().flush_interval(Duration::from_millis(100)).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn ping_interval(mut self, ping_interval: Duration) -> ConnectOptions {
        self.ping_interval = ping_interval;
        self
    }

    /// Registers asynchronous callback for errors that are receiver over the wire from the server.
    ///
    /// # Examples
    /// As asynchronous callbacks are stil not in `stable` channel, here are some examples how to
    /// work around this
    ///
    /// ## Basic
    /// If you don't need to move anything into the closure, simple signature can be used:
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().error_callback(|error| async move {
    /// println!("error occured: {}", error);
    /// }).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Advanced
    /// If you need to move something into the closure, here's an example how to do that
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let (tx, mut _rx) = tokio::sync::mpsc::channel(1);
    /// async_nats::ConnectOptions::new().error_callback(move |error| {
    ///     let tx = tx.clone();
    ///     async move {
    ///         tx.send(error).await.unwrap();
    ///         }
    /// }).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn error_callback<F, Fut>(mut self, cb: F) -> ConnectOptions
    where
        F: Fn(ServerError) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static + Send + Sync,
    {
        self.error_callback = ErrorCallback(Some(Box::new(move |error| Box::pin(cb(error)))));
        self
    }

    /// Registers asynchronous callback for reconnection events.
    ///
    /// # Examples
    /// As asynchronous callbacks are stil not in `stable` channel, here are some examples how to
    /// work around this
    ///
    /// ## Basic
    /// If you don't need to move anything into the closure, simple signature can be used:
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().reconnect_callback(|| async {
    /// println!("reconnected");
    /// }).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Advanced
    /// If you need to move something into the closure, here's an example how to do that
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let (tx, mut _rx) = tokio::sync::mpsc::channel(1);
    /// async_nats::ConnectOptions::new().reconnect_callback(move || {
    ///     let tx = tx.clone();
    ///     async move {
    ///         tx.send("reconnected").await.unwrap();
    ///         }
    /// }).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reconnect_callback<F, Fut>(mut self, cb: F) -> ConnectOptions
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static + Send + Sync,
    {
        self.reconnect_callback = Callback(Some(Box::new(move || Box::pin(cb()))));
        self
    }

    /// Registers asynchronous callback for disconection events.
    ///
    /// # Examples
    /// As asynchronous callbacks are stil not in `stable` channel, here are some examples how to
    /// work around this
    ///
    /// ## Basic
    /// If you don't need to move anything into the closure, simple signature can be used:
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().disconnect_callback(|| async {
    /// println!("disconnected");
    /// }).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Advanced
    /// If you need to move something into the closure, here's an example how to do that
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let (tx, mut _rx) = tokio::sync::mpsc::channel(1);
    /// async_nats::ConnectOptions::new().disconnect_callback(move || {
    ///     let tx = tx.clone();
    ///     async move {
    ///         tx.send("disconnected").await.unwrap();
    ///         }
    /// }).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn disconnect_callback<F, Fut>(mut self, cb: F) -> ConnectOptions
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static + Send + Sync,
    {
        self.disconnect_callback = Callback(Some(Box::new(move || Box::pin(cb()))));
        self
    }
}

type AsyncCallback =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
type AsyncErrorCallback =
    Box<dyn Fn(ServerError) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

#[derive(Default)]
pub(crate) struct Callback(Option<AsyncCallback>);
impl Callback {
    pub async fn call(&self) {
        if let Some(callback) = self.0.as_ref() {
            callback().await
        }
    }
}

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

#[derive(Default)]
pub(crate) struct ErrorCallback(Option<AsyncErrorCallback>);

impl ErrorCallback {
    pub async fn call(&self, err: ServerError) {
        if let Some(callback) = self.0.as_ref() {
            callback(err).await
        } else {
            println!("error returned from server: {}", err);
        }
    }
}

impl fmt::Debug for ErrorCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(
                &"callback",
                if self.0.is_some() { &"set" } else { &"unset" },
            )
            .finish()
    }
}
