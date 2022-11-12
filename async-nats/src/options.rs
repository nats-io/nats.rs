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

use crate::{Authorization, Client, Event, ToServerAddrs};
use futures::Future;
use std::fmt::Formatter;
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
    pub(crate) connection_timeout: Duration,
    pub(crate) auth: Authorization,
    pub(crate) tls_required: bool,
    pub(crate) certificates: Vec<PathBuf>,
    pub(crate) client_cert: Option<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) tls_client_config: Option<rustls::ClientConfig>,
    pub(crate) flush_interval: Duration,
    pub(crate) ping_interval: Duration,
    pub(crate) subscription_capacity: usize,
    pub(crate) sender_capacity: usize,
    pub(crate) event_callback: CallbackArg1<Event, ()>,
    pub(crate) inbox_prefix: String,
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) retry_on_initial_connect: bool,
}

impl fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(&"name", &self.name)
            .entry(&"no_echo", &self.no_echo)
            .entry(&"retry_on_failed_connect", &self.retry_on_failed_connect)
            .entry(&"reconnect_buffer_size", &self.reconnect_buffer_size)
            .entry(&"max_reconnects", &self.max_reconnects)
            .entry(&"connection_timeout", &self.connection_timeout)
            .entry(&"tls_required", &self.tls_required)
            .entry(&"certificates", &self.certificates)
            .entry(&"client_cert", &self.client_cert)
            .entry(&"client_key", &self.client_key)
            .entry(&"tls_client_config", &"XXXXXXXX")
            .entry(&"flush_interval", &self.flush_interval)
            .entry(&"ping_interval", &self.ping_interval)
            .entry(&"sender_capacity", &self.sender_capacity)
            .entry(&"inbox_prefix", &self.inbox_prefix)
            .entry(&"retry_on_initial_conenct", &self.retry_on_failed_connect)
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
            connection_timeout: Duration::from_secs(5),
            auth: Authorization::None,
            tls_required: false,
            certificates: Vec::new(),
            client_cert: None,
            client_key: None,
            tls_client_config: None,
            flush_interval: Duration::from_millis(100),
            ping_interval: Duration::from_secs(60),
            sender_capacity: 128,
            subscription_capacity: 1024,
            event_callback: CallbackArg1::<Event, ()>(Box::new(move |event| {
                Box::pin(async move {
                    tracing::info!("event: {}", event);
                })
            })),
            inbox_prefix: "_INBOX".to_string(),
            request_timeout: Some(Duration::from_secs(10)),
            retry_on_initial_connect: false,
        }
    }
}

impl ConnectOptions {
    /// Enables customization of NATS connection.
    ///
    /// # Examples
    /// ```no_run
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
    /// ```no_run
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
    /// let nc = async_nats::ConnectOptions::with_user_and_password("derek".into(), "s3cr3t!".into())
    ///     .connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_user_and_password(user: String, pass: String) -> Self {
        ConnectOptions {
            auth: Authorization::UserAndPassword(user, pass),
            ..Default::default()
        }
    }

    /// Authenticate with a NKey. Requires NKey Seed secret.
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    /// let nc = async_nats::ConnectOptions::with_nkey(seed.into())
    ///     .connect("localhost").await?;
    /// # std::io::Result::Ok(())
    /// # }
    /// ```
    pub fn with_nkey(seed: String) -> Self {
        ConnectOptions {
            auth: Authorization::NKey(seed),
            ..Default::default()
        }
    }

    /// Authenticate with a JWT. Requires function to sign the server nonce.
    /// The signing function is asynchronous
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    /// let key_pair = std::sync::Arc::new(nkeys::KeyPair::from_seed(seed).unwrap());
    /// // load jwt from creds file or other secure source
    /// async fn load_jwt() -> std::io::Result<String> { todo!(); }
    /// let jwt = load_jwt().await?;
    /// let nc = async_nats::ConnectOptions::with_jwt(jwt,
    ///      move |nonce| {
    ///         let key_pair = key_pair.clone();
    ///         async move { key_pair.sign(&nonce).map_err(async_nats::AuthError::new) }})
    ///     .connect("localhost").await?;
    /// # std::io::Result::Ok(())
    /// # }
    /// ```
    pub fn with_jwt<F, Fut>(jwt: String, sign_cb: F) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Vec<u8>, AuthError>> + 'static + Send + Sync,
    {
        let sign_cb = Arc::new(sign_cb);
        ConnectOptions {
            auth: Authorization::Jwt(
                jwt,
                CallbackArg1(Box::new(move |nonce: String| {
                    let sign_cb = sign_cb.clone();
                    Box::pin(async move {
                        let sig = sign_cb(nonce.as_bytes().to_vec())
                            .await
                            .map_err(AuthError::new)?;
                        Ok(base64_url::encode(&sig))
                    })
                })),
            ),
            ..Default::default()
        }
    }

    /// Authenticate with NATS using a `.creds` file.
    /// Open the provided file, load its creds,
    /// and perform the desired authentication
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let nc = async_nats::ConnectOptions::with_credentials_file("path/to/my.creds".into()).await?
    ///     .connect("connect.ngs.global").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_credentials_file(path: PathBuf) -> io::Result<Self> {
        let cred_file_contents = crate::auth_utils::load_creds(path).await?;
        Self::with_credentials(&cred_file_contents)
    }

    /// Authenticate with NATS using a credential str, in the creds file format.
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let creds =
    /// "-----BEGIN NATS USER JWT-----
    /// eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5...
    /// ------END NATS USER JWT------
    ///
    /// ************************* IMPORTANT *************************
    /// NKEY Seed printed below can be used sign and prove identity.
    /// NKEYs are sensitive and should be treated as secrets.
    ///
    /// -----BEGIN USER NKEY SEED-----
    /// SUAIO3FHUX5PNV2LQIIP7TZ3N4L7TX3W53MQGEIVYFIGA635OZCKEYHFLM
    /// ------END USER NKEY SEED------
    /// ";
    ///
    /// let nc = async_nats::ConnectOptions::with_credentials(creds)
    ///     .expect("failed to parse static creds")
    ///     .connect("connect.ngs.global").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_credentials(creds: &str) -> io::Result<Self> {
        let (jwt, key_pair) = crate::auth_utils::parse_jwt_and_key_from_creds(creds)?;
        let key_pair = std::sync::Arc::new(key_pair);
        Ok(Self::with_jwt(jwt, move |nonce| {
            let key_pair = key_pair.clone();
            async move { key_pair.sign(&nonce).map_err(AuthError::new) }
        }))
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
    /// ```no_run
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
    /// ```no_run
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
    /// ```no_run
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

    /// Sets `no_echo` option which disables delivering messages that were published from the same
    /// connection.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().no_echo().connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn no_echo(mut self) -> ConnectOptions {
        self.no_echo = true;
        self
    }

    /// Sets the capacity for `Subscribers`. Exceeding it will trigger `slow consumer` error
    /// callback and drop messages.
    /// Default is set to 1024 messages buffer.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().subscription_capacity(1024).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscription_capacity(mut self, capacity: usize) -> ConnectOptions {
        self.subscription_capacity = capacity;
        self
    }

    /// Sets a timeout for the underlying TcpStream connection to avoid hangs and deadlocks.
    /// Default is set to 5 seconds.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().connection_timeout(tokio::time::Duration::from_secs(5)).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connection_timeout(mut self, timeout: Duration) -> ConnectOptions {
        self.connection_timeout = timeout;
        self
    }

    /// Sets a timeout for `Client::request`. Default value is set to 10 seconds.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().request_timeout(Some(std::time::Duration::from_secs(3))).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_timeout(mut self, timeout: Option<Duration>) -> ConnectOptions {
        self.request_timeout = timeout;
        self
    }

    /// Registers asynchronous callback for errors that are receiver over the wire from the server.
    ///
    /// # Examples
    /// As asynchronous callbacks are still not in `stable` channel, here are some examples how to
    /// work around this
    ///
    /// ## Basic
    /// If you don't need to move anything into the closure, simple signature can be used:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().event_callback(|event| async move {
    ///         println!("event occurred: {}", event);
    /// }).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    ///
    /// ```
    ///
    /// ## Listening to specific event kind
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// async_nats::ConnectOptions::new().event_callback(|event| async move {
    ///     match event {
    ///     async_nats::Event::Disconnected => println!("disconnected"),
    ///         async_nats::Event::Connected => println!("reconnected"),
    ///         async_nats::Event::ClientError(err) => println!("client error occurred: {}", err),
    ///         other => println!("other event happened: {}", other),
    /// }
    /// }).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Advanced
    /// If you need to move something into the closure, here's an example how to do that
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let (tx, mut _rx) = tokio::sync::mpsc::channel(1);
    /// async_nats::ConnectOptions::new().event_callback(move |event| {
    ///     let tx = tx.clone();
    ///     async move {
    ///         tx.send(event).await.unwrap();
    ///         }
    /// }).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn event_callback<F, Fut>(mut self, cb: F) -> ConnectOptions
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static + Send + Sync,
    {
        self.event_callback = CallbackArg1::<Event, ()>(Box::new(move |event| Box::pin(cb(event))));
        self
    }

    /// By default, Client dispatches op's to the Client onto the channel with capacity of 128.
    /// This option enables overriding it.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// async_nats::ConnectOptions::new().client_capacity(256).connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn client_capacity(mut self, capacity: usize) -> ConnectOptions {
        self.sender_capacity = capacity;
        self
    }

    /// Sets custom prefix instead of default `_INBOX`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// async_nats::ConnectOptions::new().custom_inbox_prefix("CUSTOM").connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn custom_inbox_prefix<T: ToString>(mut self, prefix: T) -> ConnectOptions {
        self.inbox_prefix = prefix.to_string();
        self
    }

    /// Sets the name for the client.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// async_nats::ConnectOptions::new().name("rust-service").connect("demo.nats.io").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn name<T: ToString>(mut self, name: T) -> ConnectOptions {
        self.name = Some(name.to_string());
        self
    }

    pub fn retry_on_initial_connect(mut self) -> ConnectOptions {
        self.retry_on_initial_connect = true;
        self
    }
}
type AsyncCallbackArg1<A, T> =
    Box<dyn Fn(A) -> Pin<Box<dyn Future<Output = T> + Send + Sync + 'static>> + Send + Sync>;

pub(crate) struct CallbackArg1<A, T>(AsyncCallbackArg1<A, T>);

impl<A, T> CallbackArg1<A, T> {
    pub(crate) async fn call(&self, arg: A) -> T {
        (self.0.as_ref())(arg).await
    }
}

impl<A, T> fmt::Debug for CallbackArg1<A, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str("callback")
    }
}

/// Error report from signing callback.
// This was needed because std::io::Error isn't Send.
#[derive(Clone)]
pub struct AuthError(String);

impl AuthError {
    pub fn new(s: impl ToString) -> Self {
        Self(s.to_string())
    }
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&format!("AuthError({})", &self.0))
    }
}

impl std::fmt::Debug for AuthError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&format!("AuthError({})", &self.0))
    }
}

impl std::error::Error for AuthError {}
