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

use crate::auth::Auth;
use crate::connector;
use crate::{Client, ConnectError, Event, ToServerAddrs};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::engine::Engine;
use futures::Future;
use std::fmt::Formatter;
use std::{
    fmt,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::io;
use tokio_rustls::rustls;

/// Connect options. Used to connect with NATS when custom config is needed.
/// # Examples
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::ConnectError> {
/// let mut options = async_nats::ConnectOptions::new()
///     .require_tls(true)
///     .ping_interval(std::time::Duration::from_secs(10))
///     .connect("demo.nats.io")
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ConnectOptions {
    pub(crate) name: Option<String>,
    pub(crate) no_echo: bool,
    pub(crate) max_reconnects: Option<usize>,
    pub(crate) connection_timeout: Duration,
    pub(crate) auth: Auth,
    pub(crate) tls_required: bool,
    pub(crate) tls_first: bool,
    pub(crate) certificates: Vec<PathBuf>,
    pub(crate) client_cert: Option<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) tls_client_config: Option<rustls::ClientConfig>,
    pub(crate) ping_interval: Duration,
    pub(crate) subscription_capacity: usize,
    pub(crate) sender_capacity: usize,
    pub(crate) event_callback: Option<CallbackArg1<Event, ()>>,
    pub(crate) inbox_prefix: String,
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) retry_on_initial_connect: bool,
    pub(crate) ignore_discovered_servers: bool,
    pub(crate) retain_servers_order: bool,
    pub(crate) read_buffer_capacity: u16,
    pub(crate) reconnect_delay_callback: Box<dyn Fn(usize) -> Duration + Send + Sync + 'static>,
    pub(crate) auth_callback: Option<CallbackArg1<Vec<u8>, Result<Auth, AuthError>>>,
}

impl fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(&"name", &self.name)
            .entry(&"no_echo", &self.no_echo)
            .entry(&"max_reconnects", &self.max_reconnects)
            .entry(&"connection_timeout", &self.connection_timeout)
            .entry(&"tls_required", &self.tls_required)
            .entry(&"certificates", &self.certificates)
            .entry(&"client_cert", &self.client_cert)
            .entry(&"client_key", &self.client_key)
            .entry(&"tls_client_config", &"XXXXXXXX")
            .entry(&"tls_first", &self.tls_first)
            .entry(&"ping_interval", &self.ping_interval)
            .entry(&"sender_capacity", &self.sender_capacity)
            .entry(&"inbox_prefix", &self.inbox_prefix)
            .entry(&"retry_on_initial_connect", &self.retry_on_initial_connect)
            .entry(&"read_buffer_capacity", &self.read_buffer_capacity)
            .finish()
    }
}

impl Default for ConnectOptions {
    fn default() -> ConnectOptions {
        ConnectOptions {
            name: None,
            no_echo: false,
            max_reconnects: None,
            connection_timeout: Duration::from_secs(5),
            tls_required: false,
            tls_first: false,
            certificates: Vec::new(),
            client_cert: None,
            client_key: None,
            tls_client_config: None,
            ping_interval: Duration::from_secs(60),
            sender_capacity: 2048,
            subscription_capacity: 1024 * 64,
            event_callback: None,
            inbox_prefix: "_INBOX".to_string(),
            request_timeout: Some(Duration::from_secs(10)),
            retry_on_initial_connect: false,
            ignore_discovered_servers: false,
            retain_servers_order: false,
            read_buffer_capacity: 65535,
            reconnect_delay_callback: Box::new(|attempts| {
                connector::reconnect_delay_callback_default(attempts)
            }),
            auth: Default::default(),
            auth_callback: None,
        }
    }
}

impl ConnectOptions {
    /// Enables customization of NATS connection.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let mut options = async_nats::ConnectOptions::new()
    ///     .require_tls(true)
    ///     .ping_interval(std::time::Duration::from_secs(10))
    ///     .connect("demo.nats.io")
    ///     .await?;
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
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::new()
    ///     .require_tls(true)
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Pass multiple URLs.
    /// ```no_run
    /// #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::ServerAddr;
    /// let client = async_nats::connect(vec![
    ///     "demo.nats.io".parse::<ServerAddr>()?,
    ///     "other.nats.io".parse::<ServerAddr>()?,
    /// ])
    /// .await
    /// .unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect<A: ToServerAddrs>(self, addrs: A) -> Result<Client, ConnectError> {
        crate::connect_with_options(addrs, self).await
    }

    /// Creates a builder with a custom auth callback to be used when authenticating against the NATS Server.
    /// Requires an asynchronous function that accepts nonce and returns [Auth].
    /// It will overwrite all other auth methods used.
    ///
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// async_nats::ConnectOptions::with_auth_callback(move |_| async move {
    ///     let mut auth = async_nats::Auth::new();
    ///     auth.username = Some("derek".to_string());
    ///     auth.password = Some("s3cr3t".to_string());
    ///     Ok(auth)
    /// })
    /// .connect("demo.nats.io")
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_auth_callback<F, Fut>(callback: F) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Auth, AuthError>> + 'static + Send + Sync,
    {
        let mut options = ConnectOptions::new();
        options.auth_callback = Some(CallbackArg1::<Vec<u8>, Result<Auth, AuthError>>(Box::new(
            move |nonce| Box::pin(callback(nonce)),
        )));
        options
    }

    /// Authenticate against NATS Server with the provided token.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::with_token("t0k3n!".into())
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_token(token: String) -> Self {
        ConnectOptions::default().token(token)
    }

    /// Use a builder to specify a token, to be used when authenticating against the NATS Server.
    /// This can be used as a way to mix authentication methods.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::new()
    ///     .token("t0k3n!".into())
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn token(mut self, token: String) -> Self {
        self.auth.token = Some(token);
        self
    }

    /// Authenticate against NATS Server with the provided username and password.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::with_user_and_password("derek".into(), "s3cr3t!".into())
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_user_and_password(user: String, pass: String) -> Self {
        ConnectOptions::default().user_and_password(user, pass)
    }

    /// Use a builder to specify a username and password, to be used when authenticating against the NATS Server.
    /// This can be used as a way to mix authentication methods.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::new()
    ///     .user_and_password("derek".into(), "s3cr3t!".into())
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn user_and_password(mut self, user: String, pass: String) -> Self {
        self.auth.username = Some(user);
        self.auth.password = Some(pass);
        self
    }

    /// Authenticate with an NKey. Requires an NKey Seed secret.
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    /// let nc = async_nats::ConnectOptions::with_nkey(seed.into())
    ///     .connect("localhost")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_nkey(seed: String) -> Self {
        ConnectOptions::default().nkey(seed)
    }

    /// Use a builder to specify an NKey, to be used when authenticating against the NATS Server.
    /// Requires an NKey Seed Secret.
    /// This can be used as a way to mix authentication methods.
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    /// let nc = async_nats::ConnectOptions::new()
    ///     .nkey(seed.into())
    ///     .connect("localhost")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn nkey(mut self, seed: String) -> Self {
        self.auth.nkey = Some(seed);
        self
    }

    /// Authenticate with a JWT. Requires function to sign the server nonce.
    /// The signing function is asynchronous.
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    /// let key_pair = std::sync::Arc::new(nkeys::KeyPair::from_seed(seed).unwrap());
    /// // load jwt from creds file or other secure source
    /// async fn load_jwt() -> std::io::Result<String> {
    ///     todo!();
    /// }
    /// let jwt = load_jwt().await?;
    /// let nc = async_nats::ConnectOptions::with_jwt(jwt, move |nonce| {
    ///     let key_pair = key_pair.clone();
    ///     async move { key_pair.sign(&nonce).map_err(async_nats::AuthError::new) }
    /// })
    /// .connect("localhost")
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_jwt<F, Fut>(jwt: String, sign_cb: F) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Vec<u8>, AuthError>> + 'static + Send + Sync,
    {
        ConnectOptions::default().jwt(jwt, sign_cb)
    }

    /// Use a builder to specify a JWT, to be used when authenticating against the NATS Server.
    /// Requires an asynchronous function to sign the server nonce.
    /// This can be used as a way to mix authentication methods.
    ///
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    /// let key_pair = std::sync::Arc::new(nkeys::KeyPair::from_seed(seed).unwrap());
    /// // load jwt from creds file or other secure source
    /// async fn load_jwt() -> std::io::Result<String> {
    ///     todo!();
    /// }
    /// let jwt = load_jwt().await?;
    /// let nc = async_nats::ConnectOptions::new()
    ///     .jwt(jwt, move |nonce| {
    ///         let key_pair = key_pair.clone();
    ///         async move { key_pair.sign(&nonce).map_err(async_nats::AuthError::new) }
    ///     })
    ///     .connect("localhost")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn jwt<F, Fut>(mut self, jwt: String, sign_cb: F) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Vec<u8>, AuthError>> + 'static + Send + Sync,
    {
        let sign_cb = Arc::new(sign_cb);

        let jwt_sign_callback = CallbackArg1(Box::new(move |nonce: String| {
            let sign_cb = sign_cb.clone();
            Box::pin(async move {
                let sig = sign_cb(nonce.as_bytes().to_vec())
                    .await
                    .map_err(AuthError::new)?;
                Ok(URL_SAFE_NO_PAD.encode(sig))
            })
        }));

        self.auth.jwt = Some(jwt);
        self.auth.signature_callback = Some(jwt_sign_callback);
        self
    }

    /// Authenticate with NATS using a `.creds` file.
    /// Open the provided file, load its creds,
    /// and perform the desired authentication
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::with_credentials_file("path/to/my.creds")
    ///     .await?
    ///     .connect("connect.ngs.global")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_credentials_file(path: impl AsRef<Path>) -> io::Result<Self> {
        let cred_file_contents = crate::auth_utils::load_creds(path.as_ref()).await?;
        Self::with_credentials(&cred_file_contents)
    }

    /// Use a builder to specify a credentials file, to be used when authenticating against the NATS Server.
    /// This will open the credentials file and load its credentials.
    /// This can be used as a way to mix authentication methods.
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::new()
    ///     .credentials_file("path/to/my.creds")
    ///     .await?
    ///     .connect("connect.ngs.global")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn credentials_file(self, path: impl AsRef<Path>) -> io::Result<Self> {
        let cred_file_contents = crate::auth_utils::load_creds(path.as_ref()).await?;
        self.credentials(&cred_file_contents)
    }

    /// Authenticate with NATS using a credential str, in the creds file format.
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let creds = "-----BEGIN NATS USER JWT-----
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
    ///     .connect("connect.ngs.global")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_credentials(creds: &str) -> io::Result<Self> {
        ConnectOptions::default().credentials(creds)
    }

    /// Use a builder to specify a credentials string, to be used when authenticating against the NATS Server.
    /// The string should be in the credentials file format.
    /// This can be used as a way to mix authentication methods.
    ///
    /// # Example
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let creds = "-----BEGIN NATS USER JWT-----
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
    /// let nc = async_nats::ConnectOptions::new()
    ///     .credentials(creds)
    ///     .expect("failed to parse static creds")
    ///     .connect("connect.ngs.global")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn credentials(self, creds: &str) -> io::Result<Self> {
        let (jwt, key_pair) = crate::auth_utils::parse_jwt_and_key_from_creds(creds)?;
        let key_pair = std::sync::Arc::new(key_pair);

        Ok(self.jwt(jwt.to_owned(), move |nonce| {
            let key_pair = key_pair.clone();
            async move { key_pair.sign(&nonce).map_err(AuthError::new) }
        }))
    }

    /// Loads root certificates by providing the path to them.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::new()
    ///     .add_root_certificates("mycerts.pem".into())
    ///     .connect("demo.nats.io")
    ///     .await?;
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
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::new()
    ///     .add_client_certificate("cert.pem".into(), "key.pem".into())
    ///     .connect("demo.nats.io")
    ///     .await?;
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
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// let nc = async_nats::ConnectOptions::new()
    ///     .require_tls(true)
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn require_tls(mut self, is_required: bool) -> ConnectOptions {
        self.tls_required = is_required;
        self
    }

    /// Changes how tls connection is established. If `tls_first` is set,
    /// client will try to establish tls before getting info from the server.
    /// That requires the server to enable `handshake_first` option in the config.
    pub fn tls_first(mut self) -> ConnectOptions {
        self.tls_first = true;
        self.tls_required = true;
        self
    }

    /// Sets how often Client sends PING message to the server.
    ///
    /// # Examples
    /// ```no_run
    /// # use tokio::time::Duration;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// async_nats::ConnectOptions::new()
    ///     .ping_interval(Duration::from_secs(24))
    ///     .connect("demo.nats.io")
    ///     .await?;
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
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// async_nats::ConnectOptions::new()
    ///     .no_echo()
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn no_echo(mut self) -> ConnectOptions {
        self.no_echo = true;
        self
    }

    /// Sets the capacity for `Subscribers`. Exceeding it will trigger `slow consumer` error
    /// callback and drop messages.
    /// Default is set to 65536 messages buffer.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// async_nats::ConnectOptions::new()
    ///     .subscription_capacity(1024)
    ///     .connect("demo.nats.io")
    ///     .await?;
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
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// async_nats::ConnectOptions::new()
    ///     .connection_timeout(tokio::time::Duration::from_secs(5))
    ///     .connect("demo.nats.io")
    ///     .await?;
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
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// async_nats::ConnectOptions::new()
    ///     .request_timeout(Some(std::time::Duration::from_secs(3)))
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_timeout(mut self, timeout: Option<Duration>) -> ConnectOptions {
        self.request_timeout = timeout;
        self
    }

    /// Registers an asynchronous callback for errors that are received over the wire from the server.
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
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// async_nats::ConnectOptions::new()
    ///     .event_callback(|event| async move {
    ///         println!("event occurred: {}", event);
    ///     })
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Listening to specific event kind
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// async_nats::ConnectOptions::new()
    ///     .event_callback(|event| async move {
    ///         match event {
    ///             async_nats::Event::Disconnected => println!("disconnected"),
    ///             async_nats::Event::Connected => println!("reconnected"),
    ///             async_nats::Event::ClientError(err) => println!("client error occurred: {}", err),
    ///             other => println!("other event happened: {}", other),
    ///         }
    ///     })
    ///     .connect("demo.nats.io")
    ///     .await?;
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
    /// async_nats::ConnectOptions::new()
    ///     .event_callback(move |event| {
    ///         let tx = tx.clone();
    ///         async move {
    ///             tx.send(event).await.unwrap();
    ///         }
    ///     })
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn event_callback<F, Fut>(mut self, cb: F) -> ConnectOptions
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static + Send + Sync,
    {
        self.event_callback = Some(CallbackArg1::<Event, ()>(Box::new(move |event| {
            Box::pin(cb(event))
        })));
        self
    }

    /// Registers a callback for a custom reconnect delay handler that can be used to define a backoff duration strategy.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::ConnectError> {
    /// async_nats::ConnectOptions::new()
    ///     .reconnect_delay_callback(|attempts| {
    ///         println!("no of attempts: {attempts}");
    ///         std::time::Duration::from_millis(std::cmp::min((attempts * 100) as u64, 8000))
    ///     })
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reconnect_delay_callback<F>(mut self, cb: F) -> ConnectOptions
    where
        F: Fn(usize) -> Duration + Send + Sync + 'static,
    {
        self.reconnect_delay_callback = Box::new(cb);
        self
    }

    /// By default, Client dispatches op's to the Client onto the channel with capacity of 128.
    /// This option enables overriding it.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// async_nats::ConnectOptions::new()
    ///     .client_capacity(256)
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// async_nats::ConnectOptions::new()
    ///     .custom_inbox_prefix("CUSTOM")
    ///     .connect("demo.nats.io")
    ///     .await?;
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
    /// async_nats::ConnectOptions::new()
    ///     .name("rust-service")
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn name<T: ToString>(mut self, name: T) -> ConnectOptions {
        self.name = Some(name.to_string());
        self
    }

    /// By default, [`ConnectOptions::connect`] will return an error if
    /// the connection to the server cannot be established.
    ///
    /// Setting `retry_on_initial_connect` makes the client
    /// establish the connection in the background.
    pub fn retry_on_initial_connect(mut self) -> ConnectOptions {
        self.retry_on_initial_connect = true;
        self
    }

    /// Specifies the number of consecutive reconnect attempts the client will
    /// make before giving up. This is useful for preventing zombie services
    /// from endlessly reaching the servers, but it can also be a footgun and
    /// surprise for users who do not expect that the client can give up
    /// entirely.
    ///
    /// Pass `None` or `0` for no limit.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// async_nats::ConnectOptions::new()
    ///     .max_reconnects(None)
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn max_reconnects<T: Into<Option<usize>>>(mut self, max_reconnects: T) -> ConnectOptions {
        let val: Option<usize> = max_reconnects.into();
        self.max_reconnects = if val == Some(0) { None } else { val };
        self
    }

    /// By default, a server may advertise other servers in the cluster known to it.
    /// By setting this option, the client will ignore the advertised servers.
    /// This may be useful if the client may not be able to reach them.
    pub fn ignore_discovered_servers(mut self) -> ConnectOptions {
        self.ignore_discovered_servers = true;
        self
    }

    /// By default, client will pick random server to which it will try connect to.
    /// This option disables that feature, forcing it to always respect the order
    /// in which server addresses were passed.
    pub fn retain_servers_order(mut self) -> ConnectOptions {
        self.retain_servers_order = true;
        self
    }

    /// Allows passing custom rustls tls config.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let mut root_store = async_nats::rustls::RootCertStore::empty();
    ///
    /// root_store.add_parsable_certificates(rustls_native_certs::load_native_certs()?);
    ///
    /// let tls_client = async_nats::rustls::ClientConfig::builder()
    ///     .with_root_certificates(root_store)
    ///     .with_no_client_auth();
    ///
    /// let client = async_nats::ConnectOptions::new()
    ///     .require_tls(true)
    ///     .tls_client_config(tls_client)
    ///     .connect("tls://demo.nats.io")
    ///     .await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls_client_config(mut self, config: rustls::ClientConfig) -> ConnectOptions {
        self.tls_client_config = Some(config);
        self
    }

    /// Sets the initial capacity of the read buffer. Which is a buffer used to gather partial
    /// protocol messages.
    ///
    /// # Examples
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// async_nats::ConnectOptions::new()
    ///     .read_buffer_capacity(65535)
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_buffer_capacity(mut self, size: u16) -> ConnectOptions {
        self.read_buffer_capacity = size;
        self
    }
}

pub(crate) type AsyncCallbackArg1<A, T> =
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
#[derive(Clone, PartialEq)]
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
