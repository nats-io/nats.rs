use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use crate::creds_utils;
use crate::new_client::{AsyncConnection, Connection};
use crate::secure_wipe::SecureString;
use crate::tls;

pub(crate) struct Options {
    pub(crate) auth: AuthStyle,
    pub(crate) name: Option<String>,
    pub(crate) no_echo: bool,
    pub(crate) max_reconnects: Option<usize>,
    pub(crate) reconnect_buffer_size: usize,
    pub(crate) disconnect_callback: Callback,
    pub(crate) reconnect_callback: Callback,
    pub(crate) close_callback: Callback,
    pub(crate) tls_connector: Option<tls::TlsConnector>,
    pub(crate) tls_required: bool,
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(&"auth", &self.auth)
            .entry(&"name", &self.name)
            .entry(&"no_echo", &self.no_echo)
            .entry(&"reconnect_buffer_size", &self.reconnect_buffer_size)
            .entry(&"max_reconnects", &self.max_reconnects)
            .entry(&"disconnect_callback", &self.disconnect_callback)
            .entry(&"reconnect_callback", &self.reconnect_callback)
            .entry(&"close_callback", &self.close_callback)
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

mod options_typestate {
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
}

/// A configuration object for a NATS connection.
#[derive(Debug)]
pub struct ConnectionOptions<TypeState> {
    options: Options,
    typestate: PhantomData<TypeState>,
}

impl Default for ConnectionOptions<options_typestate::NoAuth> {
    fn default() -> ConnectionOptions<options_typestate::NoAuth> {
        ConnectionOptions {
            options: Options {
                auth: AuthStyle::NoAuth,
                name: None,
                no_echo: false,
                reconnect_buffer_size: 8 * 1024 * 1024,
                max_reconnects: Some(60),
                disconnect_callback: Callback(None),
                reconnect_callback: Callback(None),
                close_callback: Callback(None),
                tls_connector: None,
                tls_required: false,
            },
            typestate: PhantomData,
        }
    }
}

impl ConnectionOptions<options_typestate::NoAuth> {
    /// Options for establishing a new NATS [`Connection`].
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let options = nats::new_client::ConnectionOptions::new();
    /// let nc = options.connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new() -> ConnectionOptions<options_typestate::NoAuth> {
        ConnectionOptions::default()
    }

    /// Authenticate with NATS using a token.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::new_client::ConnectionOptions::new()
    ///     .with_token("t0k3n!")
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_token(self, token: &str) -> ConnectionOptions<options_typestate::Authenticated> {
        ConnectionOptions {
            options: Options {
                auth: AuthStyle::Token(token.to_string()),
                ..self.options
            },
            typestate: PhantomData,
        }
    }

    /// Authenticate with NATS using a username and password.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::new_client::ConnectionOptions::new()
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
            options: Options {
                auth: AuthStyle::UserPass(user.to_string(), password.to_string()),
                ..self.options
            },
            typestate: PhantomData,
        }
    }

    /// Authenticate with NATS using a credentials file.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::new_client::ConnectionOptions::new()
    ///     .with_credentials("path/to/my.creds")
    ///     .connect("connect.ngs.global")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_credentials(
        self,
        path: impl AsRef<Path>,
    ) -> ConnectionOptions<options_typestate::Authenticated> {
        ConnectionOptions {
            options: Options {
                auth: AuthStyle::Credentials {
                    jwt_cb: {
                        let path = path.as_ref().to_owned();
                        Arc::new(move || creds_utils::user_jwt_from_file(&path))
                    },
                    sig_cb: {
                        let path = path.as_ref().to_owned();
                        Arc::new(move |nonce| creds_utils::sign_nonce_with_file(nonce, &path))
                    },
                },
                ..self.options
            },
            typestate: PhantomData,
        }
    }
}

impl<TypeState> ConnectionOptions<TypeState> {
    /// Add a name option to this configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::new_client::ConnectionOptions::new()
    ///     .with_name("My App")
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_name(mut self, name: &str) -> ConnectionOptions<TypeState> {
        self.options.name = Some(name.to_string());
        self
    }

    /// Select option to not deliver messages that we have published.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::new_client::ConnectionOptions::new()
    ///     .no_echo()
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn no_echo(mut self) -> ConnectionOptions<TypeState> {
        self.options.no_echo = true;
        self
    }

    /// Sets the maximum number of reconnect attempts.
    ///
    /// If no servers remain that are under this threshold,
    /// all servers will still be attempted.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::new_client::ConnectionOptions::new()
    ///     .max_reconnects(Some(3))
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn max_reconnects(mut self, max_reconnects: Option<usize>) -> ConnectionOptions<TypeState> {
        self.options.max_reconnects = max_reconnects;
        self
    }

    /// Sets the maximum amount of bytes to buffer
    /// when accepting outgoing traffic in disconnected
    /// mode.
    ///
    /// The default value is 8mb.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::new_client::ConnectionOptions::new()
    ///     .reconnect_buffer_size(64 * 1024)
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reconnect_buffer_size(
        mut self,
        reconnect_buffer_size: usize,
    ) -> ConnectionOptions<TypeState> {
        self.options.reconnect_buffer_size = reconnect_buffer_size;
        self
    }

    /// Establishes a `Connection` with a NATS server.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let options = nats::new_client::ConnectionOptions::new();
    /// let nc = options.connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect(self, nats_url: &str) -> io::Result<Connection> {
        Connection::connect_with_options(nats_url, self.options)
    }

    /// Establishes a `Connection` with a NATS server asynchronously.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let options = nats::new_client::ConnectionOptions::new();
    /// let nc = options.connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_async(self, nats_url: &str) -> io::Result<AsyncConnection> {
        AsyncConnection::connect_with_options(nats_url, self.options).await
    }

    /// Sets a callback to be executed when connectivity to
    /// a server has been lost.
    pub fn set_disconnect_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.options.disconnect_callback = Callback(Some(Box::new(cb)));
        self
    }

    /// Sets a callback to be executed when connectivity to a
    /// server has been established.
    pub fn set_reconnect_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.options.disconnect_callback = Callback(Some(Box::new(cb)));
        self
    }

    /// Sets a callback to be executed when the client has been
    /// closed due to exhausting reconnect retries to known servers
    /// or by completing a drain request.
    pub fn set_close_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.options.close_callback = Callback(Some(Box::new(cb)));
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
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    ///
    /// let nc = nats::new_client::ConnectionOptions::new()
    ///     .tls_required(true)
    ///     .connect("tls://demo.nats.io:4443")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls_required(mut self, tls_required: bool) -> Self {
        self.options.tls_required = tls_required;
        self
    }

    /// Allows a particular TLS configuration to be set
    /// for upgrading TCP connections to TLS connections.
    ///
    /// Note that this also enforces that TLS will be
    /// enabled for all connections to all servers.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut tls_connector = nats::tls::builder()
    ///     .identity(nats::tls::Identity::from_pkcs12(b"der_bytes", "my_password")?)
    ///     .add_root_certificate(nats::tls::Certificate::from_pem(b"my_pem_bytes")?)
    ///     .build()?;
    ///
    /// let nc = nats::ConnectionOptions::new()
    ///     .tls_connector(tls_connector)
    ///     .connect("tls://demo.nats.io:4443")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls_connector(mut self, connector: tls::TlsConnector) -> Self {
        self.options.tls_connector = Some(connector);
        self.options.tls_required = true;
        self
    }
}

#[derive(Clone)]
pub(crate) enum AuthStyle {
    /// Authenticate using a token.
    Token(String),

    /// Authenticate using a username and password.
    UserPass(String, String),

    /// Authenticate using a `.creds` file.
    Credentials {
        /// Securely loads the user JWT.
        jwt_cb: Arc<dyn Fn() -> io::Result<SecureString> + Send + Sync>,
        /// Securely loads the nkey and signs the nonce passed as an argument.
        sig_cb: Arc<dyn Fn(&[u8]) -> io::Result<SecureString> + Send + Sync>,
    },

    /// No authentication.
    NoAuth,
}

impl fmt::Debug for AuthStyle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            AuthStyle::NoAuth => f.debug_struct("NoAuth").finish(),
            AuthStyle::Token(s) => f.debug_tuple("Token").field(s).finish(),
            AuthStyle::UserPass(user, pass) => {
                f.debug_tuple("Token").field(user).field(pass).finish()
            }
            AuthStyle::Credentials { .. } => f.debug_struct("Credentials").finish(),
        }
    }
}

impl Default for AuthStyle {
    fn default() -> AuthStyle {
        AuthStyle::NoAuth
    }
}

#[derive(Default)]
pub(crate) struct Callback(Option<Box<dyn Fn() + Send + Sync + 'static>>);

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
