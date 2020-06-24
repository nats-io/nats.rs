use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::asynk::Connection;
use crate::creds_utils;
use crate::secure_wipe::SecureString;
use crate::tls;

/// Connect options.
pub struct Options {
    pub(crate) auth: AuthStyle,
    pub(crate) name: Option<String>,
    pub(crate) no_echo: bool,
    pub(crate) max_reconnects: Option<usize>,
    pub(crate) reconnect_buffer_size: usize,
    pub(crate) disconnect_callback: Callback,
    pub(crate) reconnect_callback: Callback,
    pub(crate) reconnect_delay_callback: ReconnectDelayCallback,
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
            .entry(&"reconnect_delay_callback", &"set")
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

impl Default for Options {
    fn default() -> Options {
        Options {
            auth: AuthStyle::NoAuth,
            name: None,
            no_echo: false,
            reconnect_buffer_size: 8 * 1024 * 1024,
            max_reconnects: Some(60),
            disconnect_callback: Callback(None),
            reconnect_callback: Callback(None),
            reconnect_delay_callback: ReconnectDelayCallback(Box::new(
                crate::asynk::connector::backoff,
            )),
            close_callback: Callback(None),
            tls_connector: None,
            tls_required: false,
        }
    }
}

impl Options {
    /// Options for establishing a new NATS [`Connection`].
    pub fn new() -> Options {
        Options::default()
    }

    /// Authenticate with NATS using a token.
    pub fn with_token(token: &str) -> Options {
        Options {
            auth: AuthStyle::Token(token.to_string()),
            ..Default::default()
        }
    }

    /// Authenticate with NATS using a username and password.
    pub fn with_user_pass(user: &str, password: &str) -> Options {
        Options {
            auth: AuthStyle::UserPass(user.to_string(), password.to_string()),
            ..Default::default()
        }
    }

    /// Authenticate with NATS using a credentials file.
    pub fn with_credentials(path: impl AsRef<Path>) -> Options {
        Options {
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
            ..Default::default()
        }
    }

    /// Add a name option to this configuration.
    pub fn with_name(mut self, name: &str) -> Options {
        self.name = Some(name.to_string());
        self
    }

    /// Select option to not deliver messages that we have published.
    pub fn no_echo(mut self) -> Options {
        self.no_echo = true;
        self
    }

    /// Sets the maximum number of reconnect attempts.
    ///
    /// If no servers remain that are under this threshold,
    /// all servers will still be attempted.
    pub fn max_reconnects(mut self, max_reconnects: Option<usize>) -> Options {
        self.max_reconnects = max_reconnects;
        self
    }

    /// Sets the maximum amount of bytes to buffer
    /// when accepting outgoing traffic in disconnected
    /// mode.
    ///
    /// The default value is 8mb.
    pub fn reconnect_buffer_size(mut self, reconnect_buffer_size: usize) -> Options {
        self.reconnect_buffer_size = reconnect_buffer_size;
        self
    }

    /// Establishes a `Connection` with a NATS server asynchronously.
    pub async fn connect(self, nats_url: &str) -> io::Result<Connection> {
        Connection::connect_with_options(nats_url, self).await
    }

    /// Sets a callback to be executed when connectivity to
    /// a server has been lost.
    pub fn set_disconnect_callback<F>(mut self, cb: F) -> Options
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.disconnect_callback = Callback(Some(Box::new(cb)));
        self
    }

    /// Sets a callback to be executed when connectivity to a
    /// server has been established.
    pub fn set_reconnect_callback<F>(mut self, cb: F) -> Options
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.disconnect_callback = Callback(Some(Box::new(cb)));
        self
    }

    /// Set a callback to be executed for calculating the backoff duration
    /// to wait before a server reconnection attempt.
    ///
    /// The function takes the number of reconnects as an argument
    /// and returns the `Duration` that should be waited before
    /// making the next connection attempt.
    ///
    /// It is recommended that some random jitter is added to
    /// your returned `Duration`.
    pub fn set_reconnect_delay_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(usize) -> Duration + Send + Sync + 'static,
    {
        self.reconnect_delay_callback = ReconnectDelayCallback(Box::new(cb));
        self
    }

    /// Sets a callback to be executed when the client has been
    /// closed due to exhausting reconnect retries to known servers
    /// or by completing a drain request.
    pub fn set_close_callback<F>(mut self, cb: F) -> Options
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.close_callback = Callback(Some(Box::new(cb)));
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
    pub fn tls_required(mut self, tls_required: bool) -> Options {
        self.tls_required = tls_required;
        self
    }

    /// Allows a particular TLS configuration to be set
    /// for upgrading TCP connections to TLS connections.
    ///
    /// Note that this also enforces that TLS will be
    /// enabled for all connections to all servers.
    pub fn tls_connector(mut self, connector: tls::TlsConnector) -> Options {
        self.tls_connector = Some(connector);
        self.tls_required = true;
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

pub(crate) struct ReconnectDelayCallback(Box<dyn Fn(usize) -> Duration + Send + Sync + 'static>);
