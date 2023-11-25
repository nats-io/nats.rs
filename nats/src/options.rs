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

use std::cmp;
use std::convert::TryInto;
use std::fmt;
use std::io;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::auth_utils;
use crate::secure_wipe::SecureString;
use crate::Client;
use crate::Connection;
use crate::IntoServerList;

/// Connect options.
pub struct Options {
    pub(crate) auth: AuthStyle,
    pub(crate) name: Option<String>,
    pub(crate) no_echo: bool,
    pub(crate) retry_on_failed_connect: bool,
    pub(crate) max_reconnects: Option<usize>,
    pub(crate) reconnect_buffer_size: usize,
    pub(crate) tls_required: bool,
    pub(crate) certificates: Vec<PathBuf>,
    pub(crate) client_cert: Option<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) tls_client_config: Option<crate::rustls::ClientConfig>,

    pub(crate) error_callback: ErrorCallback,
    pub(crate) disconnect_callback: Callback,
    pub(crate) reconnect_callback: Callback,
    pub(crate) reconnect_delay_callback: ReconnectDelayCallback,
    pub(crate) close_callback: Callback,
    pub(crate) lame_duck_callback: Callback,
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(&"auth", &self.auth)
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
            .entry(&"error_callback", &self.error_callback)
            .entry(&"disconnect_callback", &self.disconnect_callback)
            .entry(&"reconnect_callback", &self.reconnect_callback)
            .entry(&"reconnect_delay_callback", &"set")
            .entry(&"close_callback", &self.close_callback)
            .entry(&"lame_duck_callback", &self.lame_duck_callback)
            .finish()
    }
}

impl Default for Options {
    fn default() -> Options {
        Options {
            auth: AuthStyle::NoAuth,
            name: None,
            no_echo: false,
            retry_on_failed_connect: false,
            reconnect_buffer_size: 8 * 1024 * 1024,
            max_reconnects: Some(60),
            tls_required: false,
            certificates: Vec::new(),
            client_cert: None,
            client_key: None,
            error_callback: ErrorCallback(None),
            disconnect_callback: Callback(None),
            reconnect_callback: Callback(None),
            reconnect_delay_callback: ReconnectDelayCallback(Box::new(backoff)),
            close_callback: Callback(None),
            lame_duck_callback: Callback(None),
            tls_client_config: None,
        }
    }
}

/// Calculates how long to sleep for before connecting to a server.
pub(crate) fn backoff(reconnects: usize) -> Duration {
    // Exponential backoff: 0ms, 1ms, 2ms, 4ms, 8ms, 16ms, ..., 4sec
    let base = if reconnects == 0 {
        Duration::from_millis(0)
    } else {
        let exp: u32 = (reconnects - 1).try_into().unwrap_or(std::u32::MAX);

        let max = if cfg!(feature = "fault_injection") {
            Duration::from_millis(20)
        } else {
            Duration::from_secs(4)
        };

        cmp::min(Duration::from_millis(2_u64.saturating_pow(exp)), max)
    };

    // Add some random jitter.
    let max_jitter = if cfg!(feature = "fault_injection") {
        10
    } else {
        1000
    };

    let jitter = Duration::from_millis(fastrand::u64(0..max_jitter));

    base + jitter
}

impl Options {
    /// `Options` for establishing a new NATS `Connection`.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let options = nats::Options::new();
    /// let nc = options.connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new() -> Options {
        Options::default()
    }

    /// Authenticate with NATS using a token.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::with_token("t0k3n!").connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_token(token: &str) -> Options {
        Options {
            auth: AuthStyle::Token(token.to_string()),
            ..Default::default()
        }
    }

    /// Authenticate with NATS using a username and password.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::with_user_pass("derek", "s3cr3t!").connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_user_pass(user: &str, password: &str) -> Options {
        Options {
            auth: AuthStyle::UserPass(user.to_string(), password.to_string()),
            ..Default::default()
        }
    }

    /// Authenticate with NATS using a `.creds` file.
    ///
    /// This will open the provided file, load its creds,
    /// perform the desired authentication, and then zero
    /// the memory used to store the creds before continuing.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::with_credentials("path/to/my.creds").connect("connect.ngs.global")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_credentials(path: impl AsRef<Path>) -> Options {
        Options {
            auth: AuthStyle::Credentials {
                jwt_cb: {
                    let path = path.as_ref().to_owned();
                    Arc::new(move || {
                        let (jwt, _kp) = auth_utils::load_creds(&path)?;
                        Ok(jwt)
                    })
                },
                sig_cb: {
                    let path = path.as_ref().to_owned();
                    Arc::new(move |nonce| {
                        let (_jwt, kp) = auth_utils::load_creds(&path)?;
                        auth_utils::sign_nonce(nonce, &kp)
                    })
                },
            },
            ..Default::default()
        }
    }

    /// Authenticate with NATS using a static credential str, using
    /// the creds file format. Note that this is more hazardous than
    /// using the above `with_credentials` method because it retains
    /// the secret in-memory for the lifetime of this client instead
    /// of zeroing the credentials after holding them for a very short
    /// time, as the `with_credentials` method does.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
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
    /// let nc = nats::Options::with_static_credentials(creds)
    ///     .expect("failed to parse static creds")
    ///     .connect("connect.ngs.global")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_static_credentials(creds: &str) -> io::Result<Options> {
        let (jwt, kp) = auth_utils::jwt_kp(creds)?;
        Ok(Options {
            auth: AuthStyle::Credentials {
                jwt_cb: { Arc::new(move || Ok(jwt.clone())) },
                sig_cb: { Arc::new(move |nonce| auth_utils::sign_nonce(nonce, &kp)) },
            },
            ..Default::default()
        })
    }

    /// Authenticate with a function that loads user JWT and a signature
    /// function.
    ///
    /// # Example
    /// ```no_run
    /// let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    /// let kp = nkeys::KeyPair::from_seed(seed).unwrap();
    ///
    /// fn load_jwt() -> std::io::Result<String> {
    ///     todo!()
    /// }
    ///
    /// let nc = nats::Options::with_jwt(load_jwt, move |nonce| kp.sign(nonce).unwrap())
    ///     .connect("localhost")?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn with_jwt<J, S>(jwt_cb: J, sig_cb: S) -> Options
    where
        J: Fn() -> io::Result<String> + Send + Sync + 'static,
        S: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        Options {
            auth: AuthStyle::Credentials {
                jwt_cb: Arc::new(move || jwt_cb().map(|s| s.into())),
                sig_cb: Arc::new(move |nonce| Ok(base64_url::encode(&sig_cb(nonce)).into())),
            },
            ..Default::default()
        }
    }

    /// Authenticate with NATS using a public key and a signature function.
    ///
    /// # Example
    /// ```no_run
    /// let nkey = "UAMMBNV2EYR65NYZZ7IAK5SIR5ODNTTERJOBOF4KJLMWI45YOXOSWULM";
    /// let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    /// let kp = nkeys::KeyPair::from_seed(seed).unwrap();
    ///
    /// let nc = nats::Options::with_nkey(nkey, move |nonce| kp.sign(nonce).unwrap())
    ///     .connect("localhost")?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn with_nkey<F>(nkey: &str, sig_cb: F) -> Options
    where
        F: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        Options {
            auth: AuthStyle::NKey {
                nkey_cb: {
                    let nkey = SecureString::from(nkey.to_owned());
                    Arc::new(move || Ok(nkey.clone()))
                },
                sig_cb: Arc::new(move |nonce| {
                    let sig = sig_cb(nonce);
                    Ok(SecureString::from(base64_url::encode(&sig)))
                }),
            },
            ..Default::default()
        }
    }

    /// Set client certificate and private key files.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .client_cert("client-cert.pem", "client-key.pem")
    ///     .connect("nats://localhost:4443")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn client_cert(mut self, cert: impl AsRef<Path>, key: impl AsRef<Path>) -> Options {
        self.client_cert = Some(cert.as_ref().to_owned());
        self.client_key = Some(key.as_ref().to_owned());
        self
    }

    /// Set the default TLS config that will be used
    /// for connections. Note that this is less secure
    /// than specifying TLS certificate file paths
    /// using the other methods on `Options`, which
    /// will avoid keeping raw key material in-memory
    /// and will zero memory buffers that temporarily
    /// contain key material during connection attempts.
    /// This is intended to be used as a method of
    /// last-resort when providing well-known file
    /// paths is not feasible.
    ///
    /// To avoid version conflicts, the `rustls` version
    /// used by this crate is exported as `nats::rustls`.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let mut root_store = nats::rustls::RootCertStore::empty();
    /// root_store.add_parsable_certificates(rustls_native_certs::load_native_certs()?);
    ///
    /// let tls_client_config = nats::rustls::ClientConfig::builder()
    ///     .with_root_certificates(root_store)
    ///     .with_no_client_auth();
    ///
    /// let nc = nats::Options::new()
    ///     .tls_client_config(tls_client_config)
    ///     .connect("nats://localhost:4443")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls_client_config(mut self, tls_client_config: crate::rustls::ClientConfig) -> Options {
        self.tls_client_config = Some(tls_client_config);
        self
    }

    /// Add a name option to this configuration.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .with_name("My App")
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_name(mut self, name: &str) -> Options {
        self.name = Some(name.to_string());
        self
    }

    /// Select option to not deliver messages that we have published.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new().no_echo().connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn no_echo(mut self) -> Options {
        self.no_echo = true;
        self
    }

    /// Select option to enable reconnect with backoff
    /// on first failed connection attempt.
    /// The reconnect logic with `max_reconnects` and the
    /// `reconnect_delay_callback` will be specified the same
    /// as before but will be invoked on the first failed
    /// connection attempt.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .retry_on_failed_connect()
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn retry_on_failed_connect(mut self) -> Options {
        self.retry_on_failed_connect = true;
        self
    }

    /// Set the maximum number of reconnect attempts.
    /// If no servers remain that are under this threshold,
    /// then no further reconnect shall be attempted.
    /// The reconnect attempt for a server is reset upon
    /// successful connection.
    /// If None then there is no maximum number of attempts.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .max_reconnects(3)
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn max_reconnects<T: Into<Option<usize>>>(mut self, max_reconnects: T) -> Options {
        self.max_reconnects = max_reconnects.into();
        self
    }

    /// Set the maximum amount of bytes to buffer
    /// when accepting outgoing traffic in disconnected
    /// mode.
    ///
    /// The default value is 8mb.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .reconnect_buffer_size(64 * 1024)
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reconnect_buffer_size(mut self, reconnect_buffer_size: usize) -> Options {
        self.reconnect_buffer_size = reconnect_buffer_size;
        self
    }

    /// Establish a `Connection` with one or more NATS servers.
    ///
    /// To pass more than one URL check out the the documentation of [`crate::connect()`].
    ///
    /// **Note:** If an URL provides username and password, e.g. `nats://derek:s3cr3t!@demo.nats.io`,
    /// it will override the username and password set by the [`Options`]. Be aware that providing
    /// credentials in the URL is not safe and should not be used in production.
    ///
    /// # Example
    ///
    /// ```
    /// let options = nats::Options::new();
    /// let nc = options.connect("demo.nats.io")?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// In the below case, the second server is configured to use TLS but the first one is not.
    /// Using the [`Options::tls_required()`] method can ensure that all servers are connected
    /// to with TLS, if that is your intention.
    ///
    /// ```
    /// let options = nats::Options::new();
    /// let nc = options.connect("nats://demo.nats.io:4222,tls://demo.nats.io:4443")?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn connect<I: IntoServerList>(self, nats_urls: I) -> io::Result<Connection> {
        Connection::connect_with_options(nats_urls, self)
    }

    /// Set a callback to be executed when an async error from
    /// a server has been received.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .error_callback(|err| println!("connection received an error: {}", err))
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn error_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(Error) + Send + Sync + 'static,
    {
        self.error_callback = ErrorCallback(Some(Box::new(cb)));
        self
    }

    /// Set a callback to be executed when connectivity to
    /// a server has been lost.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .disconnect_callback(|| println!("connection has been lost"))
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn disconnect_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.disconnect_callback = Callback(Some(Box::new(cb)));
        self
    }

    /// Set a callback to be executed when connectivity to a
    /// server has been reestablished.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .reconnect_callback(|| println!("connection has been reestablished"))
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reconnect_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.reconnect_callback = Callback(Some(Box::new(cb)));
        self
    }

    /// Set a callback to be executed when the client has been
    /// closed due to exhausting reconnect retries to known servers
    /// or by completing a drain request.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .close_callback(|| println!("connection has been closed"))
    ///     .connect("demo.nats.io")?;
    /// nc.drain().unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub fn close_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.close_callback = Callback(Some(Box::new(cb)));
        self
    }

    /// Set a callback to be executed when the server enters lame duck mode.
    /// Can be set to enable letting user know that client will be soon evicted.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// let nc = nats::Options::new()
    ///     .lame_duck_callback(|| println!("server entered lame duck mode"))
    ///     .connect("demo.nats.io")?;
    /// nc.drain().unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub fn lame_duck_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.lame_duck_callback = Callback(Some(Box::new(cb)));
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
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # use std::time::Duration;
    /// let nc = nats::Options::new()
    ///     .reconnect_delay_callback(|c| Duration::from_millis(std::cmp::min((c * 100) as u64, 8000)))
    ///     .connect("demo.nats.io")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reconnect_delay_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(usize) -> Duration + Send + Sync + 'static,
    {
        self.reconnect_delay_callback = ReconnectDelayCallback(Box::new(cb));
        self
    }

    /// Setting this requires that TLS be set for all server connections.
    ///
    /// If you only want to use TLS for some server connections, you may
    /// declare them separately in the connect string by prefixing them
    /// with `tls://host:port` instead of `nats://host:port`.
    ///
    /// # Examples
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    ///
    /// let nc = nats::Options::new()
    ///     .tls_required(true)
    ///     .connect("tls://demo.nats.io:4443")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls_required(mut self, tls_required: bool) -> Options {
        self.tls_required = tls_required;
        self
    }

    /// Adds a root certificate file.
    ///
    /// The file must be PEM encoded. All certificates in the file will be used.
    ///
    /// # Examples
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    ///
    /// let nc = nats::Options::new()
    ///     .add_root_certificate("my-certs.pem")
    ///     .connect("tls://demo.nats.io:4443")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_root_certificate(mut self, path: impl AsRef<Path>) -> Options {
        self.certificates.push(path.as_ref().to_owned());
        self
    }
}

#[derive(Clone, Default)]
pub(crate) enum AuthStyle {
    /// No authentication.
    #[default]
    NoAuth,

    /// Authenticate using a token.
    Token(String),

    /// Authenticate using a username and password.
    UserPass(String, String),

    /// Authenticate using a `.creds` file.
    Credentials {
        /// Securely loads the user JWT.
        jwt_cb: Arc<dyn Fn() -> io::Result<SecureString> + Send + Sync>,
        /// Securely loads the nkey and signs the nonce passed as an argument.
        #[allow(clippy::type_complexity)]
        sig_cb: Arc<dyn Fn(&[u8]) -> io::Result<SecureString> + Send + Sync>,
    },

    /// Authenticate using an nkey.
    NKey {
        /// Securely loads the public nkey.
        nkey_cb: Arc<dyn Fn() -> io::Result<SecureString> + Send + Sync>,
        /// Signs the nonce passed as an argument.
        #[allow(clippy::type_complexity)]
        sig_cb: Arc<dyn Fn(&[u8]) -> io::Result<SecureString> + Send + Sync>,
    },
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
            AuthStyle::NKey { .. } => f.debug_struct("NKey").finish(),
        }
    }
}

#[derive(Default)]
pub(crate) struct Callback(Option<Box<dyn Fn() + Send + Sync + 'static>>);
impl Callback {
    pub fn call(&self) {
        if let Some(callback) = self.0.as_ref() {
            callback();
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

pub(crate) struct ReconnectDelayCallback(Box<dyn Fn(usize) -> Duration + Send + Sync + 'static>);
impl ReconnectDelayCallback {
    pub fn call(&self, reconnects: usize) -> Duration {
        self.0(reconnects)
    }
}

pub(crate) struct ErrorCallback(Option<Box<dyn Fn(Error) + Send + Sync + 'static>>);
impl ErrorCallback {
    pub fn call(&self, client: &Client, err: Error) {
        if let Some(callback) = self.0.as_ref() {
            callback(err);
        } else {
            let si = client.server_info();
            eprintln!("{} on connection [{}]", err, si.client_id);
        }
    }
}

impl fmt::Debug for ErrorCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(
                &"error_callback",
                if self.0.is_some() { &"set" } else { &"unset" },
            )
            .finish()
    }
}
