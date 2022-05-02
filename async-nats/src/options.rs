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

use crate::{Client, ToServerAddrs};
use std::{fmt, path::PathBuf, time::Duration};

#[cfg(feature = "runtime-async-std")]
use async_std::io;
#[cfg(feature = "runtime-tokio")]
use tokio::io;

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
#[derive(Clone)]
pub struct ConnectOptions {
    // pub(crate) auth: AuthStyle,
    pub(crate) name: Option<String>,
    pub(crate) no_echo: bool,
    pub(crate) retry_on_failed_connect: bool,
    pub(crate) max_reconnects: Option<usize>,
    pub(crate) reconnect_buffer_size: usize,
    pub(crate) tls_required: bool,
    pub(crate) certificates: Vec<PathBuf>,
    pub(crate) client_cert: Option<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) tls_client_config: Option<rustls::ClientConfig>,
    pub(crate) flush_interval: Duration,
    pub(crate) ping_interval: Duration,
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
            tls_required: false,
            certificates: Vec::new(),
            client_cert: None,
            client_key: None,
            tls_client_config: None,
            flush_interval: Duration::from_millis(100),
            ping_interval: Duration::from_secs(60),
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
    pub fn new() -> Self {
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
    pub async fn connect<A: ToServerAddrs>(&mut self, addrs: A) -> io::Result<Client> {
        crate::connect_with_options(addrs, self.to_owned()).await
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
    pub fn add_root_certificates(&mut self, path: PathBuf) -> &mut ConnectOptions {
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
    pub fn add_client_certificate(&mut self, cert: PathBuf, key: PathBuf) -> &mut ConnectOptions {
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
    pub fn require_tls(&mut self, is_required: bool) -> &mut ConnectOptions {
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
    pub fn flush_interval(&mut self, flush_interval: Duration) -> &mut ConnectOptions {
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
    pub fn ping_interval(&mut self, ping_interval: Duration) -> &mut ConnectOptions {
        self.ping_interval = ping_interval;
        self
    }
}
