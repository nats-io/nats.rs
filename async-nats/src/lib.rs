use std::fmt;
use std::io;
use std::mem;
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

use blocking::unblock;

pub use nats::Headers;

/// A NATS client connection.
#[derive(Clone, Debug)]
pub struct Connection {
    inner: nats::Connection,
}

impl Connection {
    fn new(inner: nats::Connection) -> Connection {
        Self { inner }
    }

    /// Publishes a message.
    pub async fn publish(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        self.publish_with_reply_or_headers(subject, None, None, msg)
            .await
    }

    /// Publishes a message with a reply subject.
    pub async fn publish_request(
        &self,
        subject: &str,
        reply: &str,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        if let Some(res) =
            self.inner
                .try_publish_with_reply_or_headers(subject, Some(reply), None, &msg)
        {
            return res;
        }
        let subject = subject.to_string();
        let reply = reply.to_string();
        let msg = msg.as_ref().to_vec();
        let inner = self.inner.clone();
        unblock(move || inner.publish_request(&subject, &reply, msg)).await
    }

    /// Creates a new unique subject for receiving replies.
    pub fn new_inbox(&self) -> String {
        self.inner.new_inbox()
    }

    /// Publishes a message and waits for the response.
    pub async fn request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Message> {
        let subject = subject.to_string();
        let msg = msg.as_ref().to_vec();
        let inner = self.inner.clone();
        let msg = unblock(move || inner.request(&subject, msg)).await?;
        Ok(Message::new(msg))
    }

    /// Publishes a message and returns a subscription for awaiting the response.
    pub async fn request_multi(
        &self,
        subject: &str,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<Subscription> {
        let subject = subject.to_string();
        let msg = msg.as_ref().to_vec();
        let inner = self.inner.clone();
        let sub = unblock(move || inner.request_multi(&subject, msg)).await?;
        Ok(Subscription { inner: sub })
    }

    /// Creates a subscription.
    pub async fn subscribe(&self, subject: &str) -> io::Result<Subscription> {
        let subject = subject.to_string();
        let inner = self.inner.clone();
        let inner = unblock(move || inner.subscribe(&subject)).await?;
        Ok(Subscription { inner })
    }

    /// Creates a queue subscription.
    pub async fn queue_subscribe(&self, subject: &str, queue: &str) -> io::Result<Subscription> {
        let subject = subject.to_string();
        let queue = queue.to_string();
        let inner = self.inner.clone();
        let inner = unblock(move || inner.queue_subscribe(&subject, &queue)).await?;
        Ok(Subscription { inner })
    }

    /// Flushes by performing a round trip to the server.
    pub async fn flush(&self) -> io::Result<()> {
        let inner = self.inner.clone();
        unblock(move || inner.flush()).await
    }

    /// Flushes by performing a round trip to the server or times out after a duration of time.
    pub async fn flush_timeout(&self, timeout: Duration) -> io::Result<()> {
        let inner = self.inner.clone();
        unblock(move || inner.flush_timeout(timeout)).await
    }

    /// Calculates the round trip time between this client and the server.
    pub async fn rtt(&self) -> io::Result<Duration> {
        let inner = self.inner.clone();
        unblock(move || inner.rtt()).await
    }

    /// Returns the client IP as known by the most recently connected server.
    ///
    /// Supported as of server version 2.1.6.
    pub fn client_ip(&self) -> io::Result<IpAddr> {
        self.inner.client_ip()
    }

    /// Returns the client ID as known by the most recently connected server.
    pub fn client_id(&self) -> u64 {
        self.inner.client_id()
    }

    /// Unsubscribes all subscriptions and flushes the connection.
    ///
    /// Remaining messages can still be received by existing [`Subscription`]s.
    pub async fn drain(&self) -> io::Result<()> {
        let inner = self.inner.clone();
        unblock(move || inner.drain()).await
    }

    /// Closes the connection.
    pub async fn close(&self) -> io::Result<()> {
        let inner = self.inner.clone();
        unblock(move || inner.close()).await;
        Ok(())
    }

    /// Publish a message which may have a reply subject or headers set.
    pub async fn publish_with_reply_or_headers(
        &self,
        subject: &str,
        reply: Option<&str>,
        headers: Option<&Headers>,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        if let Some(res) = self
            .inner
            .try_publish_with_reply_or_headers(subject, reply, headers, &msg)
        {
            return res;
        }
        let subject = subject.to_string();
        let reply = reply.map(str::to_owned);
        let headers = headers.map(Headers::clone);
        let msg = msg.as_ref().to_vec();
        let inner = self.inner.clone();
        unblock(move || {
            inner.publish_with_reply_or_headers(
                &subject,
                reply.as_ref().map(String::as_str),
                headers.as_ref(),
                msg,
            )
        })
        .await
    }
}

/// A subscription to a subject.
#[derive(Debug)]
pub struct Subscription {
    inner: nats::Subscription,
}

impl Subscription {
    /// Gets the next message, or returns `None` if the subscription
    /// has been unsubscribed or the connection is closed.
    pub async fn next(&self) -> Option<Message> {
        if let Some(msg) = self.inner.try_next() {
            return Some(Message::new(msg));
        }
        let inner = self.inner.clone();
        let msg = unblock(move || inner.next()).await?;
        Some(Message::new(msg))
    }

    /// Stops listening for new messages, but the remaining queued messages can still be received.
    pub async fn drain(&self) -> io::Result<()> {
        let inner = self.inner.clone();
        unblock(move || inner.drain()).await
    }

    /// Stops listening for new messages and discards the remaining queued messages.
    pub async fn unsubscribe(&self) -> io::Result<()> {
        let inner = self.inner.clone();
        unblock(move || inner.unsubscribe()).await
    }
}

/// A message received on a subject.
#[derive(Clone)]
pub struct Message {
    /// The subject this message came from.
    pub subject: String,

    /// Optional reply subject that may be used for sending a response to this message.
    pub reply: Option<String>,

    /// The message contents.
    pub data: Vec<u8>,

    /// Optional headers associated with this `Message`.
    pub headers: Option<Headers>,

    inner: nats::Message,
}

impl Message {
    fn new(mut inner: nats::Message) -> Message {
        Message {
            subject: mem::replace(&mut inner.subject, String::new()),
            reply: inner.reply.take(),
            data: mem::replace(&mut inner.data, Vec::new()),
            headers: inner.headers.take(),
            inner,
        }
    }

    /// Respond to a request message.
    pub async fn respond(&self, msg: impl AsRef<[u8]>) -> io::Result<()> {
        match self.reply.as_ref() {
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no reply subject available",
            )),
            Some(reply) => {
                if let Some(res) = self
                    .inner
                    .client
                    .try_publish(reply, None, None, msg.as_ref())
                {
                    return res;
                }
                let reply = reply.to_string();
                let msg = msg.as_ref().to_vec();
                let client = self.inner.client.clone();
                unblock(move || client.publish(&reply, None, None, msg.as_ref())).await
            }
        }
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("Message")
            .field("subject", &self.subject)
            .field("headers", &self.headers)
            .field("reply", &self.reply)
            .field("length", &self.data.len())
            .finish()
    }
}

/// Connect options.
#[derive(Debug, Default)]
pub struct Options {
    inner: nats::Options,
}

impl Options {
    /// `Options` for establishing a new NATS `Connection`.
    ///
    /// # Example
    /// ```
    /// # smol::block_on(async {
    /// let options = async_nats::Options::new();
    /// let nc = options.connect("demo.nats.io").await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn new() -> Options {
        Options {
            inner: nats::Options::new(),
        }
    }

    /// Authenticate with NATS using a token.
    ///
    /// # Example
    /// ```
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::with_token("t0k3n!")
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn with_token(token: &str) -> Options {
        Options {
            inner: nats::Options::with_token(token),
        }
    }

    /// Authenticate with NATS using a username and password.
    ///
    /// # Example
    /// ```
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::with_user_pass("derek", "s3cr3t!")
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn with_user_pass(user: &str, password: &str) -> Options {
        Options {
            inner: nats::Options::with_user_pass(user, password),
        }
    }

    /// Authenticate with NATS using a `.creds` file.
    ///
    /// # Example
    /// ```no_run
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::with_credentials("path/to/my.creds")
    ///     .connect("connect.ngs.global")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn with_credentials(path: impl AsRef<Path>) -> Options {
        Options {
            inner: nats::Options::with_credentials(path),
        }
    }

    /// Authenticate with a function that loads user JWT and a signature function.
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
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::with_jwt(load_jwt, move |nonce| kp.sign(nonce).unwrap())
    ///     .connect("localhost")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn with_jwt<J, S>(jwt_cb: J, sig_cb: S) -> Options
    where
        J: Fn() -> io::Result<String> + Send + Sync + 'static,
        S: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        Options {
            inner: nats::Options::with_jwt(jwt_cb, sig_cb),
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
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::with_nkey(nkey, move |nonce| kp.sign(nonce).unwrap())
    ///     .connect("localhost")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn with_nkey<F>(nkey: &str, sig_cb: F) -> Options
    where
        F: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        Options {
            inner: nats::Options::with_nkey(nkey, sig_cb),
        }
    }

    /// Set client certificate and private key files.
    ///
    /// # Example
    /// ```no_run
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .client_cert("client-cert.pem", "client-key.pem")
    ///     .connect("nats://localhost:4443")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn client_cert(self, cert: impl AsRef<Path>, key: impl AsRef<Path>) -> Options {
        Options {
            inner: self.inner.client_cert(cert, key),
        }
    }

    /// Add a name option to this configuration.
    ///
    /// # Example
    /// ```
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .with_name("My App")
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn with_name(self, name: &str) -> Options {
        Options {
            inner: self.inner.with_name(name),
        }
    }

    /// Select option to not deliver messages that we have published.
    ///
    /// # Example
    /// ```
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .no_echo()
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn no_echo(self) -> Options {
        Options {
            inner: self.inner.no_echo(),
        }
    }

    /// Set the maximum number of reconnect attempts.
    /// If no servers remain that are under this threshold,
    /// then no further reconnect shall be attempted.
    /// The reconnect attempt for a server is reset upon
    /// successfull connection.
    /// If None then there is no maximum number of attempts.
    ///
    /// # Example
    /// ```
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .max_reconnects(3)
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn max_reconnects<T: Into<Option<usize>>>(self, max_reconnects: T) -> Options {
        Options {
            inner: self.inner.max_reconnects(max_reconnects),
        }
    }

    /// Set the maximum amount of bytes to buffer
    /// when accepting outgoing traffic in disconnected
    /// mode.
    ///
    /// The default value is 8mb.
    ///
    /// # Example
    /// ```
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .reconnect_buffer_size(64 * 1024)
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn reconnect_buffer_size(self, reconnect_buffer_size: usize) -> Options {
        Options {
            inner: self.inner.reconnect_buffer_size(reconnect_buffer_size),
        }
    }

    /// Establish a `Connection` with a NATS server.
    ///
    /// Multiple servers may be specified by separating
    /// them with commas.
    ///
    /// # Example
    ///
    /// ```
    /// # smol::block_on(async {
    /// let options = async_nats::Options::new();
    /// let nc = options.connect("demo.nats.io").await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    ///
    /// In the below case, the second server is configured
    /// to use TLS but the first one is not. Using the
    /// `tls_required` method can ensure that all
    /// servers are connected to with TLS, if that is
    /// your intention.
    ///
    ///
    /// ```
    /// # smol::block_on(async {
    /// let options = async_nats::Options::new();
    /// let nc = options
    ///     .connect("nats://demo.nats.io:4222,tls://demo.nats.io:4443")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn connect(self, nats_url: &str) -> io::Result<Connection> {
        let nats_url = nats_url.to_string();
        let conn = unblock(move || self.inner.connect(&nats_url)).await?;
        Ok(Connection::new(conn))
    }

    /// Set a callback to be executed when connectivity to
    /// a server has been lost.
    ///
    /// # Example
    ///
    /// ```
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .disconnect_callback(|| println!("connection has been lost"))
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn disconnect_callback<F>(self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Options {
            inner: self.inner.disconnect_callback(cb),
        }
    }

    /// Set a callback to be executed when connectivity to a
    /// server has been reestablished.
    ///
    /// # Example
    ///
    /// ```
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .reconnect_callback(|| println!("connection has been reestablished"))
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn reconnect_callback<F>(self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Options {
            inner: self.inner.reconnect_callback(cb),
        }
    }

    /// Set a callback to be executed when the client has been
    /// closed due to exhausting reconnect retries to known servers
    /// or by completing a drain request.
    ///
    /// # Example
    ///
    /// ```
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .close_callback(|| println!("connection has been closed"))
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn close_callback<F>(self, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Options {
            inner: self.inner.close_callback(cb),
        }
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
    /// # use std::time::Duration;
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .reconnect_delay_callback(|c| Duration::from_millis(std::cmp::min((c * 100) as u64, 8000)))
    ///     .connect("demo.nats.io")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn reconnect_delay_callback<F>(self, cb: F) -> Self
    where
        F: Fn(usize) -> Duration + Send + Sync + 'static,
    {
        Options {
            inner: self.inner.reconnect_delay_callback(cb),
        }
    }

    /// Setting this requires that TLS be set for all server connections.
    ///
    /// If you only want to use TLS for some server connections, you may
    /// declare them separately in the connect string by prefixing them
    /// with `tls://host:port` instead of `nats://host:port`.
    ///
    /// # Examples
    /// ```no_run
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .tls_required(true)
    ///     .connect("tls://demo.nats.io:4443")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn tls_required(self, tls_required: bool) -> Options {
        Options {
            inner: self.inner.tls_required(tls_required),
        }
    }

    /// Adds a root certificate file.
    ///
    /// The file must be PEM encoded. All certificates in the file will be used.
    ///
    /// # Examples
    /// ```no_run
    /// # smol::block_on(async {
    /// let nc = async_nats::Options::new()
    ///     .add_root_certificate("my-certs.pem")
    ///     .connect("tls://demo.nats.io:4443")
    ///     .await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn add_root_certificate(self, path: impl AsRef<Path>) -> Options {
        Options {
            inner: self.inner.add_root_certificate(path),
        }
    }
}

/// Connect to a NATS server at the given url.
///
/// # Example
/// ```
/// # smol::block_on(async {
/// let nc = async_nats::connect("demo.nats.io").await?;
/// # std::io::Result::Ok(()) });
/// ```
pub async fn connect(nats_url: &str) -> io::Result<Connection> {
    Options::new().connect(nats_url).await
}
