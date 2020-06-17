use std::io::{self, Error, ErrorKind};
use std::net::IpAddr;
use std::time::{Duration, Instant};

use blocking::block_on;
use futures::prelude::*;
use smol::Timer;

use crate::new_client::client::Client;
use crate::new_client::message::{AsyncMessage, Message};
use crate::new_client::options::Options;
use crate::new_client::subscription::{AsyncSubscription, Subscription};

const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// A NATS client connection.
pub struct AsyncConnection {
    client: Client,
}

impl AsyncConnection {
    /// Connects on a URL with the given options.
    pub(crate) async fn connect_with_options(
        url: &str,
        options: Options,
    ) -> io::Result<AsyncConnection> {
        let client = Client::new(url, options)?;
        client.flush().await?;
        Ok(AsyncConnection { client })
    }

    /// Connects a NATS client.
    pub async fn connect(url: &str) -> io::Result<AsyncConnection> {
        Options::new().connect_async(url).await
    }

    /// Publishes a message.
    pub async fn publish(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        self.do_publish(subject, None, msg).await
    }

    /// Publishes a message with a reply subject.
    pub async fn publish_request(
        &self,
        subject: &str,
        reply: &str,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        self.client
            .publish(subject, Some(reply), msg.as_ref())
            .await
    }

    /// Creates a new unique subject for receiving replies.
    pub fn new_inbox(&self) -> String {
        format!("_INBOX.{}", nuid::next())
    }

    /// Publishes a message and waits for the response.
    pub async fn request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<AsyncMessage> {
        // Publish a request.
        let reply = self.new_inbox();
        let mut sub = self.subscribe(&reply).await?;
        self.do_publish(subject, Some(reply.as_str()), msg).await?;

        // Wait for the response.
        sub.next()
            .await
            .ok_or_else(|| ErrorKind::ConnectionReset.into())
    }

    /// Publishes a message and returns a subscription for awaiting the response.
    pub async fn request_multi(
        &self,
        subject: &str,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<AsyncSubscription> {
        // Publish a request.
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply).await?;
        self.do_publish(subject, Some(reply.as_str()), msg).await?;

        // Return the subscription.
        Ok(sub)
    }

    /// Creates a subscription.
    pub async fn subscribe(&self, subject: &str) -> io::Result<AsyncSubscription> {
        self.do_subscribe(subject, None).await
    }

    /// Creates a queue subscription.
    pub async fn queue_subscribe(
        &self,
        subject: &str,
        queue: &str,
    ) -> io::Result<AsyncSubscription> {
        self.do_subscribe(subject, Some(queue)).await
    }

    /// Flushes by performing a round trip to the server.
    pub async fn flush(&self) -> io::Result<()> {
        self.flush_timeout(DEFAULT_FLUSH_TIMEOUT).await
    }

    /// Flushes by performing a round trip to the server or times out after a duration of time.
    pub async fn flush_timeout(&self, timeout: Duration) -> io::Result<()> {
        futures::select! {
            res = self.client.flush().fuse() => res,
            _ = Timer::after(timeout).fuse() => Err(ErrorKind::TimedOut.into()),
        }
    }

    /// Calculates the round trip time between this client and the server.
    pub async fn rtt(&self) -> io::Result<Duration> {
        let start = Instant::now();
        self.flush().await?;
        Ok(start.elapsed())
    }

    /// Returns the client IP as known by the most recently connected server.
    ///
    /// Supported as of server version 2.1.6.
    pub fn client_ip(&self) -> io::Result<IpAddr> {
        let info = self
            .client
            .server_info()
            .expect("INFO should've been received at connection");

        match info.client_ip.as_str() {
            "" => Err(Error::new(
                ErrorKind::Other,
                &*format!(
                    "client_ip was not provided by the server. \
                    It is supported on servers above version 2.1.6. \
                    The server version is {}",
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
    pub fn client_id(&self) -> u64 {
        self.client
            .server_info()
            .expect("INFO should've been received at connection")
            .client_id
    }

    /// Unsubscribes all subscriptions and flushes the connection.
    ///
    /// Remaining messages can still be received by existing [`AsyncSubscription`]s.
    pub async fn drain(&mut self) -> io::Result<()> {
        self.close().await
    }

    /// Closes the connection.
    pub async fn close(&mut self) -> io::Result<()> {
        self.client.close().await
    }

    async fn do_publish(
        &self,
        subject: &str,
        reply: Option<&str>,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        self.client.publish(subject, reply, msg.as_ref()).await
    }

    async fn do_subscribe(
        &self,
        subject: &str,
        queue: Option<&str>,
    ) -> io::Result<AsyncSubscription> {
        let (sid, receiver) = self.client.subscribe(subject, queue).await?;
        Ok(AsyncSubscription::new(sid, receiver, self.client.clone()))
    }
}

/// A NATS client connection.
pub struct Connection(pub(crate) AsyncConnection);

impl Connection {
    /// Connects on a URL with the given options.
    pub(crate) fn connect_with_options(url: &str, options: Options) -> io::Result<Connection> {
        Ok(Connection(block_on(
            AsyncConnection::connect_with_options(url, options),
        )?))
    }

    /// Connects a NATS client.
    pub fn connect(url: &str) -> io::Result<Connection> {
        Ok(Connection(block_on(AsyncConnection::connect(url))?))
    }

    /// Publishes a message.
    pub fn publish(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        block_on(self.0.publish(subject, msg))
    }

    /// Publishes a message with a reply subject.
    pub fn publish_request(
        &self,
        subject: &str,
        reply: &str,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        block_on(self.0.publish_request(subject, reply, msg))
    }

    /// Creates a new unique subject for receiving replies.
    pub fn new_inbox(&self) -> String {
        self.0.new_inbox()
    }

    /// Publishes a message and waits for the response.
    pub fn request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Message> {
        block_on(self.0.request(subject, msg)).map(Message::from_async)
    }

    /// Publishes a message and waits for the response or times out after a duration of time.
    pub fn request_timeout(
        &self,
        subject: &str,
        msg: impl AsRef<[u8]>,
        timeout: Duration,
    ) -> io::Result<Message> {
        block_on(async move {
            futures::select! {
                res = self.0.request(subject, msg).fuse() => res.map(Message::from_async),
                _ = Timer::after(timeout).fuse() => Err(ErrorKind::TimedOut.into()),
            }
        })
    }

    /// Publishes a message and returns a subscription for awaiting the response.
    pub fn request_multi(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Subscription> {
        block_on(self.0.request_multi(subject, msg)).map(Subscription)
    }

    /// Creates a subscription.
    pub fn subscribe(&self, subject: &str) -> io::Result<Subscription> {
        block_on(self.0.subscribe(subject)).map(Subscription)
    }

    /// Creates a queue subscription.
    pub fn queue_subscribe(&self, subject: &str, queue: &str) -> io::Result<Subscription> {
        block_on(self.0.queue_subscribe(subject, queue)).map(Subscription)
    }

    /// Flushes by performing a round trip to the server.
    pub fn flush(&self) -> io::Result<()> {
        block_on(self.0.flush())
    }

    /// Flushes by performing a round trip to the server or times out after a duration of time.
    pub fn flush_timeout(&self, timeout: Duration) -> io::Result<()> {
        block_on(self.0.flush_timeout(timeout))
    }

    /// Calculates the round trip time between this client and the server.
    pub fn rtt(&self) -> io::Result<Duration> {
        block_on(self.0.rtt())
    }

    /// Returns the client IP as known by the most recently connected server.
    ///
    /// Supported as of server version 2.1.6.
    pub fn client_ip(&self) -> io::Result<IpAddr> {
        self.0.client_ip()
    }

    /// Returns the client ID as known by the most recently connected server.
    pub fn client_id(&self) -> u64 {
        self.0.client_id()
    }

    /// Unsubscribes all subscriptions and flushes the connection.
    ///
    /// Remaining messages can still be received by existing [`Subscription`]s.
    pub fn drain(&mut self) -> io::Result<()> {
        block_on(self.0.drain())
    }

    /// Closes the connection.
    pub fn close(&mut self) -> io::Result<()> {
        block_on(self.0.close())
    }
}
