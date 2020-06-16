use std::io::{self, Error, ErrorKind};
use std::net::IpAddr;
use std::time::{Duration, Instant};

use blocking::block_on;
use futures::prelude::*;
use smol::Timer;

use crate::new_client::client::Client;
use crate::new_client::message::Message;
use crate::new_client::options::{ConnectionOptions, Options};
use crate::new_client::subscription::Subscription;

const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// A NATS client connection.
pub struct Connection {
    client: Client,
}

impl Connection {
    /// Connects on a URL with the given options.
    pub(crate) fn connect_with_options(url: &str, options: Options) -> io::Result<Connection> {
        let client = Client::new(url, options)?;
        block_on(client.flush())?;
        Ok(Connection { client })
    }

    /// Connects a NATS client.
    pub fn connect(url: &str) -> io::Result<Connection> {
        ConnectionOptions::new().connect(url)
    }

    /// Publishes a message.
    pub fn publish(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        self.do_publish(subject, None, msg)
    }

    /// Publishes a message with a reply subject.
    pub fn publish_request(
        &self,
        subject: &str,
        reply: &str,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        block_on(self.client.publish(subject, Some(reply), msg.as_ref()))
    }

    /// Creates a new unique subject for receiving replies.
    pub fn new_inbox(&self) -> String {
        format!("_INBOX.{}", nuid::next())
    }

    /// Publishes a message and waits for the response.
    pub fn request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Message> {
        // Publish a request.
        let reply = self.new_inbox();
        let mut sub = self.subscribe(&reply)?;
        self.do_publish(subject, Some(reply.as_str()), msg)?;

        // Wait for the response.
        sub.next()
    }

    /// Publishes a message and waits for the response or times out after a duration of time.
    pub fn request_timeout(
        &self,
        subject: &str,
        msg: impl AsRef<[u8]>,
        timeout: Duration,
    ) -> io::Result<Message> {
        // Publish a request.
        let reply = self.new_inbox();
        let mut sub = self.subscribe(&reply)?;
        self.do_publish(subject, Some(reply.as_str()), msg)?;

        // Wait for the response.
        sub.next_timeout(timeout)
    }

    /// Publishes a message and returns a subscription for awaiting the response.
    pub fn request_multi(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Subscription> {
        // Publish a request.
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply)?;
        self.do_publish(subject, Some(reply.as_str()), msg)?;

        // Return the subscription.
        Ok(sub)
    }

    /// Creates a subscription.
    pub fn subscribe(&self, subject: &str) -> io::Result<Subscription> {
        self.do_subscribe(subject, None)
    }

    /// Creates a queue subscription.
    pub fn queue_subscribe(&self, subject: &str, queue: &str) -> io::Result<Subscription> {
        self.do_subscribe(subject, Some(queue))
    }

    /// Flushes by performing a round trip to the server.
    pub fn flush(&self) -> io::Result<()> {
        self.flush_timeout(DEFAULT_FLUSH_TIMEOUT)
    }

    /// Flushes by performing a round trip to the server or times out after a duration of time.
    pub fn flush_timeout(&self, timeout: Duration) -> io::Result<()> {
        block_on(async move {
            futures::select! {
                res = self.client.flush().fuse() => res,
                _ = Timer::after(timeout).fuse() => Err(ErrorKind::TimedOut.into()),
            }
        })
    }

    /// Calculates the round trip time between this client and the server.
    pub fn rtt(&self) -> io::Result<Duration> {
        let start = Instant::now();
        self.flush()?;
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
    /// Remaining messages can still be received by existing [`Subscription`]s.
    pub fn drain(&mut self) -> io::Result<()> {
        self.close()
    }

    /// Closes the connection.
    pub fn close(&mut self) -> io::Result<()> {
        block_on(self.client.close())
    }

    fn do_publish(
        &self,
        subject: &str,
        reply: Option<&str>,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        block_on(self.client.publish(subject, reply, msg.as_ref()))
    }

    fn do_subscribe(&self, subject: &str, queue: Option<&str>) -> io::Result<Subscription> {
        let (sid, receiver) = block_on(self.client.subscribe(subject, queue))?;
        Ok(Subscription::new(sid, receiver, self.client.clone()))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Close the connection in case it hasn't been already.
        let _ = self.close();
    }
}
