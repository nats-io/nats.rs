use std::io::{self, Error, ErrorKind};
use std::net::IpAddr;
use std::time::{Duration, Instant};

use smol::future::FutureExt;
use smol::stream::StreamExt;
use smol::Timer;

use crate::asynk::client::Client;
use crate::asynk::message::Message;
use crate::asynk::subscription::Subscription;
use crate::{Headers, Options};

const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// A NATS client connection.
#[derive(Clone, Debug)]
pub struct Connection {
    client: Client,
}

impl Connection {
    /// Connects on a URL with the given options.
    pub(crate) async fn connect_with_options(
        url: &str,
        options: Options,
    ) -> io::Result<Connection> {
        let client = Client::connect(url, options).await?;
        client.flush().await?;
        Ok(Connection { client })
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
        self.client
            .publish(subject, Some(reply), None, msg.as_ref())
            .await
    }

    /// Creates a new unique subject for receiving replies.
    pub fn new_inbox(&self) -> String {
        format!("_INBOX.{}", nuid::next())
    }

    /// Publishes a message and waits for the response.
    pub async fn request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Message> {
        // Publish a request.
        let reply = self.new_inbox();
        let mut sub = self.subscribe(&reply).await?;
        self.publish_with_reply_or_headers(subject, Some(reply.as_str()), None, msg)
            .await?;

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
    ) -> io::Result<Subscription> {
        // Publish a request.
        let reply = self.new_inbox();
        let sub = self.subscribe(&reply).await?;
        self.publish_with_reply_or_headers(subject, Some(reply.as_str()), None, msg)
            .await?;

        // Return the subscription.
        Ok(sub)
    }

    /// Creates a subscription.
    pub async fn subscribe(&self, subject: &str) -> io::Result<Subscription> {
        self.do_subscribe(subject, None).await
    }

    /// Creates a queue subscription.
    pub async fn queue_subscribe(&self, subject: &str, queue: &str) -> io::Result<Subscription> {
        self.do_subscribe(subject, Some(queue)).await
    }

    /// Flushes by performing a round trip to the server.
    pub async fn flush(&self) -> io::Result<()> {
        self.flush_timeout(DEFAULT_FLUSH_TIMEOUT).await
    }

    /// Flushes by performing a round trip to the server or times out after a duration of time.
    pub async fn flush_timeout(&self, timeout: Duration) -> io::Result<()> {
        self.client
            .flush()
            .or(async {
                Timer::new(timeout).await;
                Err(ErrorKind::TimedOut.into())
            })
            .await
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
    /// Remaining messages can still be received by existing [`Subscription`]s.
    pub async fn drain(&self) -> io::Result<()> {
        self.close().await
    }

    /// Closes the connection.
    pub async fn close(&self) -> io::Result<()> {
        self.client.flush().await?;
        self.client.close().await
    }

    /// Publish a message which may have a reply subject or headers set.
    pub async fn publish_with_reply_or_headers(
        &self,
        subject: &str,
        reply: Option<&str>,
        headers: Option<&Headers>,
        msg: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        self.client
            .publish(subject, reply, headers, msg.as_ref())
            .await
    }

    async fn do_subscribe(&self, subject: &str, queue: Option<&str>) -> io::Result<Subscription> {
        let (sid, receiver) = self.client.subscribe(subject, queue).await?;
        Ok(Subscription::new(
            sid,
            subject.to_string(),
            receiver,
            self.client.clone(),
        ))
    }
}
