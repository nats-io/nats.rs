use std::io;
use std::time::Duration;

use blocking::block_on;

use crate::new_client::client::Client;
use crate::new_client::message::Message;
use crate::new_client::options::{ConnectionOptions, Options};
use crate::new_client::subscription::Subscription;

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
        block_on(self.client.publish(subject, None, msg.as_ref()))
    }

    /// Creates a new unique subject for receiving replies.
    pub fn new_inbox(&self) -> String {
        format!("_INBOX.{}", nuid::next())
    }

    /// Publishes a message with a reply subject.
    pub fn request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Message> {
        // Publish a request.
        let mut sub = self.prepare_request(subject, msg)?;

        // Wait for the response.
        sub.next()
    }

    /// Publishes a message with a reply subject or times out after a duration of time.
    pub fn request_timeout(
        &self,
        subject: &str,
        msg: impl AsRef<[u8]>,
        timeout: Duration,
    ) -> io::Result<Message> {
        // Publish a request.
        let mut sub = self.prepare_request(subject, msg)?;

        // Wait for the response.
        sub.next_timeout(timeout)
    }

    /// Creates a new subscriber.
    pub fn subscribe(&self, subject: &str) -> io::Result<Subscription> {
        let (sid, receiver) = block_on(self.client.subscribe(subject, None))?;
        Ok(Subscription::new(sid, receiver, self.client.clone()))
    }

    /// Flushes by performing a round trip to the server.
    pub fn flush(&self) -> io::Result<()> {
        block_on(self.client.flush())
    }

    /// Close the connection.
    pub fn close(&mut self) -> io::Result<()> {
        block_on(self.client.close())
    }

    fn prepare_request(&self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<Subscription> {
        let reply_to = self.new_inbox();
        let sub = self.subscribe(&reply_to)?;

        let reply_to = Some(reply_to.as_str());
        block_on(self.client.publish(subject, reply_to, msg.as_ref()))?;

        Ok(sub)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Close the connection in case it hasn't been already.
        let _ = self.close();
    }
}
