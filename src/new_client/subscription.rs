use std::fmt;
use std::io::{self, ErrorKind};
use std::time::Duration;

use blocking::block_on;
use futures::channel::mpsc;
use futures::prelude::*;
use smol::Timer;

use crate::new_client::client::Client;
use crate::new_client::message::Message;

/// A subscription to a subject.
pub struct Subscription {
    /// Subscription ID.
    sid: u64,

    /// MSG operations received from the server.
    messages: mpsc::UnboundedReceiver<Message>,

    client: Client,
}

impl Subscription {
    /// Creates a subscription.
    pub(crate) fn new(
        sid: u64,
        messages: mpsc::UnboundedReceiver<Message>,
        client: Client,
    ) -> Subscription {
        Subscription {
            sid,
            messages,
            client,
        }
    }

    /// Waits for the next message.
    pub fn next(&mut self) -> io::Result<Message> {
        // Block on the next message in the channel.
        block_on(self.messages.next()).ok_or_else(|| ErrorKind::ConnectionReset.into())
    }

    /// Waits for the next message or times out after a duration of time.
    pub fn next_timeout(&mut self, timeout: Duration) -> io::Result<Message> {
        // Block on the next message in the channel.
        block_on(async move {
            futures::select! {
                msg = self.messages.next().fuse() => {
                    match msg {
                        Some(msg) => Ok(msg),
                        None => Err(ErrorKind::ConnectionReset.into()),
                    }
                }
                _ = Timer::after(timeout).fuse() => Err(ErrorKind::TimedOut.into()),
            }
        })
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Send an UNSUB operation to the server.
        let _ = block_on(self.client.unsubscribe(self.sid));
    }
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("Subscription")
            .field("sid", &self.sid)
            .finish()
    }
}
