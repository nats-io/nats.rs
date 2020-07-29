use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use smol::stream::Stream;

use crate::asynk::client::Client;
use crate::asynk::message::Message;

/// A subscription to a subject.
pub struct Subscription {
    /// Subscription ID.
    pub(crate) sid: u64,

    /// Subject.
    pub(crate) subject: String,

    /// MSG operations received from the server.
    pub(crate) messages: async_channel::Receiver<Message>,

    /// Client associated with subscription.
    pub(crate) client: Client,
}

impl Subscription {
    /// Creates a subscription.
    pub(crate) fn new(
        sid: u64,
        subject: String,
        messages: async_channel::Receiver<Message>,
        client: Client,
    ) -> Subscription {
        Subscription {
            sid,
            subject,
            messages,
            client,
        }
    }

    /// Attemps to receive the next message without blocking.
    pub(crate) fn try_next(&self) -> Option<Message> {
        self.messages.try_recv().ok()
    }

    /// Stops listening for new messages, but the remaining queued messages can still be received.
    pub async fn drain(&self) -> io::Result<()> {
        self.client.flush().await?;
        self.client.unsubscribe(self.sid);
        self.messages.close();
        Ok(())
    }

    /// Stops listening for new messages and discards the remaining queued messages.
    pub async fn unsubscribe(&self) -> io::Result<()> {
        self.drain().await?;
        // Discard all queued messages.
        while self.messages.try_recv().is_ok() {}
        Ok(())
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.client.unsubscribe(self.sid);
    }
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("Subscription")
            .field("sid", &self.sid)
            .finish()
    }
}

impl Stream for Subscription {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.messages).poll_next(cx)
    }
}
