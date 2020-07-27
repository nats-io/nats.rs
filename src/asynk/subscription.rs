use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use smol::{future, stream::Stream};

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

    /// Whether the subscription is actively listening for new messages.
    pub(crate) active: AtomicBool,
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
            active: AtomicBool::new(true),
        }
    }

    /// Attemps to receive the next message without blocking.
    pub(crate) fn try_next(&self) -> Option<Message> {
        self.messages.try_recv().ok()
    }

    /// Unsubscribes and flushes the connection.
    ///
    /// The remaining messages can still be received.
    pub async fn drain(&self) -> io::Result<()> {
        if self.active.swap(false, Ordering::SeqCst) {
            // Flush and unsubscribe.
            self.client.flush().await?;
            self.client.unsubscribe(self.sid).await?;
            Ok(())
        } else {
            Ok(())
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.active.swap(false, Ordering::SeqCst) {
            // TODO(stjepang): Instead of blocking, we should just enqueue a dead subscription ID
            // for later cleanup.
            let _ = future::block_on(self.client.unsubscribe(self.sid));
        }
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
