use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use blocking::block_on;
use futures::prelude::*;

use crate::asynk::client::Client;
use crate::asynk::message::AsyncMessage;

/// A subscription to a subject.
pub struct AsyncSubscription {
    /// Subscription ID.
    pub(crate) sid: u64,

    /// Subject.
    pub(crate) subject: String,

    /// MSG operations received from the server.
    pub(crate) messages: async_channel::Receiver<AsyncMessage>,

    /// Client associated with subscription.
    client: Client,

    /// Whether the subscription is actively listening for new messages.
    active: bool,
}

impl AsyncSubscription {
    /// Creates a subscription.
    pub(crate) fn new(
        sid: u64,
        subject: String,
        messages: async_channel::Receiver<AsyncMessage>,
        client: Client,
    ) -> AsyncSubscription {
        AsyncSubscription {
            sid,
            subject,
            messages,
            client,
            active: true,
        }
    }

    pub(crate) fn try_next(&mut self) -> Option<AsyncMessage> {
        self.messages.try_recv().ok()
    }

    /// Unsubscribes and flushes the connection.
    ///
    /// The remaining messages can still be received.
    pub async fn drain(&mut self) -> io::Result<()> {
        if self.active {
            self.active = false;

            // Flush and unsubscribe.
            self.client.flush().await?;
            self.client.unsubscribe(self.sid).await?;
            Ok(())
        } else {
            Ok(())
        }
    }
}

impl Drop for AsyncSubscription {
    fn drop(&mut self) {
        if self.active {
            self.active = false;

            // TODO(stjepang): Instead of blocking, we should just enqueue a dead subscription ID
            // for later cleanup.
            let _ = block_on(self.client.unsubscribe(self.sid));
        }
    }
}

impl fmt::Debug for AsyncSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("AsyncSubscription")
            .field("sid", &self.sid)
            .finish()
    }
}

impl Stream for AsyncSubscription {
    type Item = AsyncMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.messages).poll_next(cx)
    }
}
