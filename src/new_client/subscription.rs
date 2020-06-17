use std::fmt;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use blocking::block_on;
use futures::channel::mpsc;
use futures::prelude::*;
use smol::Timer;

use crate::new_client::client::Client;
use crate::new_client::message::{AsyncMessage, Message};

/// A subscription to a subject.
pub struct AsyncSubscription {
    /// Subscription ID.
    sid: u64,

    /// MSG operations received from the server.
    messages: mpsc::UnboundedReceiver<AsyncMessage>,

    /// Client associated with subscription.
    client: Client,

    /// Whether the subscription is actively listening for new messages.
    active: bool,
}

impl AsyncSubscription {
    /// Creates a subscription.
    pub(crate) fn new(
        sid: u64,
        messages: mpsc::UnboundedReceiver<AsyncMessage>,
        client: Client,
    ) -> AsyncSubscription {
        AsyncSubscription {
            sid,
            messages,
            client,
            active: false,
        }
    }

    /// Unsubscribes and flushes the connection.
    ///
    /// The remaining messages can still be received.
    pub async fn drain(&mut self) -> io::Result<()> {
        if !self.active {
            self.active = true;

            // Send an UNSUB operation to the server.
            self.client.unsubscribe(self.sid).await?;
            self.client.flush().await?;
            Ok(())
        } else {
            Ok(())
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

/// A subscription to a subject.
pub struct Subscription(pub(crate) AsyncSubscription);

impl Subscription {
    /// Waits for the next message.
    pub fn next(&mut self) -> io::Result<Message> {
        // Block on the next message in the channel.
        block_on(self.0.next())
            .ok_or_else(|| ErrorKind::ConnectionReset.into())
            .map(Message::from_async)
    }

    /// Waits for the next message or times out after a duration of time.
    pub fn next_timeout(&mut self, timeout: Duration) -> io::Result<Message> {
        block_on(async move {
            futures::select! {
                msg = self.0.next().fuse() => {
                    match msg {
                        Some(msg) => Ok(Message::from_async(msg)),
                        None => Err(ErrorKind::ConnectionReset.into()),
                    }
                }
                _ = Timer::after(timeout).fuse() => Err(ErrorKind::TimedOut.into()),
            }
        })
    }

    /// Unsubscribes and flushes the connection.
    ///
    /// The remaining messages can still be received.
    pub fn drain(&mut self) -> io::Result<()> {
        block_on(self.0.drain())
    }
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("Subscription")
            .field("sid", &self.0.sid)
            .finish()
    }
}
