use std::io;
use std::{sync::Arc, thread, time::Duration};

use crossbeam_channel::RecvTimeoutError;
use smol::{future, prelude::*, Timer};

use crate::{asynk, Message};

/// A `Subscription` receives `Message`s published to specific NATS `Subject`s.
#[derive(Clone, Debug)]
pub struct Subscription(pub(crate) Arc<asynk::Subscription>);

impl Subscription {
    /// Get the next message, or None if the subscription
    /// has been unsubscribed or the connection closed.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// # nc.publish("foo", "hello")?;
    /// if let Some(msg) = sub.next() {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn next(&self) -> Option<Message> {
        future::block_on(async { self.0.messages.recv().await.ok() }).map(Message::from_async)
    }

    /// Try to get the next message, or None if no messages
    /// are present or if the subscription has been unsubscribed
    /// or the connection closed.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// if let Some(msg) = sub.try_next() {
    ///   println!("Received {}", msg);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_next(&self) -> Option<Message> {
        self.0.try_next().map(Message::from_async)
    }

    /// Get the next message, or a timeout error if no messages are available for timout.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// if let Ok(msg) = sub.next_timeout(std::time::Duration::from_secs(1)) {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_timeout(&self, timeout: Duration) -> Result<Message, RecvTimeoutError> {
        future::block_on(
            async {
                match self.0.messages.recv().await {
                    Ok(msg) => Ok(Message::from_async(msg)),
                    Err(_) => Err(RecvTimeoutError::Disconnected),
                }
            }
            .or(async {
                Timer::new(timeout).await;
                Err(RecvTimeoutError::Timeout)
            }),
        )
    }

    /// Returns a blocking message iterator. Same as calling `iter()`.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// for msg in sub.messages() {}
    /// # Ok(())
    /// # }
    /// ```
    pub const fn messages(&self) -> Iter<'_> {
        Iter { subscription: self }
    }

    /// Returns a blocking message iterator.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// for msg in sub.iter() {}
    /// # Ok(())
    /// # }
    /// ```
    pub const fn iter(&self) -> Iter<'_> {
        Iter { subscription: self }
    }

    /// Returns a non-blocking message iterator.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// for msg in sub.try_iter() {}
    /// # Ok(())
    /// # }
    /// ```
    pub const fn try_iter(&self) -> TryIter<'_> {
        TryIter { subscription: self }
    }

    /// Returns a blocking message iterator with a time deadline for blocking.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub = nc.subscribe("foo")?;
    /// for msg in sub.timeout_iter(std::time::Duration::from_secs(1)) {}
    /// # Ok(())
    /// # }
    /// ```
    pub const fn timeout_iter(&self, timeout: Duration) -> TimeoutIter<'_> {
        TimeoutIter {
            subscription: self,
            to: timeout,
        }
    }

    /// Attach a closure to handle messages.
    /// This closure will execute in a separate thread.
    /// The result of this call is a `Handler` which can not be
    /// iterated and must be unsubscribed or closed directly to unregister interest.
    /// A `Handler` will not unregister interest with the server when `drop(&mut self)` is called.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// nc.subscribe("bar")?.with_handler(move |msg| {
    ///     println!("Received {}", &msg);
    ///     Ok(())
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_handler<F>(self, handler: F) -> Handler
    where
        F: Fn(Message) -> io::Result<()> + Sync + Send + 'static,
    {
        // This will allow us to not have to capture the return. When it is dropped it
        // will not unsubscribe from the server.
        let sub = self.clone();
        thread::Builder::new()
            .name(format!("nats_subscriber_{}_{}", self.0.sid, self.0.subject))
            .spawn(move || {
                for m in sub.iter() {
                    if let Err(e) = handler(m) {
                        // TODO(dlc) - Capture for last error?
                        log::error!("Error in callback! {:?}", e);
                    }
                }
            })
            .expect("threads should be spawnable");
        Handler { sub: self }
    }

    /// Unsubscribe a subscription immediately without draining.
    /// Use `drain` instead if you want any pending messages
    /// to be processed by a handler, if one is configured.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?;
    /// sub.unsubscribe()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn unsubscribe(self) -> io::Result<()> {
        future::block_on(self.0.unsubscribe())
    }

    /// Close a subscription. Same as `unsubscribe`
    ///
    /// Use `drain` instead if you want any pending messages
    /// to be processed by a handler, if one is configured.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?;
    /// sub.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(self) -> io::Result<()> {
        self.unsubscribe()
    }

    /// Send an unsubscription then flush the connection,
    /// allowing any unprocessed messages to be handled
    /// by a handler function if one is configured.
    ///
    /// After the flush returns, we know that a round-trip
    /// to the server has happened after it received our
    /// unsubscription, so we shut down the subscriber
    /// afterwards.
    ///
    /// A similar method exists on the `Connection` struct
    /// which will drain all subscriptions for the NATS
    /// client, and transition the entire system into
    /// the closed state afterward.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::{Arc, atomic::{AtomicBool, Ordering::SeqCst}};
    /// # use std::thread;
    /// # use std::time::Duration;
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    ///
    /// let mut sub = nc.subscribe("test.drain")?;
    ///
    /// nc.publish("test.drain", "message")?;
    /// sub.drain()?;
    ///
    /// let mut received = false;
    /// for _ in sub {
    ///     received = true;
    /// }
    ///
    /// assert!(received);
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn drain(&self) -> io::Result<()> {
        future::block_on(async { self.0.drain().await })
    }
}

impl IntoIterator for Subscription {
    type Item = Message;
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        IntoIter { subscription: self }
    }
}

impl<'a> IntoIterator for &'a Subscription {
    type Item = Message;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        Iter { subscription: self }
    }
}

/// A `Handler` may be used to unsubscribe a handler thread.
pub struct Handler {
    sub: Subscription,
}

impl Handler {
    /// Unsubscribe a subscription.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?.with_handler(move |msg| {
    ///     println!("Received {}", &msg);
    ///     Ok(())
    /// });
    /// sub.unsubscribe()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn unsubscribe(self) -> io::Result<()> {
        self.sub.drain()
    }
}

/// A non-blocking iterator over messages from a `Subscription`
pub struct TryIter<'a> {
    subscription: &'a Subscription,
}

impl<'a> Iterator for TryIter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.try_next()
    }
}

/// An iterator over messages from a `Subscription`
pub struct Iter<'a> {
    subscription: &'a Subscription,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.next()
    }
}

/// An iterator over messages from a `Subscription`
pub struct IntoIter {
    subscription: Subscription,
}

impl Iterator for IntoIter {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.next()
    }
}

/// An iterator over messages from a `Subscription`
/// where `None` will be returned if a new `Message`
/// has not been received by the end of a timeout.
pub struct TimeoutIter<'a> {
    subscription: &'a Subscription,
    to: Duration,
}

impl<'a> Iterator for TimeoutIter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.next_timeout(self.to).ok()
    }
}
