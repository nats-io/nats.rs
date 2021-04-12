use std::io;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel as channel;

use crate::client::Client;
use crate::message::Message;

#[derive(Debug)]
struct Inner {
    /// Subscription ID.
    pub(crate) sid: u64,

    /// Subject.
    pub(crate) subject: String,

    /// MSG operations received from the server.
    pub(crate) messages: channel::Receiver<Message>,

    /// Client associated with subscription.
    pub(crate) client: Client,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.client.unsubscribe(self.sid).ok();
    }
}

/// A `Subscription` receives `Message`s published
/// to specific NATS `Subject`s.
#[derive(Clone, Debug)]
pub struct Subscription(Arc<Inner>);

impl Subscription {
    /// Creates a subscription.
    pub(crate) fn new(
        sid: u64,
        subject: String,
        messages: channel::Receiver<Message>,
        client: Client,
    ) -> Subscription {
        Subscription(Arc::new(Inner {
            sid,
            subject,
            messages,
            client,
        }))
    }

    /// Get a crossbeam Receiver for subscription messages.
    /// Useful for `crossbeam_channel::select` macro
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// # let sub1 = nc.subscribe("foo")?;
    /// # let sub2 = nc.subscribe("bar")?;
    /// # nc.publish("foo", "hello")?;
    /// let sub1_ch = sub1.receiver();
    /// let sub2_ch = sub2.receiver();
    /// crossbeam_channel::select! {
    ///     recv(sub1_ch) -> msg => {
    ///         println!("Got message from sub1: {:?}", msg);
    ///         Ok(())
    ///     }
    ///     recv(sub2_ch) -> msg => {
    ///         println!("Got message from sub2: {:?}", msg);
    ///         Ok(())
    ///     }
    /// }
    /// # }
    /// ```
    pub fn receiver(&self) -> &channel::Receiver<Message> {
        &self.0.messages
    }

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
        self.0.messages.recv().ok()
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
        self.0.messages.try_recv().ok()
    }

    /// Get the next message, or a timeout error
    /// if no messages are available for timout.
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
    pub fn next_timeout(&self, timeout: Duration) -> io::Result<Message> {
        match self.0.messages.recv_timeout(timeout) {
            Ok(msg) => Ok(msg),
            Err(channel::RecvTimeoutError::Timeout) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "next_timeout: timed out",
            )),
            Err(channel::RecvTimeoutError::Disconnected) => {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "next_timeout: unsubscribed",
                ))
            }
        }
    }

    /// Returns a blocking message iterator.
    /// Same as calling `iter()`.
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
    pub fn messages(&self) -> Iter<'_> {
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
    pub fn iter(&self) -> Iter<'_> {
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
    pub fn try_iter(&self) -> TryIter<'_> {
        TryIter { subscription: self }
    }

    /// Returns a blocking message iterator with a time
    /// deadline for blocking.
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
    pub fn timeout_iter(&self, timeout: Duration) -> TimeoutIter<'_> {
        TimeoutIter {
            subscription: self,
            to: timeout,
        }
    }

    /// Attach a closure to handle messages. This closure will execute in a
    /// separate thread. The result of this call is a `Handler` which can
    /// not be iterated and must be unsubscribed or closed directly to
    /// unregister interest. A `Handler` will not unregister interest with
    /// the server when `drop(&mut self)` is called.
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
        F: Fn(Message) -> io::Result<()> + Send + 'static,
    {
        // This will allow us to not have to capture the return. When it is
        // dropped it will not unsubscribe from the server.
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
        self.drain()?;
        // Discard all queued messages.
        while self.0.messages.try_recv().is_ok() {}
        Ok(())
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
        self.0.client.flush(crate::DEFAULT_FLUSH_TIMEOUT)?;
        self.0.client.unsubscribe(self.0.sid)?;
        Ok(())
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
