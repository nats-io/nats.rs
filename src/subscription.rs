use std::{io, sync::Arc, thread, time::Duration};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use crate::{Message, SharedState, ShutdownDropper};

/// A `Subscription` receives `Message`s published to specific NATS `Subject`s.
#[derive(Clone, Debug)]
pub struct Subscription {
    pub(crate) subject: String,
    pub(crate) sid: usize,
    pub(crate) recv: Receiver<Message>,
    pub(crate) shared_state: Arc<SharedState>,
    pub(crate) do_unsub: bool,
    pub(crate) shutdown_dropper: Arc<ShutdownDropper>,
}

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
        self.recv.iter().next()
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
        self.recv.try_iter().next()
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
        self.recv.recv_timeout(timeout)
    }

    /// Returns a blocking message iterator. Same as calling `iter()`.
    ///
    /// # Example
    /// ```rust,no_run
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
    /// ```rust,no_run
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
    pub fn with_handler<F>(mut self, handler: F) -> Handler
    where
        F: Fn(Message) -> io::Result<()> + Sync + Send + 'static,
    {
        // This will allow us to not have to capture the return. When it is dropped it
        // will not unsubscribe from the server.
        self.do_unsub = false;
        let r = self.recv.clone();
        thread::spawn(move || {
            for m in r.iter() {
                if let Err(e) = handler(m) {
                    // TODO(dlc) - Capture for last error?
                    eprintln!("Error in callback! {:?}", e);
                }
            }
        });
        Handler { sub: self }
    }

    fn unsub(&mut self) -> io::Result<()> {
        self.do_unsub = false;
        self.shared_state.subs.write().remove(&self.sid);

        self.shared_state.outbound.send_unsub(self.sid)
    }

    /// Unsubscribe a subscription.
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
    pub fn unsubscribe(mut self) -> io::Result<()> {
        self.unsub()
    }

    /// Close a subscription. Same as `unsubscribe`
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
    pub fn close(mut self) -> io::Result<()> {
        self.unsub()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.do_unsub {
            if let Err(error) = self.unsub() {
                eprintln!("error unsubscribing during Subscription Drop: {:?}", error);
            }
        }
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
    pub fn unsubscribe(mut self) -> io::Result<()> {
        self.sub.unsub()
    }

    /// Close a subscription. Same as `unsubscribe`
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let nc = nats::connect("demo.nats.io")?;
    /// let sub = nc.subscribe("foo")?.with_handler(move |msg| {
    ///     println!("Received {}", &msg);
    ///     Ok(())
    /// });
    /// sub.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(mut self) -> io::Result<()> {
        self.sub.unsub()
    }
}

/// A non-blocking iterator over messages from a `Subscription`
pub struct TryIter<'a> {
    subscription: &'a Subscription,
}

impl<'a> Iterator for TryIter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.recv.try_recv().ok()
    }
}

/// An iterator over messages from a `Subscription`
pub struct Iter<'a> {
    subscription: &'a Subscription,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.recv.recv().ok()
    }
}
/// An iterator over messages from a `Subscription`
pub struct IntoIter {
    subscription: Subscription,
}

impl Iterator for IntoIter {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.recv.recv().ok()
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
        self.subscription.recv.recv_timeout(self.to).ok()
    }
}
