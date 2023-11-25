// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use portable_atomic::AtomicU64;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel as channel;

use crate::jetstream::{AckPolicy, ConsumerInfo, ConsumerOwnership, JetStream};
use crate::message::Message;
use crate::DEFAULT_FLUSH_TIMEOUT;

#[derive(Debug)]
pub(crate) struct Inner {
    /// Subscription ID.
    pub(crate) sid: Arc<AtomicU64>,

    /// MSG operations received from the server.
    pub(crate) messages: channel::Receiver<Message>,

    /// Name of the stream associated with the subscription.
    pub(crate) stream: String,

    /// Name of the consumer associated with the subscription.
    pub(crate) consumer: String,

    /// Ack policy used in while processing messages.
    pub(crate) consumer_ack_policy: AckPolicy,

    pub(crate) num_pending: u64,

    /// Indicates if we own the consumer and are responsible for deleting it or not.
    pub(crate) consumer_ownership: ConsumerOwnership,

    /// Client associated with subscription.
    pub(crate) context: JetStream,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.context
            .connection
            .0
            .client
            .unsubscribe(self.sid.load(Ordering::Relaxed))
            .ok();

        // Delete the consumer, if we own it.
        if self.consumer_ownership == ConsumerOwnership::Yes {
            self.context
                .delete_consumer(&self.stream, &self.consumer)
                .ok();
        }
    }
}

/// A `PushSubscription` receives `Message`s published
/// to specific NATS `Subject`s.
#[derive(Clone, Debug)]
pub struct PushSubscription(pub(crate) Arc<Inner>);

impl PushSubscription {
    /// Creates a subscription.
    pub(crate) fn new(
        sid: Arc<AtomicU64>,
        consumer_info: ConsumerInfo,
        consumer_ownership: ConsumerOwnership,
        messages: channel::Receiver<Message>,
        context: JetStream,
    ) -> PushSubscription {
        PushSubscription(Arc::new(Inner {
            sid,
            stream: consumer_info.stream_name,
            consumer: consumer_info.name,
            consumer_ack_policy: consumer_info.config.ack_policy,
            num_pending: consumer_info.num_pending,
            consumer_ownership,
            messages,
            context,
        }))
    }

    /// Preprocesses the given message.
    /// Returns true if the message was processed and should be filtered out from the user's view.
    fn preprocess(&self, message: &Message) -> bool {
        if message.is_flow_control() {
            message.respond(b"").ok();

            return true;
        }

        if message.is_idle_heartbeat() {
            return true;
        }

        false
    }

    /// Get the next message non-protocol message, or None if the subscription has been
    /// unsubscribed or the connection closed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("next")?;
    /// # context.publish("next", "hello")?;
    ///
    /// # let subscription = context.subscribe("next")?;
    /// if let Some(message) = subscription.next() {
    ///     println!("Received {}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn next(&self) -> Option<Message> {
        loop {
            return match self.0.messages.recv().ok() {
                Some(message) => {
                    if self.preprocess(&message) {
                        continue;
                    }

                    Some(message)
                }
                None => None,
            };
        }
    }

    /// Try to get the next non-protocol message, or None if no messages
    /// are present or if the subscription has been unsubscribed
    /// or the connection closed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("try_next");
    /// # let subscription = context.subscribe("try_next")?;
    /// if let Some(message) = subscription.try_next() {
    ///     println!("Received {}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_next(&self) -> Option<Message> {
        loop {
            return match self.0.messages.try_recv().ok() {
                Some(message) => {
                    if self.preprocess(&message) {
                        continue;
                    }

                    Some(message)
                }
                None => None,
            };
        }
    }

    /// Get the next message, or a timeout error
    /// if no messages are available for timeout.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// # context.add_stream("next_timeout");
    /// # let subscription = context.subscribe("next_timeout")?;
    /// if let Ok(message) = subscription.next_timeout(std::time::Duration::from_secs(1)) {
    ///     println!("Received {}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_timeout(&self, mut timeout: Duration) -> io::Result<Message> {
        loop {
            let start = Instant::now();
            return match self.0.messages.recv_timeout(timeout) {
                Ok(message) => {
                    if self.preprocess(&message) {
                        timeout = timeout.saturating_sub(start.elapsed());
                        continue;
                    }

                    Ok(message)
                }
                Err(channel::RecvTimeoutError::Timeout) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "next_timeout: timed out",
                )),
                Err(channel::RecvTimeoutError::Disconnected) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    "next_timeout: unsubscribed",
                )),
            };
        }
    }

    /// Returns a blocking message iterator.
    /// Same as calling `iter()`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// # context.add_stream("iter");
    /// # let subscription = context.subscribe("messages")?;
    /// for message in subscription.messages() {
    ///     println!("Received message {:?}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn messages(&self) -> Iter<'_> {
        Iter { subscription: self }
    }

    /// Returns a blocking message iterator.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// # context.add_stream("iter");
    /// #
    /// # let subscription = context.subscribe("iter")?;
    /// for message in subscription.iter() {}
    /// # Ok(())
    /// # }
    /// ```
    pub fn iter(&self) -> Iter<'_> {
        Iter { subscription: self }
    }

    /// Returns a non-blocking message iterator.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// # context.add_stream("try_iter");
    /// #
    /// # let subscription = context.subscribe("try_iter")?;
    /// for message in subscription.try_iter() {}
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
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("timeout_iter");
    /// # context.publish("timeout_iter", b"hello timeout");
    /// #
    /// # let subscription = context.subscribe("timeout_iter")?;
    /// #
    /// for message in subscription.timeout_iter(std::time::Duration::from_secs(1)) {
    ///     println!("Received message {:?}", message);
    /// }
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
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// # context.add_stream("with_handler");
    /// context
    ///     .subscribe("with_handler")?
    ///     .with_handler(move |message| {
    ///         println!("received {}", &message);
    ///         Ok(())
    ///     });
    ///
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
            .name(format!(
                "nats_jetstream_push_subscriber_{}_{}",
                self.0.stream, self.0.consumer,
            ))
            .spawn(move || {
                for m in &sub {
                    if let Err(e) = handler(m) {
                        // TODO(dlc) - Capture for last error?
                        log::error!("Error in callback! {:?}", e);
                    }
                }
            })
            .expect("threads should be spawnable");

        Handler { subscription: self }
    }

    /// Attach a closure to process and acknowledge messages. This closure will execute in a separate thread.
    ///
    /// The result of this call is a `Handler`
    /// which can not be iterated and must be unsubscribed or closed directly to
    /// unregister interest. A `Handler` will not unregister interest with
    /// the server when `drop(&mut self)` is called.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// # context.add_stream("with_process_handler");
    /// context
    ///     .subscribe("with_process_handler")?
    ///     .with_process_handler(|message| {
    ///         println!("Received {}", &message);
    ///
    ///         Ok(())
    ///     });
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_process_handler<F>(self, handler: F) -> Handler
    where
        F: Fn(&Message) -> io::Result<()> + Send + 'static,
    {
        let consumer_ack_policy = self.0.consumer_ack_policy;

        // This will allow us to not have to capture the return. When it is
        // dropped it will not unsubscribe from the server.
        let sub = self.clone();
        thread::Builder::new()
            .name(format!(
                "nats_push_subscriber_{}_{}",
                self.0.consumer, self.0.stream
            ))
            .spawn(move || {
                for message in &sub {
                    if let Err(err) = handler(&message) {
                        log::error!("Error in callback! {:?}", err);
                    }

                    if consumer_ack_policy != AckPolicy::None {
                        if let Err(err) = message.ack() {
                            log::error!("Error in callback! {:?}", err);
                        }
                    }
                }
            })
            .expect("threads should be spawnable");

        Handler { subscription: self }
    }

    /// Process and acknowledge a single message, waiting indefinitely for
    /// one to arrive.
    ///
    /// Does not acknowledge the processed message if the closure returns an `Err`.
    ///
    /// Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("process")?;
    /// # context.publish("process", "hello")?;
    /// #
    /// let mut subscription = context.subscribe("process")?;
    /// subscription.process(|message| {
    ///     println!("Received message {:?}", message);
    ///
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn process<R, F: Fn(&Message) -> io::Result<R>>(&mut self, f: F) -> io::Result<R> {
        let next = self.next().unwrap();

        let result = f(&next)?;
        if self.0.consumer_ack_policy != AckPolicy::None {
            next.ack()?;
        }

        Ok(result)
    }

    /// Process and acknowledge a single message, waiting up to timeout configured `timeout` before
    /// returning a timeout error.
    ///
    /// Does not ack the processed message if the internal closure returns an `Err`.
    ///
    /// Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("process_timeout")?;
    /// # context.publish("process_timeout", "hello")?;
    /// #
    /// let mut subscription = context.subscribe("process_timeout")?;
    /// subscription.process_timeout(std::time::Duration::from_secs(1), |message| {
    ///     println!("Received message {:?}", message);
    ///
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn process_timeout<R, F: Fn(&Message) -> io::Result<R>>(
        &mut self,
        timeout: Duration,
        f: F,
    ) -> io::Result<R> {
        let next = self.next_timeout(timeout)?;

        let ret = f(&next)?;
        if self.0.consumer_ack_policy != AckPolicy::None {
            next.ack()?;
        }

        Ok(ret)
    }

    /// Sends a request to fetch current information about the target consumer.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("consumer_info")?;
    /// let subscription = context.subscribe("consumer_info")?;
    /// let info = subscription.consumer_info()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn consumer_info(&self) -> io::Result<ConsumerInfo> {
        self.0
            .context
            .consumer_info(&self.0.stream, &self.0.consumer)
    }

    /// Unsubscribe a subscription immediately without draining.
    /// Use `drain` instead if you want any pending messages
    /// to be processed by a handler, if one is configured.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("unsubscribe")?;
    /// #
    /// let subscription = context.subscribe("unsubscribe")?;
    /// subscription.unsubscribe()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn unsubscribe(self) -> io::Result<()> {
        self.0
            .context
            .connection
            .0
            .client
            .unsubscribe(self.0.sid.load(Ordering::Relaxed))?;

        // Discard all queued messages.
        while self.0.messages.try_recv().is_ok() {}

        // Delete the consumer, if we own it.
        if self.0.consumer_ownership == ConsumerOwnership::Yes {
            self.0
                .context
                .delete_consumer(&self.0.stream, &self.0.consumer)
                .ok();
        }

        Ok(())
    }

    /// Close a subscription. Same as `unsubscribe`
    ///
    /// Use `drain` instead if you want any pending messages
    /// to be processed by a handler, if one is configured.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// # context.add_stream("close")?;
    /// let subscription = context.subscribe("close")?;
    /// subscription.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(self) -> io::Result<()> {
        self.unsubscribe()
    }

    /// Send an unsubscription and flush the connection,
    /// allowing any unprocessed messages to be handled
    /// by a `Subscription`
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
    /// ```no_run
    /// # use std::sync::{Arc, atomic::{AtomicBool, Ordering::SeqCst}};
    /// # use std::thread;
    /// # use std::time::Duration;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// # context.add_stream("drain")?;
    /// let mut subscription = context.subscribe("drain")?;
    ///
    /// context.publish("drain", "foo")?;
    /// context.publish("drain", "bar")?;
    /// context.publish("drain", "baz")?;
    ///
    /// subscription.drain()?;
    ///
    /// assert!(subscription.next().is_some());
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn drain(&self) -> io::Result<()> {
        // Unsubscribe
        self.0
            .context
            .connection
            .0
            .client
            .flush(DEFAULT_FLUSH_TIMEOUT)?;

        self.0
            .context
            .connection
            .0
            .client
            .unsubscribe(self.0.sid.load(Ordering::Relaxed))?;

        // Delete the consumer, if we own it.
        if self.0.consumer_ownership == ConsumerOwnership::Yes {
            self.0
                .context
                .delete_consumer(&self.0.stream, &self.0.consumer)
                .ok();
        }

        Ok(())
    }
}

impl IntoIterator for PushSubscription {
    type Item = Message;
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        IntoIter { subscription: self }
    }
}

impl<'a> IntoIterator for &'a PushSubscription {
    type Item = Message;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        Iter { subscription: self }
    }
}

/// A `Handler` may be used to unsubscribe a handler thread.
pub struct Handler {
    subscription: PushSubscription,
}

impl Handler {
    /// Unsubscribe a subscription.
    ///
    /// # Example
    /// ```no_run
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
        self.subscription.unsubscribe()
    }
}

/// A non-blocking iterator over messages from a `PushSubscription`
pub struct TryIter<'a> {
    subscription: &'a PushSubscription,
}

impl<'a> Iterator for TryIter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.try_next()
    }
}

/// An iterator over messages from a `PushSubscription`
pub struct Iter<'a> {
    subscription: &'a PushSubscription,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.next()
    }
}

/// An iterator over messages from a `PushSubscription`
pub struct IntoIter {
    subscription: PushSubscription,
}

impl Iterator for IntoIter {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.next()
    }
}

/// An iterator over messages from a `PushSubscription`
/// where `None` will be returned if a new `Message`
/// has not been received by the end of a timeout.
pub struct TimeoutIter<'a> {
    subscription: &'a PushSubscription,
    to: Duration,
}

impl<'a> Iterator for TimeoutIter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.next_timeout(self.to).ok()
    }
}
