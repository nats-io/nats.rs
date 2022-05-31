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

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::jetstream::{ConsumerInfo, ConsumerOwnership, JetStream};
use crate::Message;

use super::{AckPolicy, BatchOptions};
use crossbeam_channel as channel;

#[derive(Debug)]
pub(crate) struct Inner {
    pid: u64,

    /// messages channel for this subscription.
    pub(crate) messages: channel::Receiver<Message>,

    /// sid of the inbox subscription
    pub(crate) inbox: String,

    /// Ack policy used in methods that automatically ack.
    pub(crate) consumer_ack_policy: AckPolicy,

    /// Name of the stream associated with the subscription.
    pub(crate) info: ConsumerInfo,

    /// Indicates if we own the consumer and are responsible for deleting it or not.
    pub(crate) consumer_ownership: ConsumerOwnership,

    /// Client associated with subscription.
    pub(crate) context: JetStream,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.context.connection.0.client.unsubscribe(self.pid).ok();

        // Delete the consumer, if we own it.
        if self.consumer_ownership == ConsumerOwnership::Yes {
            self.context
                .delete_consumer(&self.info.stream_name, &self.info.name)
                .ok();
        }
    }
}

/// A `PullSubscription` pulls messages from Server triggered by client actions
/// Pull Subscription does nothing on itself. It has to explicitly request messages
/// using one of available
#[derive(Clone, Debug)]
pub struct PullSubscription(pub(crate) Arc<Inner>);

impl PullSubscription {
    /// Creates a subscription.
    pub(crate) fn new(
        pid: u64,
        consumer_info: ConsumerInfo,
        consumer_ownership: ConsumerOwnership,
        inbox: String,
        messages: channel::Receiver<Message>,
        context: JetStream,
    ) -> PullSubscription {
        PullSubscription(Arc::new(Inner {
            pid,
            inbox,
            messages,
            consumer_ownership,
            consumer_ack_policy: consumer_info.config.ack_policy,
            info: consumer_info,
            context,
        }))
    }

    /// Fetch given amount of messages for `PullSubscription` and return Iterator
    /// to handle them. The returned iterator is blocking, meaning it will wait until
    /// every message from the batch are processed.
    /// It can accept either `usize` defined size of the batch, or `BatchOptions` defining
    /// also `expires` and `no_wait`.
    /// If `no_wait` will be specified, iterator will also return when there are no more messages
    /// in the Consumer.
    ///
    /// # Example
    /// ```
    /// # use nats::jetstream::BatchOptions;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("next")?;
    /// # for _ in 0..20 {
    /// #    context.publish("next", "hello")?;
    /// # }
    /// let consumer = context.pull_subscribe("next")?;
    ///
    /// // pass just number of messages to be fetched
    /// for message in consumer.fetch(10)? {
    ///     println!("received message: {:?}", message);
    /// }
    ///
    /// // pass whole `BatchOptions` to fetch
    /// let messages = consumer.fetch(BatchOptions{
    ///     expires: None,
    ///     no_wait: false,
    ///     batch: 10,
    /// })?;
    /// for message in messages {
    ///     println!("received message {:?}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn fetch<I: Into<BatchOptions>>(&self, batch: I) -> io::Result<BatchIter<'_>> {
        let batch_options = batch.into();
        self.request_batch(batch_options)?;
        Ok(BatchIter {
            batch_size: batch_options.batch,
            processed: 0,
            subscription: self,
        })
    }

    /// Fetch given amount of messages for `PullSubscription` and return Iterator
    /// to handle them. The returned iterator is will retrieve message or wait for new ones for
    /// a given set of time.
    /// It will stop when all messages for given batch are processed.
    /// That can happen if there are no more messages in the stream, or when iterator processed
    /// number of messages specified in batch.
    /// It can accept either `usize` defined size of the batch, or `BatchOptions` defining
    /// also `expires` and `no_wait`.
    /// If `no_wait` will be specified, iterator will also return when there are no more messages
    /// in the Consumer.
    ///
    /// # Example
    /// ```
    /// # use std::time::Duration;
    /// # use nats::jetstream::BatchOptions;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("timeout_fetch")?;
    /// # for _ in 0..20 {
    /// #    context.publish("timeout_fetch", "hello")?;
    /// # }
    /// let consumer = context.pull_subscribe("timeout_fetch")?;
    ///
    /// // pass just number of messages to be fetched
    /// for message in consumer.timeout_fetch(10, Duration::from_millis(100))? {
    ///     println!("received message: {:?}", message);
    /// }
    ///
    /// // pass whole `BatchOptions` to fetch
    /// let messages = consumer.timeout_fetch(BatchOptions{
    ///     expires: None,
    ///     no_wait: false,
    ///     batch: 10,
    /// }, Duration::from_millis(100))?;
    /// for message in messages {
    ///     println!("received message {:?}", message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn timeout_fetch<I: Into<BatchOptions>>(
        &self,
        batch: I,
        timeout: Duration,
    ) -> io::Result<TimeoutBatchIter<'_>> {
        let batch_options = batch.into();
        self.request_batch(batch_options)?;
        Ok(TimeoutBatchIter {
            timeout,
            batch_size: batch_options.batch,
            processed: 0,
            subscription: self,
        })
    }

    /// High level method that fetches given set of messages, processes them in user-provider
    /// closure and acks them automatically according to `Consumer` `AckPolicy`.
    ///
    /// # Example
    /// ```
    /// # use nats::jetstream::BatchOptions;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("fetch_with_handler")?;
    /// # for _ in 0..20 {
    /// #    context.publish("fetch_with_handler", "hello")?;
    /// # }
    /// let consumer = context.pull_subscribe("fetch_with_handler")?;
    ///
    /// consumer.fetch_with_handler(10, |message| {
    ///     println!("received message: {:?}", message);
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn fetch_with_handler<F, I>(&self, batch: I, mut handler: F) -> io::Result<()>
    where
        F: FnMut(&Message) -> io::Result<()>,
        I: Into<BatchOptions> + Copy,
    {
        let mut last_message;
        let consumer_ack_policy = self.0.consumer_ack_policy;
        let batch = self.fetch(batch)?;
        for message in batch {
            handler(&message)?;
            if consumer_ack_policy != AckPolicy::None {
                message.ack()?
            }
            last_message = Some(message);
            // if the policy is ack all - optimize and send the ack
            // after the last message was processed.
            if consumer_ack_policy == AckPolicy::All {
                if let Some(last_message) = last_message {
                    last_message.ack()?;
                }
            }
        }
        Ok(())
    }

    /// A low level method that should be used only in specific cases.
    /// Pulls next message available for this `PullSubscription`.
    /// This operation is blocking and will indefinately wait for new messages.
    /// Keep in mind that this requires user to request for messages first.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("next")?;
    /// # context.publish("next", "hello")?;
    /// let consumer = context.pull_subscribe("next")?;
    /// consumer.request_batch(1)?;
    /// let message = consumer.next();
    /// println!("Received message: {:?}", message);
    /// # Ok(())
    /// # }
    /// ```
    pub fn next(&self) -> Option<Message> {
        self.preprocess(self.0.messages.recv().ok())
    }

    /// A low level method that should be used only in specific cases.
    /// Pulls next message available for this `PullSubscription`.
    /// This operation is non blocking, that will yield `None` if there are no messages each time
    /// it is called.
    /// Keep in mind that this requires user to request for messages first.
    ///
    /// # Example
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("try_next")?;
    /// context.publish("try_next", "hello")?;
    /// let consumer = context.pull_subscribe("try_next")?;
    /// consumer.request_batch(1)?;
    /// let message = consumer.try_next();
    /// println!("Received message: {:?}", message);
    /// assert!(consumer.try_next().is_none());
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_next(&self) -> Option<Message> {
        self.preprocess(self.0.messages.try_recv().ok())
    }

    /// A low level method that should be used only in specific cases.
    /// Pulls next message available for this `PullSubscription`.
    /// This operation is contrast to its siblings `next` and `try_next` returns `Message` wrapped
    /// in `io::Result` as it might return `timeout` error, either on waiting for next message, or
    /// network.
    /// Keep in mind that this requires user to request for messages first.
    ///
    /// # Example
    /// ```
    /// # use std::time::Duration;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("next_timeout")?;
    /// # context.publish("next_timeout", "hello")?;
    /// # context.publish("next_timeout", "hello")?;
    /// let consumer = context.pull_subscribe("next_timeout")?;
    ///
    /// consumer.request_batch(1)?;
    ///
    /// let message = consumer.next_timeout(Duration::from_millis(1000))?;
    /// println!("Received message: {:?}", message);
    ///
    /// // timeout on second, as there are no messages.
    /// let message = consumer.next_timeout(Duration::from_millis(100));
    /// assert!(message.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_timeout(&self, mut timeout: Duration) -> io::Result<Message> {
        loop {
            let start = Instant::now();
            return match self.0.messages.recv_timeout(timeout) {
                Ok(message) => {
                    if message.is_no_messages() {
                        timeout = timeout.saturating_sub(start.elapsed());
                        continue;
                    }
                    if message.is_request_timeout() {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "next_timeout: Pull Request timed out",
                        ));
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

    /// Sends request for another set of messages to Pull Consumer.
    /// This method does not return any messages. It can be used
    /// to have more granular control of how many request and when are sent.
    ///
    /// # Example
    /// ```
    /// # use nats::jetstream::BatchOptions;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client);
    /// #
    /// # context.add_stream("request_batch")?;
    ///
    /// let consumer = context.pull_subscribe("request_batch")?;
    /// // request specific number of messages.
    /// consumer.request_batch(10)?;
    ///
    /// // request messages specifying whole config.
    /// consumer.request_batch(BatchOptions{
    ///     expires: None,
    ///     no_wait: false,
    ///     batch: 10,
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_batch<I: Into<BatchOptions>>(&self, batch: I) -> io::Result<()> {
        let batch_opts = batch.into();

        let subject = format!(
            "{}CONSUMER.MSG.NEXT.{}.{}",
            self.0.context.api_prefix(),
            self.0.info.stream_name,
            self.0.info.name,
        );

        let request = serde_json::to_vec(&batch_opts)?;

        self.0.context.connection.publish_with_reply_or_headers(
            &subject,
            Some(self.0.inbox.as_str()),
            None,
            request,
        )?;
        Ok(())
    }

    /// Low level API that should be used with care.
    /// For standard use cases consider using [`PullSubscription::fetch`] or [`PullSubscription::fetch_with_handler`].
    /// Returns iterator for Current Subscription.
    /// As Pull Consumers requires Client to fetch messages, this will yield nothing if explicit [`PullSubscription::request_batch`] was not sent.
    ///
    /// # Example
    /// ```
    /// # use nats::jetstream::BatchOptions;
    /// # fn main() -> std::io::Result<()> {
    /// # let client = nats::connect("demo.nats.io")?;
    /// # let context = nats::jetstream::new(client.clone());
    /// #
    /// # context.add_stream("iter")?;
    /// # for i in 0..20 {
    /// # client.publish("iter", b"data")?;
    /// # }
    ///
    /// let consumer = context.pull_subscribe("iter")?;
    /// // request specific number of messages.
    /// consumer.request_batch(10)?;
    ///
    /// // request messages specifying whole config.
    /// consumer.request_batch(BatchOptions{
    ///     expires: Some(10000),
    ///     no_wait: true,
    ///     batch: 10,
    /// })?;
    /// for (i, message) in consumer.iter().enumerate() {
    ///     println!("recieved message: {:?}", message);
    ///     message.ack()?;
    /// #   break;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn iter(&self) -> Iter<'_> {
        Iter { subscription: self }
    }

    /// utility to stop iterators if `no messages` or `request timeout` is encountered.
    fn preprocess(&self, message: Option<Message>) -> Option<Message> {
        if let Some(message) = message {
            if message.is_no_messages() {
                return None;
            }
            if message.is_request_timeout() {
                return None;
            }
            return Some(message);
        }
        message
    }
}

/// Interator that will endlessly wait for messages, unless `no messages` or `request timeout` is encountered.
pub struct Iter<'a> {
    subscription: &'a PullSubscription,
}
impl<'a> Iterator for Iter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        self.subscription.next()
    }
}

/// Iterator that retrieves messages unless `no messages` or `request timeout` is enocuntered, or
/// timeout is reached.
pub struct TimeoutIter<'a> {
    subscription: &'a PullSubscription,
    timeout: Duration,
}
impl<'a> Iterator for TimeoutIter<'a> {
    type Item = io::Result<Message>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(self.subscription.next_timeout(self.timeout))
    }
}

/// Iterator for handling batches of messages. Works like `Iter` except stopping after
/// reading number of messages defined in `batch_size`.
pub struct BatchIter<'a> {
    batch_size: usize,
    processed: usize,
    subscription: &'a PullSubscription,
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        if self.processed >= self.batch_size {
            None
        } else {
            self.processed += 1;
            self.subscription.next()
        }
    }
}

/// Iterator for handling batches of messages. Works like `TryIter` except stopping after
/// reading number of messages defined in `batch_size`.
pub struct TryBatchIter<'a> {
    batch_size: usize,
    processed: usize,
    subscription: &'a PullSubscription,
}

impl<'a> Iterator for TryBatchIter<'a> {
    type Item = Message;
    fn next(&mut self) -> Option<Self::Item> {
        if self.processed == 0 {
            self.processed += 1;
            return self.subscription.next();
        }
        if self.processed >= self.batch_size {
            None
        } else {
            self.processed += 1;
            self.subscription.try_next()
        }
    }
}

/// Iterator for handling batches of messages. Works like `TimeoutIter` except stopping after
/// reading number of messages defined in `batch_size`.
pub struct TimeoutBatchIter<'a> {
    batch_size: usize,
    processed: usize,
    timeout: Duration,
    subscription: &'a PullSubscription,
}

impl<'a> Iterator for TimeoutBatchIter<'a> {
    type Item = io::Result<Message>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.processed >= self.batch_size {
            None
        } else {
            self.processed += 1;
            Some(self.subscription.next_timeout(self.timeout))
        }
    }
}

impl From<usize> for BatchOptions {
    fn from(batch: usize) -> Self {
        BatchOptions {
            batch,
            expires: None,
            no_wait: false,
        }
    }
}
