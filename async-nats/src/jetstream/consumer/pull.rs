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

use bytes::Bytes;
use futures::future::BoxFuture;
use std::{task::Poll, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    jetstream::{self, Context},
    Error, StatusCode, Subscriber,
};

use super::{AckPolicy, Consumer, DeliverPolicy, FromConsumer, IntoConsumerConfig, ReplayPolicy};
use jetstream::consumer;

impl Consumer<Config> {
    /// Returns a stream of messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_or_create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// jetstream.publish("events".to_string(), "data".into()).await?;
    ///
    /// let consumer = stream.get_or_create_consumer("consumer", async_nats::jetstream::consumer::pull::Config {
    ///     durable_name: Some("consumer".to_string()),
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let mut messages = consumer.stream().await?.take(100);
    /// while let Some(Ok(message)) = messages.next().await {
    ///   println!("got message {:?}", message);
    ///   message.ack().await?;
    /// }
    /// Ok(())
    /// # }
    /// ```
    pub async fn stream(&self) -> Result<Stream<'_>, Error> {
        Stream::stream(
            BatchConfig {
                batch: 100,
                expires: Some(Duration::from_secs(30).as_nanos().try_into().unwrap()),
                no_wait: false,
                max_bytes: 0,
                idle_heartbeat: Duration::default(),
            },
            self,
        )
        .await
    }

    /// Enables customization of all types of [futures::Stream] by setting timeouts, hearbeats, maximum number of
    /// messages or bytes buffered.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.iterator_builder()
    ///     .max_messages_per_batch(100)
    ///     .max_bytes_per_batch(1024)
    ///     .into_stream().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    pub fn iterator_builder(&self) -> IteratorBuilder<'_> {
        IteratorBuilder::new(self)
    }

    pub(crate) async fn request_batch<I: Into<BatchConfig>>(
        &self,
        batch: I,
        inbox: String,
    ) -> Result<(), Error> {
        let subject = format!(
            "{}.CONSUMER.MSG.NEXT.{}.{}",
            self.context.prefix, self.info.stream_name, self.info.name
        );

        let payload = serde_json::to_vec(&batch.into())?;

        self.context
            .client
            .publish_with_reply(subject, inbox, payload.into())
            .await?;
        Ok(())
    }

    /// Returns a batch of specified number of messages, or if there are less messages on the
    /// [Stream] than requested, returns all available messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_or_create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// jetstream.publish("events".to_string(), "data".into()).await?;
    ///
    /// let consumer = stream.get_or_create_consumer("consumer", async_nats::jetstream::consumer::pull::Config {
    ///     durable_name: Some("consumer".to_string()),
    ///     ..Default::default()
    /// }).await?;
    ///
    /// for _ in 0..100 {
    ///     jetstream.publish("events".to_string(), "data".into()).await?;
    /// }
    ///
    /// let mut messages = consumer.fetch(200).await?;
    /// // will finish after 100 messages, as that is the number of messages available on the
    /// // stream.
    /// while let Some(Ok(message)) = messages.next().await {
    ///   println!("got message {:?}", message);
    ///   message.ack().await?;
    /// }
    /// Ok(())
    /// # }
    /// ```
    pub async fn fetch(&self, batch: usize) -> Result<Batch, Error> {
        Batch::batch(
            BatchConfig {
                batch,
                expires: None,
                no_wait: true,
                ..Default::default()
            },
            self,
        )
        .await
    }

    /// Returns a batch of specified number of messages unless timeout happens first.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_or_create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// jetstream.publish("events".to_string(), "data".into()).await?;
    ///
    /// let consumer = stream.get_or_create_consumer("consumer", async_nats::jetstream::consumer::pull::Config {
    ///     durable_name: Some("consumer".to_string()),
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let mut messages = consumer.batch(100, None).await?;
    /// while let Some(Ok(message)) = messages.next().await {
    ///   println!("got message {:?}", message);
    ///   message.ack().await?;
    /// }
    /// Ok(())
    /// # }
    /// ```
    pub async fn batch(&self, batch: usize, expires: Option<usize>) -> Result<Batch, Error> {
        Batch::batch(
            BatchConfig {
                batch,
                expires,
                no_wait: false,
                idle_heartbeat: Duration::default(),
                ..Default::default()
            },
            self,
        )
        .await
    }

    /// Returns a sequence of [Batches][Batch] allowing for iterating over batches, and then over
    /// messages in those batches.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_or_create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// jetstream.publish("events".to_string(), "data".into()).await?;
    ///
    /// let consumer = stream.get_or_create_consumer("consumer", async_nats::jetstream::consumer::pull::Config {
    ///     durable_name: Some("consumer".to_string()),
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let mut iter = consumer.sequence(50).unwrap().take(10);
    /// while let Ok(Some(mut batch)) = iter.try_next().await {
    ///     while let Ok(Some(message)) = batch.try_next().await {
    ///         println!("message received: {:?}", message);
    ///     }
    /// }
    /// Ok(())
    /// # }
    /// ```
    pub fn sequence(&self, batch: usize) -> Result<Sequence, Error> {
        let context = self.context.clone();
        let subject = format!(
            "{}.CONSUMER.MSG.NEXT.{}.{}",
            self.context.prefix, self.info.stream_name, self.info.name
        );

        let request = serde_json::to_vec(&BatchConfig {
            batch,
            ..Default::default()
        })
        .map(Bytes::from)?;

        Ok(Sequence {
            context,
            subject,
            request,
            pending_messages: batch,
            next: None,
        })
    }
}

pub struct Batch {
    pending_messages: usize,
    subscriber: Subscriber,
    context: Context,
}

impl<'a> Batch {
    async fn batch(batch: BatchConfig, consumer: &Consumer<Config>) -> Result<Batch, Error> {
        let inbox = consumer.context.client.new_inbox();
        let subscription = consumer.context.client.subscribe(inbox.clone()).await?;
        consumer.request_batch(batch, inbox.clone()).await?;

        Ok(Batch {
            pending_messages: batch.batch,
            subscriber: subscription,
            context: consumer.context.clone(),
        })
    }
}

impl futures::Stream for Batch {
    type Item = Result<jetstream::Message, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.pending_messages == 0 {
            return std::task::Poll::Ready(None);
        }

        match self.subscriber.receiver.poll_recv(cx) {
            Poll::Ready(maybe_message) => match maybe_message {
                Some(message) => match message.status.unwrap_or(StatusCode::OK) {
                    StatusCode::TIMEOUT => Poll::Ready(None),
                    StatusCode::IDLE_HEARBEAT => Poll::Pending,
                    StatusCode::OK => {
                        self.pending_messages -= 1;
                        Poll::Ready(Some(Ok(jetstream::Message {
                            context: self.context.clone(),
                            message,
                        })))
                    }
                    status => {
                        return Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "eror while processing messages from the stream: {}, {:?}",
                                status, message.description
                            ),
                        )))))
                    }
                },
                None => Poll::Ready(None),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub struct Sequence<'a> {
    context: Context,
    subject: String,
    request: Bytes,
    pending_messages: usize,
    next: Option<BoxFuture<'a, Result<Batch, Error>>>,
}

impl<'a> futures::Stream for Sequence<'a> {
    type Item = Result<Batch, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.next.as_mut() {
            None => {
                let context = self.context.clone();
                let subject = self.subject.clone();
                let request = self.request.clone();
                let pending_messages = self.pending_messages;

                self.next = Some(Box::pin(async move {
                    let inbox = context.client.new_inbox();
                    let subscriber = context.client.subscribe(inbox.clone()).await?;

                    context
                        .client
                        .publish_with_reply(subject, inbox, request)
                        .await?;

                    Ok(Batch {
                        pending_messages,
                        subscriber,
                        context,
                    })
                }));

                match self.next.as_mut().unwrap().as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        self.next = None;
                        Poll::Ready(Some(result))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }

            Some(next) => match next.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    self.next = None;
                    Poll::Ready(Some(result))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

pub struct Stream<'a> {
    pending_messages: usize,
    subscriber: Subscriber,
    context: Context,
    inbox: String,
    subject: String,
    batch_config: BatchConfig,
    request: Option<BoxFuture<'a, Result<(), Error>>>,
}

impl<'a> Stream<'a> {
    async fn stream(
        batch_config: BatchConfig,
        consumer: &Consumer<Config>,
    ) -> Result<Stream<'a>, Error> {
        let inbox = consumer.context.client.new_inbox();
        let subscription = consumer.context.client.subscribe(inbox.clone()).await?;
        let subject = format!(
            "{}.CONSUMER.MSG.NEXT.{}.{}",
            consumer.context.prefix, consumer.info.stream_name, consumer.info.name
        );

        Ok(Stream {
            pending_messages: 0,
            subscriber: subscription,
            context: consumer.context.clone(),
            request: None,
            inbox,
            subject,
            batch_config,
        })
    }
}

impl<'a> futures::Stream for Stream<'a> {
    type Item = Result<jetstream::Message, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match self.request.as_mut() {
                None => {
                    let context = self.context.clone();
                    let inbox = self.inbox.clone();
                    let subject = self.subject.clone();

                    let next_request_threshold =
                        self.pending_messages < std::cmp::min(self.batch_config.batch / 2, 100);

                    if next_request_threshold {
                        let batch = self.batch_config;
                        self.pending_messages += batch.batch;
                        self.request = Some(Box::pin(async move {
                            let request = serde_json::to_vec(&batch).map(Bytes::from)?;

                            context
                                .client
                                .publish_with_reply(subject, inbox, request)
                                .await?;
                            Ok(())
                        }));
                    }

                    if let Some(request) = self.request.as_mut() {
                        match request.as_mut().poll(cx) {
                            Poll::Ready(result) => {
                                self.request = None;
                                result?;
                            }
                            Poll::Pending => {}
                        }
                    }
                }

                Some(request) => match request.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        self.request = None;
                        result?;
                    }
                    Poll::Pending => {}
                },
            }
            match self.subscriber.receiver.poll_recv(cx) {
                Poll::Ready(maybe_message) => match maybe_message {
                    Some(message) => match message.status.unwrap_or(StatusCode::OK) {
                        StatusCode::TIMEOUT => {
                            self.pending_messages = 0;
                            continue;
                        }
                        StatusCode::IDLE_HEARBEAT => {}
                        StatusCode::OK => {
                            self.pending_messages -= 1;
                            return Poll::Ready(Some(Ok(jetstream::Message {
                                context: self.context.clone(),
                                message,
                            })));
                        }
                        status => {
                            return Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!(
                                    "eror while processing messages from the stream: {}, {:?}",
                                    status, message.description
                                ),
                            )))))
                        }
                    },
                    None => return Poll::Ready(None),
                },
                Poll::Pending => {
                    return std::task::Poll::Pending;
                }
            }
        }
    }
}

/// Used for building configuration for a [Stream]. Created by a [Consumer::iterator_builder] on a [Consumer].
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error>  {
/// use futures::StreamExt;
/// let client = async_nats::connect("localhost:4222").await?;
/// let jetstream = async_nats::jetstream::new(client);
///
/// let consumer = jetstream
///     .get_stream("events").await?
///     .get_consumer("pull").await?;
///
/// let mut messages = consumer.iterator_builder()
///     .max_messages_per_batch(100)
///     .max_bytes_per_batch(1024)
///     .into_stream().await?;
///
/// while let Some(message) = messages.next().await {
///     let message = message?;
///     println!("message: {:?}", message);
///     message.ack().await?;
/// }
/// # Ok(())
/// # }
pub struct IteratorBuilder<'a> {
    batch: usize,
    max_bytes: usize,
    hearbeat: Duration,
    expires: usize,
    consumer: &'a Consumer<Config>,
}

impl<'a> IteratorBuilder<'a> {
    pub fn new(consumer: &'a Consumer<Config>) -> Self {
        IteratorBuilder {
            consumer,
            batch: 200,
            max_bytes: 0,
            expires: 0,
            hearbeat: Duration::default(),
        }
    }

    /// Sets max bytes that can be buffered on the Client while processing already received
    /// messages.
    /// Higher values will yield better performance, but also potentially increase memory usage if
    /// application is acknowledging messages much slower than they arrive.
    ///
    /// Default values should provide reasonable balance between performance and memory usage.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.iterator_builder()
    ///     .max_bytes_per_batch(1024)
    ///     .into_stream().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    pub fn max_bytes_per_batch(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Sets max number of messages that can be buffered on the Client while processing already received
    /// messages.
    /// Higher values will yield better performance, but also potentially increase memory usage if
    /// application is acknowledging messages much slower than they arrive.
    ///
    /// Default values should provide reasonable balance between performance and memory usage.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.iterator_builder()
    ///     .max_messages_per_batch(100)
    ///     .into_stream().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    pub fn max_messages_per_batch(mut self, batch: usize) -> Self {
        self.batch = batch;
        self
    }

    /// Sets hearbeat which will be send by the server if there are no messages for a given
    /// [Consumer] pending.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.iterator_builder()
    ///     .hearbeat(std::time::Duration::from_secs(10))
    ///     .into_stream().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    pub fn hearbeat(mut self, hearbeat: Duration) -> Self {
        self.hearbeat = hearbeat;
        self
    }

    /// Low level API that does not need tweaking for most use cases.
    /// Sets how long each batch request waits for whole batch of messages before timing out.
    /// [Consumer] pending.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.iterator_builder()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .into_stream().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    pub fn expires(mut self, expires: Duration) -> Self {
        self.expires = expires.as_nanos().try_into().unwrap();
        self
    }

    /// Creates [Stream] iterator with provided configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.iterator_builder()
    ///     .max_messages_per_batch(100)
    ///     .into_stream().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    pub async fn into_stream(self) -> Result<Stream<'a>, Error> {
        Stream::stream(
            BatchConfig {
                batch: self.batch,
                expires: Some(self.expires),
                no_wait: false,
                max_bytes: self.max_bytes,
                idle_heartbeat: self.hearbeat,
            },
            self.consumer,
        )
        .await
    }

    /// Creates [Batch] iterator with provided configuration.
    ///
    /// This variant will not wait for more messages if given [Consumer] on the server
    /// has less messages already available than requested. In such case, iterator will finish.
    ///
    /// Setting expires and reaching its limit will result in stopped iterator with `timeout`
    /// error.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.iterator_builder()
    ///     .max_messages_per_batch(100)
    ///     .into_fetch().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    pub async fn into_fetch(self) -> Result<Batch, Error> {
        Batch::batch(
            BatchConfig {
                batch: self.batch,
                expires: Some(self.expires),
                no_wait: true,
                max_bytes: self.max_bytes,
                idle_heartbeat: self.hearbeat,
            },
            self.consumer,
        )
        .await
    }

    /// Creates [Batch] iterator with provided configuration.
    /// This variants waits until all messages arrive or timeout happens.
    ///
    /// Setting expires and reaching its limit will result in stopped iterator with `timeout`
    /// error.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events").await?
    ///     .get_consumer("pull").await?;
    ///
    /// let mut messages = consumer.iterator_builder()
    ///     .max_messages_per_batch(100)
    ///     .into_batch().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    pub async fn into_batch(self) -> Result<Batch, Error> {
        Batch::batch(
            BatchConfig {
                batch: self.batch,
                expires: Some(self.expires),
                no_wait: false,
                max_bytes: self.max_bytes,
                idle_heartbeat: self.hearbeat,
            },
            self.consumer,
        )
        .await
    }
}

/// Used for next Pull Request for Pull Consumer
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct BatchConfig {
    /// The number of messages that are being requested to be delivered.
    pub batch: usize,
    /// The optional number of nanoseconds that the server will store this next request for
    /// before forgetting about the pending batch size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<usize>,
    /// This optionally causes the server not to store this pending request at all, but when there are no
    /// messages to deliver will send a nil bytes message with a Status header of 404, this way you
    /// can know when you reached the end of the stream for example. A 409 is returned if the
    /// Consumer has reached MaxAckPending limits.
    #[serde(default, skip_serializing_if = "is_default")]
    pub no_wait: bool,

    /// Sets max number of bytes in total in given batch size. This works together with `batch`.
    /// Whichever value is reached first, batch will complete.
    pub max_bytes: usize,

    /// Setting this other than zero will cause the server to send 100 Idle Hearbeat status to the
    /// client
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub idle_heartbeat: Duration,
}

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Config {
    /// Setting `durable_name` to `Some(...)` will cause this consumer
    /// to be "durable". This may be a good choice for workloads that
    /// benefit from the `JetStream` server or cluster remembering the
    /// progress of consumers for fault tolerance purposes. If a consumer
    /// crashes, the `JetStream` server or cluster will remember which
    /// messages the consumer acknowledged. When the consumer recovers,
    /// this information will allow the consumer to resume processing
    /// where it left off. If you're unsure, set this to `Some(...)`.
    ///
    /// Setting `durable_name` to `None` will cause this consumer to
    /// be "ephemeral". This may be a good choice for workloads where
    /// you don't need the `JetStream` server to remember the consumer's
    /// progress in the case of a crash, such as certain "high churn"
    /// workloads or workloads where a crashed instance is not required
    /// to recover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_name: Option<String>,
    /// A short description of the purpose of this consumer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Allows for a variety of options that determine how this consumer will receive messages
    #[serde(flatten)]
    pub deliver_policy: DeliverPolicy,
    /// How messages should be acknowledged
    pub ack_policy: AckPolicy,
    /// How long to allow messages to remain un-acknowledged before attempting redelivery
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub ack_wait: Duration,
    /// Maximum number of times a specific message will be delivered. Use this to avoid poison pill messages that repeatedly crash your consumer processes forever.
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_deliver: i64,
    /// When consuming from a Stream with many subjects, or wildcards, this selects only specific incoming subjects. Supports wildcards.
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subject: String,
    /// Whether messages are sent as quickly as possible or at the rate of receipt
    pub replay_policy: ReplayPolicy,
    /// The rate of message delivery in bits per second
    #[serde(default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    /// What percentage of acknowledgements should be samples for observability, 0-100
    #[serde(default, skip_serializing_if = "is_default")]
    pub sample_frequency: u8,
    /// The maximum number of waiting consumers.
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_waiting: i64,
    /// The maximum number of unacknowledged messages that may be
    /// in-flight before pausing sending additional messages to
    /// this consumer.
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_ack_pending: i64,
    /// Only deliver headers without payloads.
    #[serde(default, skip_serializing_if = "is_default")]
    pub headers_only: bool,
    /// Maximum size of a request batch
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_batch: i64,
    /// Maximum value for request exiration
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub max_expires: Duration,
    /// Threshold for ephemeral consumer intactivity
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub inactive_threshold: Duration,
}

impl IntoConsumerConfig for &Config {
    fn into_consumer_config(self) -> consumer::Config {
        self.clone().into_consumer_config()
    }
}

impl IntoConsumerConfig for Config {
    fn into_consumer_config(self) -> consumer::Config {
        jetstream::consumer::Config {
            deliver_subject: None,
            durable_name: self.durable_name,
            description: self.description,
            deliver_group: None,
            deliver_policy: self.deliver_policy,
            ack_policy: self.ack_policy,
            ack_wait: self.ack_wait,
            max_deliver: self.max_deliver,
            filter_subject: self.filter_subject,
            replay_policy: self.replay_policy,
            rate_limit: self.rate_limit,
            sample_frequency: self.sample_frequency,
            max_waiting: self.max_waiting,
            max_ack_pending: self.max_ack_pending,
            headers_only: self.headers_only,
            flow_control: false,
            idle_heartbeat: Duration::default(),
            max_batch: self.max_batch,
            max_expires: self.max_expires,
            inactive_threshold: self.inactive_threshold,
        }
    }
}
impl FromConsumer for Config {
    fn try_from_consumer_config(config: consumer::Config) -> Result<Self, Error> {
        if config.deliver_subject.is_some() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "pull consumer cannot have delivery subject",
            )));
        }
        Ok(Config {
            durable_name: config.durable_name,
            description: config.description,
            deliver_policy: config.deliver_policy,
            ack_policy: config.ack_policy,
            ack_wait: config.ack_wait,
            max_deliver: config.max_deliver,
            filter_subject: config.filter_subject,
            replay_policy: config.replay_policy,
            rate_limit: config.rate_limit,
            sample_frequency: config.sample_frequency,
            max_waiting: config.max_waiting,
            max_ack_pending: config.max_ack_pending,
            headers_only: config.headers_only,
            max_batch: config.max_batch,
            max_expires: config.max_expires,
            inactive_threshold: config.inactive_threshold,
        })
    }
}
