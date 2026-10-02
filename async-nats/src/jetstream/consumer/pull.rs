// Copyright 2020-2023 The NATS Authors
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
use futures_util::{
    future::{BoxFuture, Either},
    FutureExt, StreamExt,
};

#[cfg(feature = "server_2_11")]
use crate::datetime::{rfc3339, DateTime};

#[cfg(feature = "server_2_10")]
use std::collections::HashMap;
use std::{future, pin::Pin, task::Poll, time::Duration};
use tokio::{task::JoinHandle, time::Sleep};

use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

use crate::{
    connection::State,
    error::Error,
    jetstream::{self, Context},
    StatusCode, SubscribeError, Subscriber,
};

use crate::subject::Subject;

#[cfg(feature = "server_2_11")]
use super::PriorityPolicy;

use super::{
    backoff, poll_missed_heartbeat, AckPolicy, Consumer, DeliverPolicy, FromConsumer,
    IntoConsumerConfig, ReplayPolicy, StreamError, StreamErrorKind,
};
use jetstream::consumer;

impl Consumer<Config> {
    /// Returns a stream of messages for Pull Consumer.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures_util::StreamExt;
    /// use futures_util::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .get_or_create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         max_messages: 10_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// jetstream.publish("events", "data".into()).await?;
    ///
    /// let consumer = stream
    ///     .get_or_create_consumer(
    ///         "consumer",
    ///         async_nats::jetstream::consumer::pull::Config {
    ///             durable_name: Some("consumer".to_string()),
    ///             ..Default::default()
    ///         },
    ///     )
    ///     .await?;
    ///
    /// let mut messages = consumer.messages().await?.take(100);
    /// while let Some(Ok(message)) = messages.next().await {
    ///     println!("got message {:?}", message);
    ///     message.ack().await?;
    /// }
    /// Ok(())
    /// # }
    /// ```
    ///
    /// Each pull request asks for 200 messages, expires after 30 seconds and uses a 15 second
    /// idle heartbeat. The batch and expiry are capped by the consumer's `max_batch` and
    /// `max_expires` when those are lower, and the heartbeat follows a capped expiry down to
    /// half of it. Use [Consumer::stream] to pick other values.
    pub async fn messages(&self) -> Result<Stream, StreamError> {
        self.stream()
            .heartbeat(DEFAULT_IDLE_HEARTBEAT)
            .messages()
            .await
    }

    /// Enables customization of [Stream] by setting timeouts, heartbeats, maximum number of
    /// messages or bytes buffered.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .stream()
    ///     .max_messages_per_batch(100)
    ///     .max_bytes_per_batch(1024)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream(&self) -> StreamBuilder<'_> {
        StreamBuilder::new(self)
    }

    pub async fn request_batch<I: Into<BatchConfig>>(
        &self,
        batch: I,
        inbox: Subject,
    ) -> Result<(), BatchRequestError> {
        debug!("sending batch");
        let subject = format!(
            "{}.CONSUMER.MSG.NEXT.{}.{}",
            self.context.prefix, self.info.stream_name, self.info.name
        );

        let payload = serde_json::to_vec(&batch.into())
            .map_err(|err| BatchRequestError::with_source(BatchRequestErrorKind::Serialize, err))?;

        self.context
            .client
            .publish_with_reply(subject, inbox, payload.into())
            .await
            .map_err(|err| BatchRequestError::with_source(BatchRequestErrorKind::Publish, err))?;
        debug!("batch request sent");
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
    /// use futures_util::StreamExt;
    /// use futures_util::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .get_or_create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         max_messages: 10_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// jetstream.publish("events", "data".into()).await?;
    ///
    /// let consumer = stream
    ///     .get_or_create_consumer(
    ///         "consumer",
    ///         async_nats::jetstream::consumer::pull::Config {
    ///             durable_name: Some("consumer".to_string()),
    ///             ..Default::default()
    ///         },
    ///     )
    ///     .await?;
    ///
    /// for _ in 0..100 {
    ///     jetstream.publish("events", "data".into()).await?;
    /// }
    ///
    /// let mut messages = consumer.fetch().max_messages(200).messages().await?;
    /// // will finish after 100 messages, as that is the number of messages available on the
    /// // stream.
    /// while let Some(Ok(message)) = messages.next().await {
    ///     println!("got message {:?}", message);
    ///     message.ack().await?;
    /// }
    /// Ok(())
    /// # }
    /// ```
    pub fn fetch(&self) -> FetchBuilder<'_> {
        FetchBuilder::new(self)
    }

    /// Returns a batch of specified number of messages unless timeout happens first.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures_util::StreamExt;
    /// use futures_util::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .get_or_create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         max_messages: 10_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// jetstream.publish("events", "data".into()).await?;
    ///
    /// let consumer = stream
    ///     .get_or_create_consumer(
    ///         "consumer",
    ///         async_nats::jetstream::consumer::pull::Config {
    ///             durable_name: Some("consumer".to_string()),
    ///             ..Default::default()
    ///         },
    ///     )
    ///     .await?;
    ///
    /// let mut messages = consumer.batch().max_messages(100).messages().await?;
    /// while let Some(Ok(message)) = messages.next().await {
    ///     println!("got message {:?}", message);
    ///     message.ack().await?;
    /// }
    /// Ok(())
    /// # }
    /// ```
    pub fn batch(&self) -> BatchBuilder<'_> {
        BatchBuilder::new(self)
    }

    /// Returns a sequence of [Batches][Batch] allowing for iterating over batches, and then over
    /// messages in those batches.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures_util::StreamExt;
    /// use futures_util::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .get_or_create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         max_messages: 10_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// jetstream.publish("events", "data".into()).await?;
    ///
    /// let consumer = stream
    ///     .get_or_create_consumer(
    ///         "consumer",
    ///         async_nats::jetstream::consumer::pull::Config {
    ///             durable_name: Some("consumer".to_string()),
    ///             ..Default::default()
    ///         },
    ///     )
    ///     .await?;
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
    pub fn sequence(&self, batch: usize) -> Result<Sequence, BatchError> {
        let context = self.context.clone();
        let subject = format!(
            "{}.CONSUMER.MSG.NEXT.{}.{}",
            self.context.prefix, self.info.stream_name, self.info.name
        );

        let request = serde_json::to_vec(&BatchConfig {
            batch,
            expires: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .map(Bytes::from)
        .map_err(|err| BatchRequestError::with_source(BatchRequestErrorKind::Serialize, err))?;

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
    timeout: Option<Pin<Box<Sleep>>>,
    terminated: bool,
}

impl Batch {
    async fn batch(batch: BatchConfig, consumer: &Consumer<Config>) -> Result<Batch, BatchError> {
        let inbox = Subject::from(consumer.context.client.new_inbox());
        let subscription = consumer.context.client.subscribe(inbox.clone()).await?;
        consumer.request_batch(batch.clone(), inbox.clone()).await?;

        let sleep = batch.expires.map(|expires| {
            Box::pin(tokio::time::sleep(
                expires.saturating_add(Duration::from_secs(5)),
            ))
        });

        Ok(Batch {
            pending_messages: batch.batch,
            subscriber: subscription,
            context: consumer.context.clone(),
            terminated: false,
            timeout: sleep,
        })
    }
}

impl futures_util::Stream for Batch {
    type Item = Result<jetstream::Message, crate::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }
        if self.pending_messages == 0 {
            self.terminated = true;
            return Poll::Ready(None);
        }
        if let Some(sleep) = self.timeout.as_mut() {
            match sleep.poll_unpin(cx) {
                Poll::Ready(_) => {
                    debug!("batch timeout timer triggered");
                    // TODO(tp): Maybe we can be smarter here and before timing out, check if
                    // we consumed all the messages from the subscription buffer in case of user
                    // slowly consuming messages. Keep in mind that we time out here only if
                    // for some reason we missed timeout from the server and few seconds have
                    // passed since expected timeout message.
                    self.terminated = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => (),
            }
        }
        match self.subscriber.receiver.poll_recv(cx) {
            Poll::Ready(maybe_message) => match maybe_message {
                Some(message) => match message.status.unwrap_or(StatusCode::OK) {
                    StatusCode::TIMEOUT => {
                        debug!("received timeout. Iterator done");
                        self.terminated = true;
                        Poll::Ready(None)
                    }
                    StatusCode::IDLE_HEARTBEAT => {
                        debug!("received heartbeat");
                        Poll::Pending
                    }
                    // If this is fetch variant, terminate on no more messages.
                    // We do not need to check if this is a fetch, not batch,
                    // as only fetch will send back `NO_MESSAGES` status.
                    StatusCode::NOT_FOUND => {
                        debug!("received `NO_MESSAGES`. Iterator done");
                        self.terminated = true;
                        Poll::Ready(None)
                    }
                    StatusCode::OK => {
                        debug!("received message");
                        self.pending_messages -= 1;
                        Poll::Ready(Some(Ok(jetstream::Message {
                            context: self.context.clone(),
                            message,
                        })))
                    }
                    status => {
                        debug!("received error");
                        self.terminated = true;
                        Poll::Ready(Some(Err(Box::new(std::io::Error::other(format!(
                            "error while processing messages from the stream: {}, {:?}",
                            status, message.description
                        ))))))
                    }
                },
                None => Poll::Ready(None),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub struct Sequence {
    context: Context,
    subject: String,
    request: Bytes,
    pending_messages: usize,
    next: Option<BoxFuture<'static, Result<Batch, MessagesError>>>,
}

impl futures_util::Stream for Sequence {
    type Item = Result<Batch, MessagesError>;

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

                let next = self.next.insert(Box::pin(async move {
                    let inbox = context.client.new_inbox();
                    let subscriber = context
                        .client
                        .subscribe(inbox.clone())
                        .await
                        .map_err(|err| MessagesError::with_source(MessagesErrorKind::Pull, err))?;

                    context
                        .client
                        .publish_with_reply(subject, inbox, request)
                        .await
                        .map_err(|err| MessagesError::with_source(MessagesErrorKind::Pull, err))?;

                    // TODO(tp): Add timeout config and defaults.
                    Ok(Batch {
                        pending_messages,
                        subscriber,
                        context,
                        terminated: false,
                        timeout: Some(Box::pin(tokio::time::sleep(Duration::from_secs(60)))),
                    })
                }));

                match next.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        self.next = None;
                        Poll::Ready(Some(result.map_err(|err| {
                            MessagesError::with_source(MessagesErrorKind::Pull, err)
                        })))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }

            Some(next) => match next.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    self.next = None;
                    Poll::Ready(Some(result.map_err(|err| {
                        MessagesError::with_source(MessagesErrorKind::Pull, err)
                    })))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl Consumer<OrderedConfig> {
    /// Returns a stream of messages for Ordered Pull Consumer.
    ///
    /// Ordered consumers uses single replica ephemeral consumer, no matter the replication factor of the
    /// Stream. It does not use acks, instead it tracks sequences and recreate itself whenever it
    /// sees mismatch.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures_util::StreamExt;
    /// use futures_util::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .get_or_create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         max_messages: 10_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// jetstream.publish("events", "data".into()).await?;
    ///
    /// let consumer = stream
    ///     .get_or_create_consumer(
    ///         "consumer",
    ///         async_nats::jetstream::consumer::pull::OrderedConfig {
    ///             name: Some("consumer".to_string()),
    ///             ..Default::default()
    ///         },
    ///     )
    ///     .await?;
    ///
    /// let mut messages = consumer.messages().await?.take(100);
    /// while let Some(Ok(message)) = messages.next().await {
    ///     println!("got message {:?}", message);
    /// }
    /// Ok(())
    /// # }
    /// ```
    pub async fn messages(self) -> Result<Ordered, StreamError> {
        let config = Consumer {
            config: self.config.clone().into(),
            context: self.context.clone(),
            info: self.info.clone(),
        };
        let stream = Stream::stream(ordered_batch_config(&self.info), &config).await?;

        Ok(Ordered {
            consumer_sequence: 0,
            stream_sequence: 0,
            missed_heartbeats: false,
            limit_exceeded: false,
            create_stream: None,
            context: self.context.clone(),
            consumer_name: self.info.name.clone(),
            name_prefix: self.info.name.clone(),
            serial: 0,
            consumer: self.config,
            stream: Some(stream),
            stream_name: self.info.stream_name.clone(),
        })
    }
}

/// Configuration for consumers. From a high level, the
/// `durable_name` and `deliver_subject` fields have a particularly
/// strong influence on the consumer's overall behavior.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OrderedConfig {
    /// A name of the consumer. Can be specified for both durable and ephemeral
    /// consumers.
    ///
    /// When the consumer is recreated, the replacement is named `{name}_{n}`,
    /// where `n` counts the recreates. Without a name, the name the server gave
    /// the first consumer is used instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// A short description of the purpose of this consumer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subject: String,
    #[cfg(feature = "server_2_10")]
    /// Fulfills the same role as [Config::filter_subject], but allows filtering by many subjects.
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subjects: Vec<String>,
    /// Whether messages are sent as quickly as possible or at the rate of receipt
    pub replay_policy: ReplayPolicy,
    /// The rate of message delivery in bits per second
    #[serde(rename = "rate_limit_bps", default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    /// What percentage of acknowledgments should be samples for observability, 0-100
    #[serde(
        rename = "sample_freq",
        with = "super::sample_freq_deser",
        default,
        skip_serializing_if = "is_default"
    )]
    pub sample_frequency: u8,
    /// Only deliver headers without payloads.
    #[serde(default, skip_serializing_if = "is_default")]
    pub headers_only: bool,
    /// Allows for a variety of options that determine how this consumer will receive messages
    #[serde(flatten)]
    pub deliver_policy: DeliverPolicy,
    /// The maximum number of waiting consumers.
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_waiting: i64,
    #[cfg(feature = "server_2_10")]
    /// Additional consumer metadata.
    #[serde(default, skip_serializing_if = "is_default")]
    pub metadata: HashMap<String, String>,
    /// Maximum number of messages that can be requested in single Pull Request.
    /// This is used explicitly by [Consumer::batch] and [Consumer::fetch], but also, under the hood, by [Consumer::messages] and
    /// [Consumer::stream]
    pub max_batch: i64,
    /// Maximum number of bytes that can be requested in single Pull Request.
    /// This is used explicitly by [Consumer::batch] and [Consumer::fetch], but also, under the hood, by [Consumer::messages] and
    /// [Consumer::stream]
    pub max_bytes: i64,
    /// Maximum expiry that can be set for a single Pull Request.
    /// This is used explicitly by [Consumer::batch] and [Consumer::fetch], but also, under the hood, by [Consumer::messages] and
    /// [Consumer::stream]. The ordered consumer caps its pull expiry (30 seconds by default) at this
    /// value, so values well below a second make it poll the server continuously.
    pub max_expires: Duration,
}

impl From<OrderedConfig> for Config {
    fn from(config: OrderedConfig) -> Self {
        Config {
            durable_name: None,
            name: config.name,
            description: config.description,
            deliver_policy: config.deliver_policy,
            ack_policy: AckPolicy::None,
            ack_wait: Duration::default(),
            max_deliver: 1,
            filter_subject: config.filter_subject,
            #[cfg(feature = "server_2_10")]
            filter_subjects: config.filter_subjects,
            replay_policy: config.replay_policy,
            rate_limit: config.rate_limit,
            sample_frequency: config.sample_frequency,
            max_waiting: config.max_waiting,
            max_ack_pending: 0,
            headers_only: config.headers_only,
            max_batch: config.max_batch,
            max_bytes: config.max_bytes,
            max_expires: config.max_expires,
            inactive_threshold: Duration::from_secs(30),
            num_replicas: 1,
            memory_storage: true,
            #[cfg(feature = "server_2_10")]
            metadata: config.metadata,
            backoff: Vec::new(),
            #[cfg(feature = "server_2_11")]
            priority_policy: PriorityPolicy::None,
            #[cfg(feature = "server_2_11")]
            priority_groups: Vec::new(),
            #[cfg(feature = "server_2_11")]
            pause_until: None,
        }
    }
}

impl FromConsumer for OrderedConfig {
    fn try_from_consumer_config(
        config: crate::jetstream::consumer::Config,
    ) -> Result<Self, crate::Error>
    where
        Self: Sized,
    {
        Ok(OrderedConfig {
            name: config.name,
            description: config.description,
            filter_subject: config.filter_subject,
            #[cfg(feature = "server_2_10")]
            filter_subjects: config.filter_subjects,
            replay_policy: config.replay_policy,
            rate_limit: config.rate_limit,
            sample_frequency: config.sample_frequency,
            headers_only: config.headers_only,
            deliver_policy: config.deliver_policy,
            max_waiting: config.max_waiting,
            #[cfg(feature = "server_2_10")]
            metadata: config.metadata,
            max_batch: config.max_batch,
            max_bytes: config.max_bytes,
            max_expires: config.max_expires,
        })
    }
}

impl IntoConsumerConfig for OrderedConfig {
    fn into_consumer_config(self) -> super::Config {
        jetstream::consumer::Config {
            deliver_subject: None,
            durable_name: None,
            name: self.name,
            description: self.description,
            deliver_group: None,
            deliver_policy: self.deliver_policy,
            ack_policy: AckPolicy::None,
            ack_wait: Duration::default(),
            max_deliver: 1,
            filter_subject: self.filter_subject,
            #[cfg(feature = "server_2_10")]
            filter_subjects: self.filter_subjects,
            replay_policy: self.replay_policy,
            rate_limit: self.rate_limit,
            sample_frequency: self.sample_frequency,
            max_waiting: self.max_waiting,
            max_ack_pending: 0,
            headers_only: self.headers_only,
            flow_control: false,
            idle_heartbeat: Duration::default(),
            max_batch: self.max_batch,
            max_bytes: self.max_bytes,
            max_expires: self.max_expires,
            inactive_threshold: Duration::from_secs(30),
            num_replicas: 1,
            memory_storage: true,
            #[cfg(feature = "server_2_10")]
            metadata: self.metadata,
            backoff: Vec::new(),
            #[cfg(feature = "server_2_11")]
            priority_policy: PriorityPolicy::None,
            #[cfg(feature = "server_2_11")]
            priority_groups: Vec::new(),
            #[cfg(feature = "server_2_11")]
            pause_until: None,
        }
    }
}

pub struct Ordered {
    context: Context,
    stream_name: String,
    consumer: OrderedConfig,
    consumer_name: String,
    name_prefix: String,
    serial: u64,
    stream: Option<Stream>,
    create_stream: Option<BoxFuture<'static, Result<Stream, ConsumerRecreateError>>>,
    consumer_sequence: u64,
    stream_sequence: u64,
    missed_heartbeats: bool,
    /// Set when the consumer was recreated because the server rejected a pull for exceeding
    /// its request limits (they changed after creation); cleared by the next delivered message.
    /// A second rejection without a message in between means recreating does not help, so the
    /// error is surfaced instead.
    limit_exceeded: bool,
}

impl futures_util::Stream for Ordered {
    type Item = Result<jetstream::Message, OrderedError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut recreate = false;
        // Poll messages
        if let Some(stream) = self.stream.as_mut() {
            match stream.poll_next_unpin(cx) {
                Poll::Ready(message) => match message {
                    Some(message) => match message {
                        Ok(message) => {
                            self.missed_heartbeats = false;
                            let info = message.info().map_err(|err| {
                                OrderedError::with_source(OrderedErrorKind::Other, err)
                            })?;
                            trace!("consumer sequence: {:?}, stream sequence {:?}, consumer sequence in message: {:?} stream sequence in message: {:?}",
                                           self.consumer_sequence,
                                           self.stream_sequence,
                                           info.consumer_sequence,
                                           info.stream_sequence);
                            if info.consumer_sequence != self.consumer_sequence + 1 {
                                debug!(
                                    "ordered consumer mismatch. current {}, info: {}",
                                    self.consumer_sequence, info.consumer_sequence
                                );
                                recreate = true;
                                self.consumer_sequence = 0;
                            } else {
                                self.stream_sequence = info.stream_sequence;
                                self.consumer_sequence = info.consumer_sequence;
                                self.limit_exceeded = false;
                                return Poll::Ready(Some(Ok(message)));
                            }
                        }
                        Err(err) => match err.kind() {
                            MessagesErrorKind::MissingHeartbeat => {
                                // If we have missed heartbeats set, it means this is a second
                                // missed heartbeat, so we need to recreate consumer.
                                if self.missed_heartbeats {
                                    self.consumer_sequence = 0;
                                    recreate = true;
                                } else {
                                    self.missed_heartbeats = true;
                                }
                            }
                            MessagesErrorKind::ConsumerDeleted
                            | MessagesErrorKind::NoResponders => {
                                recreate = true;
                                self.consumer_sequence = 0;
                            }
                            // Recreating reads the current limits; see `limit_exceeded`.
                            MessagesErrorKind::RequestLimitExceeded => {
                                if self.limit_exceeded {
                                    return Poll::Ready(Some(Err(err.into())));
                                }
                                self.limit_exceeded = true;
                                recreate = true;
                                self.consumer_sequence = 0;
                            }
                            MessagesErrorKind::Pull
                            | MessagesErrorKind::PushBasedConsumer
                            | MessagesErrorKind::Other => {
                                return Poll::Ready(Some(Err(err.into())));
                            }
                        },
                    },
                    None => return Poll::Ready(None),
                },
                Poll::Pending => (),
            }
        }
        // Recreate consumer if needed
        if recreate {
            self.stream = None;
            self.serial += 1;
            let name = format!("{}_{}", self.name_prefix, self.serial);
            let consumer_name = std::mem::replace(&mut self.consumer_name, name.clone());
            self.create_stream = Some(Box::pin({
                let context = self.context.clone();
                let config = OrderedConfig {
                    name: Some(name),
                    ..self.consumer.clone()
                };
                let stream_name = self.stream_name.clone();
                let sequence = self.stream_sequence;
                async move {
                    tryhard::retry_fn(|| {
                        recreate_consumer_stream(
                            &context,
                            &config,
                            &stream_name,
                            &consumer_name,
                            sequence,
                        )
                    })
                    .retries(u32::MAX)
                    .custom_backoff(backoff)
                    .await
                }
            }))
        }
        // check for recreation future
        if let Some(result) = self.create_stream.as_mut() {
            match result.poll_unpin(cx) {
                Poll::Ready(result) => match result {
                    Ok(stream) => {
                        self.create_stream = None;
                        self.stream = Some(stream);
                        return self.poll_next(cx);
                    }
                    Err(err) => {
                        return Poll::Ready(Some(Err(OrderedError::with_source(
                            OrderedErrorKind::Recreate,
                            err,
                        ))))
                    }
                },
                Poll::Pending => (),
            }
        }
        Poll::Pending
    }
}

pub struct Stream {
    pending_messages: usize,
    pending_bytes: usize,
    request_result_rx: tokio::sync::mpsc::Receiver<Result<bool, super::RequestError>>,
    request_tx: tokio::sync::watch::Sender<()>,
    subscriber: Subscriber,
    batch_config: BatchConfig,
    context: Context,
    pending_request: bool,
    task_handle: JoinHandle<()>,
    terminated: bool,
    heartbeat_timeout: Option<Pin<Box<tokio::time::Sleep>>>,
    started: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl Stream {
    /// Ends the iterator and stops the background task from sending further pull requests.
    fn terminate(&mut self) {
        self.terminated = true;
        self.task_handle.abort();
    }

    async fn stream(
        batch_config: BatchConfig,
        consumer: &Consumer<Config>,
    ) -> Result<Stream, StreamError> {
        let inbox = consumer.context.client.new_inbox();
        let subscription = consumer
            .context
            .client
            .subscribe(inbox.clone())
            .await
            .map_err(|err| StreamError::with_source(StreamErrorKind::Other, err))?;
        let subject = format!(
            "{}.CONSUMER.MSG.NEXT.{}.{}",
            consumer.context.prefix, consumer.info.stream_name, consumer.info.name
        );

        let (request_result_tx, request_result_rx) = tokio::sync::mpsc::channel(1);
        let (request_tx, mut request_rx) = tokio::sync::watch::channel(());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task_handle = tokio::task::spawn({
            let batch = batch_config.clone();
            let consumer = consumer.clone();
            let mut context = consumer.context.clone();
            let inbox = inbox.clone();
            async move {
                started_rx.await.ok();
                loop {
                    // this is just in edge case of missing response for some reason.
                    let expires = batch_config
                        .expires
                        .map(|expires| {
                            if expires.is_zero() {
                                Either::Left(future::pending())
                            } else {
                                Either::Right(tokio::time::sleep(
                                    expires.saturating_add(Duration::from_secs(5)),
                                ))
                            }
                        })
                        .unwrap_or_else(|| Either::Left(future::pending()));
                    // Need to check previous state, as `changed` will always fire on first
                    // call.
                    let prev_state = context.client.state.borrow().to_owned();
                    let mut pending_reset = false;

                    tokio::select! {
                       _ = context.client.state.changed() => {
                            let state = context.client.state.borrow().to_owned();
                            if !(state == crate::connection::State::Connected
                                && prev_state != State::Connected) {
                                    continue;
                                }
                            debug!("detected !Connected -> Connected state change");

                            match tryhard::retry_fn(|| consumer.get_info())
                                .retries(5).custom_backoff(backoff).await
                                .map_err(|err| crate::RequestError::with_source(crate::RequestErrorKind::Other, err).into()) {
                                    Ok(info) => {
                                        if info.num_waiting == 0 {
                                            pending_reset = true;
                                        }
                                    }
                                    Err(err) => {
                                         if let Err(err) = request_result_tx.send(Err(err)).await {
                                            debug!("failed to sent request result: {}", err);
                                        }
                                    },
                            }
                        },
                        _ = request_rx.changed() => debug!("task received request request"),
                        _ = expires => {
                            pending_reset = true;
                            debug!("expired pull request")},
                    }

                    let request = serde_json::to_vec(&batch).map(Bytes::from).unwrap();
                    let result = context
                        .client
                        .publish_with_reply(subject.clone(), inbox.clone(), request.clone())
                        .await
                        .map(|_| pending_reset);
                    // TODO: add tracing instead of ignoring this.
                    request_result_tx
                        .send(result.map(|_| pending_reset).map_err(|err| {
                            crate::RequestError::with_source(crate::RequestErrorKind::Other, err)
                                .into()
                        }))
                        .await
                        .ok();
                    trace!("result send over tx");
                }
            }
        });

        Ok(Stream {
            task_handle,
            request_result_rx,
            request_tx,
            batch_config,
            pending_messages: 0,
            pending_bytes: 0,
            subscriber: subscription,
            context: consumer.context.clone(),
            pending_request: false,
            terminated: false,
            heartbeat_timeout: None,
            started: Some(started_tx),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OrderedErrorKind {
    MissingHeartbeat,
    ConsumerDeleted,
    Pull,
    PushBasedConsumer,
    Recreate,
    NoResponders,
    /// The server kept rejecting pull requests for exceeding the consumer's request limits even
    /// after the consumer was recreated. See [`MessagesErrorKind::RequestLimitExceeded`].
    RequestLimitExceeded,
    Other,
}

impl std::fmt::Display for OrderedErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeartbeat => write!(f, "missed idle heartbeat"),
            Self::ConsumerDeleted => write!(f, "consumer deleted"),
            Self::Pull => write!(f, "pull request failed"),
            Self::Other => write!(f, "error"),
            Self::PushBasedConsumer => write!(f, "cannot use with push consumer"),
            Self::Recreate => write!(f, "consumer recreation failed"),
            Self::NoResponders => write!(f, "no responders"),
            Self::RequestLimitExceeded => write!(f, "pull request exceeded consumer limits"),
        }
    }
}

pub type OrderedError = Error<OrderedErrorKind>;

impl From<MessagesError> for OrderedError {
    fn from(err: MessagesError) -> Self {
        match err.kind() {
            MessagesErrorKind::MissingHeartbeat => {
                OrderedError::new(OrderedErrorKind::MissingHeartbeat)
            }
            MessagesErrorKind::ConsumerDeleted => {
                OrderedError::new(OrderedErrorKind::ConsumerDeleted)
            }
            MessagesErrorKind::Pull => OrderedError {
                kind: OrderedErrorKind::Pull,
                source: err.source,
            },
            MessagesErrorKind::PushBasedConsumer => {
                OrderedError::new(OrderedErrorKind::PushBasedConsumer)
            }
            MessagesErrorKind::Other => OrderedError {
                kind: OrderedErrorKind::Other,
                source: err.source,
            },
            MessagesErrorKind::NoResponders => OrderedError::new(OrderedErrorKind::NoResponders),
            MessagesErrorKind::RequestLimitExceeded => OrderedError {
                kind: OrderedErrorKind::RequestLimitExceeded,
                source: err.source,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MessagesErrorKind {
    MissingHeartbeat,
    ConsumerDeleted,
    Pull,
    PushBasedConsumer,
    NoResponders,
    /// The server rejected the pull request because it exceeded one of the consumer's request
    /// limits: `max_batch`, `max_expires` or `max_bytes`. The error source carries the server's
    /// description, for example `Exceeded MaxRequestBatch of 100`. The iterator ends after this
    /// error. [`Consumer::fetch`] and [`Consumer::batch`] report the same server rejection as an
    /// opaque error.
    RequestLimitExceeded,
    Other,
}

impl std::fmt::Display for MessagesErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeartbeat => write!(f, "missed idle heartbeat"),
            Self::ConsumerDeleted => write!(f, "consumer deleted"),
            Self::Pull => write!(f, "pull request failed"),
            Self::Other => write!(f, "error"),
            Self::NoResponders => write!(f, "no responders"),
            Self::PushBasedConsumer => write!(f, "cannot use with push consumer"),
            Self::RequestLimitExceeded => write!(f, "pull request exceeded consumer limits"),
        }
    }
}

pub type MessagesError = Error<MessagesErrorKind>;

impl futures_util::Stream for Stream {
    type Item = Result<jetstream::Message, MessagesError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if let Some(started) = self.started.take() {
            trace!("stream started, sending started signal");
            if started.send(()).is_err() {
                debug!("failed to send started signal");
            }
        }
        if self.terminated {
            return Poll::Ready(None);
        }

        loop {
            trace!("pending messages: {}", self.pending_messages);
            if (self.pending_messages <= self.batch_config.batch / 2
                || (self.batch_config.max_bytes > 0
                    && self.pending_bytes <= self.batch_config.max_bytes / 2))
                && !self.pending_request
            {
                debug!("pending messages reached threshold to send new fetch request");
                self.request_tx.send(()).ok();
                self.pending_request = true;
            }

            match self.request_result_rx.poll_recv(cx) {
                Poll::Ready(resp) => match resp {
                    Some(resp) => match resp {
                        Ok(reset) => {
                            trace!("request response: {:?}", reset);
                            debug!("request sent, setting pending messages");
                            if reset {
                                self.pending_messages = self.batch_config.batch;
                                self.pending_bytes = self.batch_config.max_bytes;
                            } else {
                                self.pending_messages += self.batch_config.batch;
                                self.pending_bytes += self.batch_config.max_bytes;
                            }
                            self.pending_request = false;
                            continue;
                        }
                        Err(err) => {
                            return Poll::Ready(Some(Err(MessagesError::with_source(
                                MessagesErrorKind::Pull,
                                err,
                            ))))
                        }
                    },
                    None => return Poll::Ready(None),
                },
                Poll::Pending => {
                    trace!("pending result");
                }
            }

            trace!("polling subscriber");
            match self.subscriber.receiver.poll_recv(cx) {
                Poll::Ready(maybe_message) => {
                    self.heartbeat_timeout = None;
                    match maybe_message {
                        Some(message) => match message.status.unwrap_or(StatusCode::OK) {
                            StatusCode::TIMEOUT | StatusCode::REQUEST_TERMINATED => {
                                debug!("received status message: {:?}", message);
                                // If consumer has been deleted, error and shutdown the iterator.
                                if message.description.as_deref() == Some("Consumer Deleted") {
                                    self.terminate();
                                    return Poll::Ready(Some(Err(MessagesError::new(
                                        MessagesErrorKind::ConsumerDeleted,
                                    ))));
                                }
                                // If consumer is not pull based, error and shutdown the iterator.
                                if message.description.as_deref() == Some("Consumer is push based")
                                {
                                    self.terminate();
                                    return Poll::Ready(Some(Err(MessagesError::new(
                                        MessagesErrorKind::PushBasedConsumer,
                                    ))));
                                }
                                // Bare 409 without pending headers; the same request would be
                                // rejected again, so end the iterator instead of re-pulling.
                                if let Some(description) =
                                    message.description.as_deref().filter(|description| {
                                        description.starts_with("Exceeded MaxRequest")
                                    })
                                {
                                    self.terminate();
                                    return Poll::Ready(Some(Err(MessagesError::with_source(
                                        MessagesErrorKind::RequestLimitExceeded,
                                        description.to_string(),
                                    ))));
                                }

                                // Do accounting for messages left after terminated/completed pull request.
                                let pending_messages = message
                                    .headers
                                    .as_ref()
                                    .and_then(|headers| headers.get("Nats-Pending-Messages"))
                                    .map_or(Ok(self.batch_config.batch), |x| x.as_str().parse())
                                    .map_err(|err| {
                                        MessagesError::with_source(MessagesErrorKind::Other, err)
                                    })?;

                                let pending_bytes = message
                                    .headers
                                    .as_ref()
                                    .and_then(|headers| headers.get("Nats-Pending-Bytes"))
                                    .map_or(Ok(self.batch_config.max_bytes), |x| x.as_str().parse())
                                    .map_err(|err| {
                                        MessagesError::with_source(MessagesErrorKind::Other, err)
                                    })?;

                                debug!(
                                    "timeout reached. remaining messages: {}, bytes {}",
                                    pending_messages, pending_bytes
                                );
                                self.pending_messages =
                                    self.pending_messages.saturating_sub(pending_messages);
                                trace!("message bytes len: {}", pending_bytes);
                                self.pending_bytes =
                                    self.pending_bytes.saturating_sub(pending_bytes);
                                continue;
                            }
                            // Idle Hearbeat means we have no messages, but consumer is fine.
                            StatusCode::IDLE_HEARTBEAT => {
                                debug!("received idle heartbeat");
                                continue;
                            }
                            // We got an message from a stream.
                            StatusCode::OK => {
                                trace!("message received");
                                self.pending_messages = self.pending_messages.saturating_sub(1);
                                self.pending_bytes =
                                    self.pending_bytes.saturating_sub(message.length);
                                return Poll::Ready(Some(Ok(jetstream::Message {
                                    context: self.context.clone(),
                                    message,
                                })));
                            }
                            StatusCode::NO_RESPONDERS => {
                                debug!("received no responders");
                                return Poll::Ready(Some(Err(MessagesError::new(
                                    MessagesErrorKind::NoResponders,
                                ))));
                            }
                            status => {
                                debug!("received unknown message: {:?}", message);
                                return Poll::Ready(Some(Err(MessagesError::with_source(
                                    MessagesErrorKind::Other,
                                    format!(
                                        "error while processing messages from the stream: {}, {:?}",
                                        status, message.description
                                    ),
                                ))));
                            }
                        },
                        None => return Poll::Ready(None),
                    }
                }
                Poll::Pending => {
                    debug!("subscriber still pending");
                    break;
                }
            }
        }

        let idle_heartbeat = self.batch_config.idle_heartbeat;
        if poll_missed_heartbeat(&mut self.heartbeat_timeout, idle_heartbeat, cx) {
            return Poll::Ready(Some(Err(MessagesError::new(
                MessagesErrorKind::MissingHeartbeat,
            ))));
        }
        Poll::Pending
    }
}

/// Used for building configuration for a [Stream]. Created by a [Consumer::stream] on a [Consumer].
///
/// Values not set here default to 200 messages per pull request expiring after 30 seconds, with
/// no idle heartbeat. Those defaults are capped by the consumer's `max_batch` and `max_expires`
/// when lower. Values set explicitly are sent as given, except that a heartbeat is lowered to
/// half of the expiry when the expiry was left to its default, as the server requires. A pull
/// request exceeding the consumer's limits ends the stream with
/// [`MessagesErrorKind::RequestLimitExceeded`].
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error>  {
/// use futures_util::StreamExt;
/// use async_nats::jetstream::consumer::PullConsumer;
/// let client = async_nats::connect("localhost:4222").await?;
/// let jetstream = async_nats::jetstream::new(client);
///
/// let consumer: PullConsumer = jetstream
///     .get_stream("events").await?
///     .get_consumer("pull").await?;
///
/// let mut messages = consumer.stream()
///     .max_messages_per_batch(100)
///     .max_bytes_per_batch(1024)
///     .messages().await?;
///
/// while let Some(message) = messages.next().await {
///     let message = message?;
///     println!("message: {:?}", message);
///     message.ack().await?;
/// }
/// # Ok(())
/// # }
pub struct StreamBuilder<'a> {
    batch: Option<usize>,
    max_bytes: usize,
    heartbeat: Option<Duration>,
    expires: Option<Duration>,
    group: Option<String>,
    min_pending: Option<usize>,
    min_ack_pending: Option<usize>,
    #[cfg(feature = "server_2_12")]
    priority: Option<usize>,
    consumer: &'a Consumer<Config>,
}

impl<'a> StreamBuilder<'a> {
    pub fn new(consumer: &'a Consumer<Config>) -> Self {
        StreamBuilder {
            consumer,
            batch: None,
            max_bytes: 0,
            expires: None,
            heartbeat: None,
            group: None,
            min_pending: None,
            min_ack_pending: None,
            #[cfg(feature = "server_2_12")]
            priority: None,
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
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .stream()
    ///     .max_bytes_per_batch(1024)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
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
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .stream()
    ///     .max_messages_per_batch(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn max_messages_per_batch(mut self, batch: usize) -> Self {
        self.batch = Some(batch);
        self
    }

    /// Sets heartbeat which will be send by the server if there are no messages for a given
    /// [Consumer] pending. Lowered to half of the expiry if [`StreamBuilder::expires`] is not set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .stream()
    ///     .heartbeat(std::time::Duration::from_secs(10))
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn heartbeat(mut self, heartbeat: Duration) -> Self {
        self.heartbeat = Some(heartbeat);
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
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .stream()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn expires(mut self, expires: Duration) -> Self {
        self.expires = Some(expires);
        self
    }

    /// Sets overflow threshold for minimum pending messages before this stream will start getting
    /// messages for a [Consumer].
    /// To use overflow, [Consumer] needs to have enabled [Config::priority_groups] and [PriorityPolicy::Overflow] set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .stream()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .group("A")
    ///     .min_pending(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn min_pending(mut self, min_pending: usize) -> Self {
        self.min_pending = Some(min_pending);
        self
    }

    /// Sets the priority at which this stream will get messages. If there are any requests with
    /// lower priority number, this stream will not get messages until those are satisfied.
    #[cfg(feature = "server_2_12")]
    pub fn priority(mut self, priority: usize) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Sets overflow threshold for minimum pending acknowledgements before this stream will start getting
    /// messages for a [Consumer].
    /// To use overflow, [Consumer] needs to have enabled [Config::priority_groups] and [PriorityPolicy::Overflow] set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .stream()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .group("A")
    ///     .min_ack_pending(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn min_ack_pending(mut self, min_ack_pending: usize) -> Self {
        self.min_ack_pending = Some(min_ack_pending);
        self
    }

    /// Setting group when using [Consumer] with [Config::priority_groups].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .stream()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .group("A")
    ///     .min_ack_pending(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn group<T: Into<String>>(mut self, group: T) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Creates actual [Stream] with provided configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .stream()
    ///     .max_messages_per_batch(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn messages(self) -> Result<Stream, StreamError> {
        let limits = &self.consumer.info.config;
        let batch = self
            .batch
            .unwrap_or_else(|| clamp_batch(DEFAULT_BATCH, limits.max_batch));
        let expires = self
            .expires
            .unwrap_or_else(|| clamp_expires(DEFAULT_EXPIRES, limits.max_expires));
        let idle_heartbeat = match self.heartbeat {
            // The user chose both; the server validates the pair.
            Some(heartbeat) if self.expires.is_some() => heartbeat,
            // We chose the expiry, so keep the heartbeat valid for it.
            Some(heartbeat) => clamp_heartbeat(heartbeat, expires),
            None => Duration::ZERO,
        };
        Stream::stream(
            BatchConfig {
                batch,
                expires: Some(expires),
                no_wait: false,
                max_bytes: self.max_bytes,
                idle_heartbeat,
                min_pending: self.min_pending,
                group: self.group,
                min_ack_pending: self.min_ack_pending,
                #[cfg(feature = "server_2_12")]
                priority: self.priority,
            },
            self.consumer,
        )
        .await
    }
}

/// Used for building configuration for a [Batch] with `fetch()` semantics. Created by a [FetchBuilder] on a [Consumer].
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error>  {
/// use async_nats::jetstream::consumer::PullConsumer;
/// use futures_util::StreamExt;
/// let client = async_nats::connect("localhost:4222").await?;
/// let jetstream = async_nats::jetstream::new(client);
///
/// let consumer: PullConsumer = jetstream
///     .get_stream("events")
///     .await?
///     .get_consumer("pull")
///     .await?;
///
/// let mut messages = consumer
///     .fetch()
///     .max_messages(100)
///     .max_bytes(1024)
///     .messages()
///     .await?;
///
/// while let Some(message) = messages.next().await {
///     let message = message?;
///     println!("message: {:?}", message);
///     message.ack().await?;
/// }
/// # Ok(())
/// # }
/// ```
pub struct FetchBuilder<'a> {
    batch: usize,
    max_bytes: usize,
    heartbeat: Duration,
    expires: Option<Duration>,
    min_pending: Option<usize>,
    min_ack_pending: Option<usize>,
    group: Option<String>,
    #[cfg(feature = "server_2_12")]
    priority: Option<usize>,
    consumer: &'a Consumer<Config>,
}

impl<'a> FetchBuilder<'a> {
    pub fn new(consumer: &'a Consumer<Config>) -> Self {
        FetchBuilder {
            consumer,
            batch: 200,
            max_bytes: 0,
            expires: None,
            min_pending: None,
            min_ack_pending: None,
            group: None,
            #[cfg(feature = "server_2_12")]
            priority: None,
            heartbeat: Duration::default(),
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
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer.fetch().max_bytes(1024).messages().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
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
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer.fetch().max_messages(100).messages().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn max_messages(mut self, batch: usize) -> Self {
        self.batch = batch;
        self
    }

    /// Sets heartbeat which will be send by the server if there are no messages for a given
    /// [Consumer] pending.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .fetch()
    ///     .heartbeat(std::time::Duration::from_secs(10))
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn heartbeat(mut self, heartbeat: Duration) -> Self {
        self.heartbeat = heartbeat;
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
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .fetch()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn expires(mut self, expires: Duration) -> Self {
        self.expires = Some(expires);
        self
    }

    /// Sets overflow threshold for minimum pending messages before this stream will start getting
    /// messages.
    /// To use overflow, [Consumer] needs to have enabled [Config::priority_groups] and
    /// [PriorityPolicy::Overflow] set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .fetch()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .group("A")
    ///     .min_pending(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn min_pending(mut self, min_pending: usize) -> Self {
        self.min_pending = Some(min_pending);
        self
    }

    /// Sets the priority at which this stream will get messages. If there are any requests with
    /// lower priority number, this stream will not get messages until those are satisfied.
    #[cfg(feature = "server_2_12")]
    pub fn priority(mut self, priority: usize) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Sets overflow threshold for minimum pending acknowledgments before this stream will start getting
    /// messages.
    /// To use overflow, [Consumer] needs to have enabled [Config::priority_groups] and
    /// [PriorityPolicy::Overflow] set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .fetch()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .group("A")
    ///     .min_ack_pending(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn min_ack_pending(mut self, min_ack_pending: usize) -> Self {
        self.min_ack_pending = Some(min_ack_pending);
        self
    }

    /// Setting group when using [Consumer] with [PriorityPolicy].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .fetch()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .group("A")
    ///     .min_ack_pending(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn group<T: Into<String>>(mut self, group: T) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Creates actual [Stream] with provided configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer.fetch().max_messages(100).messages().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn messages(self) -> Result<Batch, BatchError> {
        Batch::batch(
            BatchConfig {
                batch: self.batch,
                expires: self.expires,
                no_wait: true,
                max_bytes: self.max_bytes,
                idle_heartbeat: self.heartbeat,
                min_pending: self.min_pending,
                min_ack_pending: self.min_ack_pending,
                group: self.group,
                #[cfg(feature = "server_2_12")]
                priority: self.priority,
            },
            self.consumer,
        )
        .await
    }
}

/// Used for building configuration for a [Batch]. Created by a [Consumer::batch] on a [Consumer].
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error>  {
/// use async_nats::jetstream::consumer::PullConsumer;
/// use futures_util::StreamExt;
/// let client = async_nats::connect("localhost:4222").await?;
/// let jetstream = async_nats::jetstream::new(client);
///
/// let consumer: PullConsumer = jetstream
///     .get_stream("events")
///     .await?
///     .get_consumer("pull")
///     .await?;
///
/// let mut messages = consumer
///     .batch()
///     .max_messages(100)
///     .max_bytes(1024)
///     .messages()
///     .await?;
///
/// while let Some(message) = messages.next().await {
///     let message = message?;
///     println!("message: {:?}", message);
///     message.ack().await?;
/// }
/// # Ok(())
/// # }
/// ```
pub struct BatchBuilder<'a> {
    batch: usize,
    max_bytes: usize,
    heartbeat: Duration,
    expires: Duration,
    min_pending: Option<usize>,
    min_ack_pending: Option<usize>,
    group: Option<String>,
    consumer: &'a Consumer<Config>,
}

impl<'a> BatchBuilder<'a> {
    pub fn new(consumer: &'a Consumer<Config>) -> Self {
        BatchBuilder {
            consumer,
            batch: 200,
            max_bytes: 0,
            expires: Duration::ZERO,
            heartbeat: Duration::default(),
            min_pending: None,
            min_ack_pending: None,
            group: None,
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
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer.batch().max_bytes(1024).messages().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
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
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer.batch().max_messages(100).messages().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn max_messages(mut self, batch: usize) -> Self {
        self.batch = batch;
        self
    }

    /// Sets heartbeat which will be send by the server if there are no messages for a given
    /// [Consumer] pending.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .batch()
    ///     .heartbeat(std::time::Duration::from_secs(10))
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn heartbeat(mut self, heartbeat: Duration) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    /// Sets overflow threshold for minimum pending messages before this stream will start getting
    /// messages.
    /// To use overflow, [Consumer] needs to have enabled [Config::priority_groups] and
    /// [PriorityPolicy::Overflow] set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .batch()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .group("A")
    ///     .min_pending(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn min_pending(mut self, min_pending: usize) -> Self {
        self.min_pending = Some(min_pending);
        self
    }

    /// Sets overflow threshold for minimum pending acknowledgments before this stream will start getting
    /// messages.
    /// To use overflow, [Consumer] needs to have enabled [Config::priority_groups] and
    /// [PriorityPolicy::Overflow] set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .batch()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .group("A")
    ///     .min_ack_pending(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn min_ack_pending(mut self, min_ack_pending: usize) -> Self {
        self.min_ack_pending = Some(min_ack_pending);
        self
    }

    /// Setting group when using [Consumer] with [PriorityPolicy].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .batch()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .group("A")
    ///     .min_ack_pending(100)
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn group<T: Into<String>>(mut self, group: T) -> Self {
        self.group = Some(group.into());
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
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer
    ///     .batch()
    ///     .expires(std::time::Duration::from_secs(30))
    ///     .messages()
    ///     .await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn expires(mut self, expires: Duration) -> Self {
        self.expires = expires;
        self
    }

    /// Creates actual [Stream] with provided configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures_util::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let consumer: PullConsumer = jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .get_consumer("pull")
    ///     .await?;
    ///
    /// let mut messages = consumer.batch().max_messages(100).messages().await?;
    ///
    /// while let Some(message) = messages.next().await {
    ///     let message = message?;
    ///     println!("message: {:?}", message);
    ///     message.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn messages(self) -> Result<Batch, BatchError> {
        let config = BatchConfig {
            batch: self.batch,
            expires: Some(self.expires),
            no_wait: false,
            max_bytes: self.max_bytes,
            idle_heartbeat: self.heartbeat,
            min_pending: self.min_pending,
            min_ack_pending: self.min_ack_pending,
            group: self.group,
            #[cfg(feature = "server_2_12")]
            priority: None,
        };
        Batch::batch(config, self.consumer).await
    }
}

/// Used for next Pull Request for Pull Consumer
#[derive(Debug, Default, Serialize, Clone, PartialEq, Eq)]
pub struct BatchConfig {
    /// The number of messages that are being requested to be delivered.
    pub batch: usize,
    /// The optional number of nanoseconds that the server will store this next request for
    /// before forgetting about the pending batch size.
    #[serde(skip_serializing_if = "Option::is_none", with = "serde_nanos")]
    pub expires: Option<Duration>,
    /// This optionally causes the server not to store this pending request at all, but when there are no
    /// messages to deliver will send a nil bytes message with a Status header of 404, this way you
    /// can know when you reached the end of the stream for example. A 409 is returned if the
    /// Consumer has reached MaxAckPending limits.
    #[serde(skip_serializing_if = "is_default")]
    pub no_wait: bool,

    /// Sets max number of bytes in total in given batch size. This works together with `batch`.
    /// Whichever value is reached first, batch will complete.
    pub max_bytes: usize,

    /// Setting this other than zero will cause the server to send 100 Idle Heartbeat status to the
    /// client
    #[serde(with = "serde_nanos", skip_serializing_if = "is_default")]
    pub idle_heartbeat: Duration,

    pub min_pending: Option<usize>,
    pub min_ack_pending: Option<usize>,
    pub group: Option<String>,
    #[cfg(feature = "server_2_12")]
    pub priority: Option<usize>,
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
    /// A name of the consumer. Can be specified for both durable and ephemeral
    /// consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
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
    #[cfg(feature = "server_2_10")]
    /// Fulfills the same role as [Config::filter_subject], but allows filtering by many subjects.
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subjects: Vec<String>,
    /// Whether messages are sent as quickly as possible or at the rate of receipt
    pub replay_policy: ReplayPolicy,
    /// The rate of message delivery in bits per second
    #[serde(rename = "rate_limit_bps", default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    /// What percentage of acknowledgments should be samples for observability, 0-100
    #[serde(
        rename = "sample_freq",
        with = "super::sample_freq_deser",
        default,
        skip_serializing_if = "is_default"
    )]
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
    /// Maximum value of request max_bytes
    #[serde(default, skip_serializing_if = "is_default")]
    pub max_bytes: i64,
    /// Maximum value for request expiration
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub max_expires: Duration,
    /// Threshold for consumer inactivity
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub inactive_threshold: Duration,
    /// Number of consumer replicas
    #[serde(default, skip_serializing_if = "is_default")]
    pub num_replicas: usize,
    /// Force consumer to use memory storage.
    #[serde(default, skip_serializing_if = "is_default")]
    pub memory_storage: bool,
    #[cfg(feature = "server_2_10")]
    // Additional consumer metadata.
    #[serde(default, skip_serializing_if = "is_default")]
    pub metadata: HashMap<String, String>,
    /// Custom backoff for missed acknowledgments.
    #[serde(default, skip_serializing_if = "is_default", with = "serde_nanos")]
    pub backoff: Vec<Duration>,

    /// Priority policy for this consumer. Requires [Config::priority_groups] to be set.
    #[cfg(feature = "server_2_11")]
    #[serde(default, skip_serializing_if = "is_default")]
    pub priority_policy: PriorityPolicy,
    /// Priority groups for this consumer. Currently only one group is supported and is used
    /// in conjunction with [Config::priority_policy].
    #[cfg(feature = "server_2_11")]
    #[serde(default, skip_serializing_if = "is_default")]
    pub priority_groups: Vec<String>,
    /// For suspending the consumer until the deadline.
    #[cfg(feature = "server_2_11")]
    #[serde(
        default,
        with = "rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub pause_until: Option<DateTime>,
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
            name: self.name,
            durable_name: self.durable_name,
            description: self.description,
            deliver_group: None,
            deliver_policy: self.deliver_policy,
            ack_policy: self.ack_policy,
            ack_wait: self.ack_wait,
            max_deliver: self.max_deliver,
            filter_subject: self.filter_subject,
            #[cfg(feature = "server_2_10")]
            filter_subjects: self.filter_subjects,
            replay_policy: self.replay_policy,
            rate_limit: self.rate_limit,
            sample_frequency: self.sample_frequency,
            max_waiting: self.max_waiting,
            max_ack_pending: self.max_ack_pending,
            headers_only: self.headers_only,
            flow_control: false,
            idle_heartbeat: Duration::default(),
            max_batch: self.max_batch,
            max_bytes: self.max_bytes,
            max_expires: self.max_expires,
            inactive_threshold: self.inactive_threshold,
            num_replicas: self.num_replicas,
            memory_storage: self.memory_storage,
            #[cfg(feature = "server_2_10")]
            metadata: self.metadata,
            backoff: self.backoff,
            #[cfg(feature = "server_2_11")]
            priority_policy: self.priority_policy,
            #[cfg(feature = "server_2_11")]
            priority_groups: self.priority_groups,
            #[cfg(feature = "server_2_11")]
            pause_until: self.pause_until,
        }
    }
}
impl FromConsumer for Config {
    fn try_from_consumer_config(config: consumer::Config) -> Result<Self, crate::Error> {
        if config.deliver_subject.is_some() {
            return Err(Box::new(std::io::Error::other(
                "pull consumer cannot have delivery subject",
            )));
        }
        Ok(Config {
            durable_name: config.durable_name,
            name: config.name,
            description: config.description,
            deliver_policy: config.deliver_policy,
            ack_policy: config.ack_policy,
            ack_wait: config.ack_wait,
            max_deliver: config.max_deliver,
            filter_subject: config.filter_subject,
            #[cfg(feature = "server_2_10")]
            filter_subjects: config.filter_subjects,
            replay_policy: config.replay_policy,
            rate_limit: config.rate_limit,
            sample_frequency: config.sample_frequency,
            max_waiting: config.max_waiting,
            max_ack_pending: config.max_ack_pending,
            headers_only: config.headers_only,
            max_batch: config.max_batch,
            max_bytes: config.max_bytes,
            max_expires: config.max_expires,
            inactive_threshold: config.inactive_threshold,
            num_replicas: config.num_replicas,
            memory_storage: config.memory_storage,
            #[cfg(feature = "server_2_10")]
            metadata: config.metadata,
            backoff: config.backoff,
            #[cfg(feature = "server_2_11")]
            priority_policy: config.priority_policy,
            #[cfg(feature = "server_2_11")]
            priority_groups: config.priority_groups,
            #[cfg(feature = "server_2_11")]
            pause_until: config.pause_until,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BatchRequestErrorKind {
    Publish,
    Flush,
    Serialize,
}

impl std::fmt::Display for BatchRequestErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Publish => write!(f, "publish failed"),
            Self::Flush => write!(f, "flush failed"),
            Self::Serialize => write!(f, "serialize failed"),
        }
    }
}

pub type BatchRequestError = Error<BatchRequestErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BatchErrorKind {
    Subscribe,
    Pull,
    Flush,
    Serialize,
}

impl std::fmt::Display for BatchErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pull => write!(f, "pull request failed"),
            Self::Flush => write!(f, "flush failed"),
            Self::Serialize => write!(f, "serialize failed"),
            Self::Subscribe => write!(f, "subscribe failed"),
        }
    }
}

pub type BatchError = Error<BatchErrorKind>;

impl From<SubscribeError> for BatchError {
    fn from(err: SubscribeError) -> Self {
        BatchError::with_source(BatchErrorKind::Subscribe, err)
    }
}

impl From<BatchRequestError> for BatchError {
    fn from(err: BatchRequestError) -> Self {
        BatchError::with_source(BatchErrorKind::Pull, err)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConsumerRecreateErrorKind {
    GetStream,
    Recreate,
    TimedOut,
}

impl std::fmt::Display for ConsumerRecreateErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GetStream => write!(f, "error getting stream"),
            Self::Recreate => write!(f, "consumer creation failed"),
            Self::TimedOut => write!(f, "timed out"),
        }
    }
}

pub type ConsumerRecreateError = Error<ConsumerRecreateErrorKind>;

/// Default number of messages a [Stream] asks for in a single pull request.
const DEFAULT_BATCH: usize = 200;
/// Default time a pull request waits for messages.
const DEFAULT_EXPIRES: Duration = Duration::from_secs(30);
/// Default idle heartbeat for [Consumer::messages] and ordered consumers.
const DEFAULT_IDLE_HEARTBEAT: Duration = Duration::from_secs(15);
/// The number of messages an ordered consumer asks for in a single pull request.
const ORDERED_BATCH: usize = 500;

/// Caps a default pull batch at the consumer's `max_batch`; zero means no limit. The limit is
/// set by the user or copied by the server from its `jetstream.limits.max_request_batch`, and a
/// request above it is rejected with `409 Exceeded MaxRequestBatch`.
fn clamp_batch(batch: usize, max_batch: i64) -> usize {
    match usize::try_from(max_batch) {
        Ok(max_batch) if max_batch > 0 => batch.min(max_batch),
        _ => batch,
    }
}

/// Caps a default pull expiry at the consumer's `max_expires`; zero means no limit. A request
/// above it is rejected with `409 Exceeded MaxRequestExpires`.
fn clamp_expires(expires: Duration, max_expires: Duration) -> Duration {
    if max_expires.is_zero() {
        expires
    } else {
        expires.min(max_expires)
    }
}

/// Keeps the idle heartbeat at or below half of the expiry, as the server requires.
fn clamp_heartbeat(heartbeat: Duration, expires: Duration) -> Duration {
    heartbeat.min(expires / 2)
}

/// Builds the pull request configuration for an ordered consumer, with the batch and expiry
/// capped by the consumer's limits.
fn ordered_batch_config(info: &consumer::Info) -> BatchConfig {
    let expires = clamp_expires(DEFAULT_EXPIRES, info.config.max_expires);
    BatchConfig {
        batch: clamp_batch(ORDERED_BATCH, info.config.max_batch),
        expires: Some(expires),
        no_wait: false,
        max_bytes: 0,
        idle_heartbeat: clamp_heartbeat(DEFAULT_IDLE_HEARTBEAT, expires),
        min_pending: None,
        min_ack_pending: None,
        group: None,
        #[cfg(feature = "server_2_12")]
        priority: None,
    }
}

async fn recreate_consumer_stream(
    context: &Context,
    config: &OrderedConfig,
    stream_name: &str,
    consumer_name: &str,
    sequence: u64,
) -> Result<Stream, ConsumerRecreateError> {
    let span = tracing::span!(
        tracing::Level::DEBUG,
        "recreate_ordered_consumer",
        stream_name = stream_name,
        consumer_name = consumer_name,
        sequence = sequence
    );
    let _span_handle = span.enter();
    let config = config.to_owned();
    trace!("delete old consumer before creating new one");

    tokio::time::timeout(
        Duration::from_secs(5),
        context.delete_consumer_from_stream(consumer_name, stream_name),
    )
    .await
    .ok();

    let deliver_policy = {
        if sequence == 0 {
            DeliverPolicy::All
        } else {
            DeliverPolicy::ByStartSequence {
                start_sequence: sequence + 1,
            }
        }
    };
    trace!("create the new ordered consumer for sequence {}", sequence);
    let consumer = tokio::time::timeout(
        Duration::from_secs(5),
        context.create_consumer_on_stream(
            jetstream::consumer::pull::OrderedConfig {
                deliver_policy,
                ..config.clone()
            },
            stream_name,
        ),
    )
    .await
    .map_err(|err| ConsumerRecreateError::with_source(ConsumerRecreateErrorKind::TimedOut, err))?
    .map_err(|err| ConsumerRecreateError::with_source(ConsumerRecreateErrorKind::Recreate, err))?;

    let batch_config = ordered_batch_config(&consumer.info);
    let config = Consumer {
        config: config.clone().into(),
        context: context.clone(),
        info: consumer.info,
    };

    trace!("create iterator");
    let stream = tokio::time::timeout(
        Duration::from_secs(5),
        Stream::stream(batch_config, &config),
    )
    .await
    .map_err(|err| ConsumerRecreateError::with_source(ConsumerRecreateErrorKind::TimedOut, err))?
    .map_err(|err| ConsumerRecreateError::with_source(ConsumerRecreateErrorKind::Recreate, err));
    trace!("recreated consumer");
    stream
}
