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
use futures::{
    future::{BoxFuture, Either},
    FutureExt, StreamExt,
};

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

use super::{
    AckPolicy, Consumer, DeliverPolicy, FromConsumer, IntoConsumerConfig, ReplayPolicy,
    StreamError, StreamErrorKind,
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
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
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
    pub async fn messages(&self) -> Result<Stream, StreamError> {
        Stream::stream(
            BatchConfig {
                batch: 200,
                expires: Some(Duration::from_secs(30)),
                no_wait: false,
                max_bytes: 0,
                idle_heartbeat: Duration::from_secs(15),
            },
            self,
        )
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
    /// use futures::StreamExt;
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

    pub(crate) async fn request_batch<I: Into<BatchConfig>>(
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
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
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
    pub fn fetch(&self) -> FetchBuilder {
        FetchBuilder::new(self)
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
    pub fn batch(&self) -> BatchBuilder {
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
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
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

impl<'a> Batch {
    async fn batch(batch: BatchConfig, consumer: &Consumer<Config>) -> Result<Batch, BatchError> {
        let inbox = Subject::from(consumer.context.client.new_inbox());
        let subscription = consumer.context.client.subscribe(inbox.clone()).await?;
        consumer.request_batch(batch, inbox.clone()).await?;

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

impl futures::Stream for Batch {
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
                        Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "error while processing messages from the stream: {}, {:?}",
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

pub struct Sequence {
    context: Context,
    subject: String,
    request: Bytes,
    pending_messages: usize,
    next: Option<BoxFuture<'static, Result<Batch, MessagesError>>>,
}

impl futures::Stream for Sequence {
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
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
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
        let stream = Stream::stream(
            BatchConfig {
                batch: 500,
                expires: Some(Duration::from_secs(30)),
                no_wait: false,
                max_bytes: 0,
                idle_heartbeat: Duration::from_secs(15),
            },
            &config,
        )
        .await?;

        Ok(Ordered {
            consumer_sequence: 0,
            stream_sequence: 0,
            missed_heartbeats: false,
            create_stream: None,
            context: self.context.clone(),
            consumer_name: self
                .config
                .name
                .clone()
                .unwrap_or_else(|| self.context.client.new_inbox()),
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
    #[serde(default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    /// What percentage of acknowledgments should be samples for observability, 0-100
    #[serde(default, skip_serializing_if = "is_default")]
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
    // Additional consumer metadata.
    #[serde(default, skip_serializing_if = "is_default")]
    pub metadata: HashMap<String, String>,
    // Maximum number of messages that can be requested in single Pull Request.
    // This is used explicitly by [batch] and [fetch], but also, under the hood, by [messages] and
    // [stream]
    pub max_batch: i64,
    // Maximum number of bytes that can be requested in single Pull Request.
    // This is used explicitly by [batch] and [fetch], but also, under the hood, by [messages] and
    // [stream]
    pub max_bytes: i64,
    // Maximum expiry that can be set for a single Pull Request.
    // This is used explicitly by [batch] and [fetch], but also, under the hood, by [messages] and
    // [stream]
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
            max_batch: 0,
            max_bytes: 0,
            max_expires: Duration::default(),
            inactive_threshold: Duration::from_secs(30),
            num_replicas: 1,
            memory_storage: true,
            #[cfg(feature = "server_2_10")]
            metadata: self.metadata,
            backoff: Vec::new(),
        }
    }
}

pub struct Ordered {
    context: Context,
    stream_name: String,
    consumer: OrderedConfig,
    consumer_name: String,
    stream: Option<Stream>,
    create_stream: Option<BoxFuture<'static, Result<Stream, ConsumerRecreateError>>>,
    consumer_sequence: u64,
    stream_sequence: u64,
    missed_heartbeats: bool,
}

impl futures::Stream for Ordered {
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
                            MessagesErrorKind::ConsumerDeleted => {
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
            self.create_stream = Some(Box::pin({
                let context = self.context.clone();
                let config = self.consumer.clone();
                let stream_name = self.stream_name.clone();
                let consumer_name = self.consumer_name.clone();
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
                    .retries(5)
                    .exponential_backoff(Duration::from_millis(500))
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
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl Stream {
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
        let task_handle = tokio::task::spawn({
            let batch = batch_config;
            let consumer = consumer.clone();
            let mut context = consumer.context.clone();
            let inbox = inbox.clone();
            async move {
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

                            match tryhard::retry_fn(|| consumer.fetch_info()).retries(5).exponential_backoff(Duration::from_millis(500)).await {
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
                        .unwrap();
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
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MessagesErrorKind {
    MissingHeartbeat,
    ConsumerDeleted,
    Pull,
    PushBasedConsumer,
    Other,
}

impl std::fmt::Display for MessagesErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeartbeat => write!(f, "missed idle heartbeat"),
            Self::ConsumerDeleted => write!(f, "consumer deleted"),
            Self::Pull => write!(f, "pull request failed"),
            Self::Other => write!(f, "error"),
            Self::PushBasedConsumer => write!(f, "cannot use with push consumer"),
        }
    }
}

pub type MessagesError = Error<MessagesErrorKind>;

impl futures::Stream for Stream {
    type Item = Result<jetstream::Message, MessagesError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        if !self.batch_config.idle_heartbeat.is_zero() {
            trace!("checking idle hearbeats");
            let timeout = self.batch_config.idle_heartbeat.saturating_mul(2);
            match self
                .heartbeat_timeout
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)))
                .poll_unpin(cx)
            {
                Poll::Ready(_) => {
                    self.heartbeat_timeout = None;
                    return Poll::Ready(Some(Err(MessagesError::new(
                        MessagesErrorKind::MissingHeartbeat,
                    ))));
                }
                Poll::Pending => (),
            }
        }

        loop {
            trace!("pending messages: {}", self.pending_messages);
            if (self.pending_messages <= self.batch_config.batch / 2
                || (self.batch_config.max_bytes > 0
                    && self.pending_bytes <= self.batch_config.max_bytes / 2))
                && !self.pending_request
            {
                debug!("pending messages reached threshold to send new fetch request");
                self.request_tx.send(()).unwrap();
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
                                    self.terminated = true;
                                    return Poll::Ready(Some(Err(MessagesError::new(
                                        MessagesErrorKind::ConsumerDeleted,
                                    ))));
                                }
                                // If consumer is not pull based, error and shutdown the iterator.
                                if message.description.as_deref() == Some("Consumer is push based")
                                {
                                    self.terminated = true;
                                    return Poll::Ready(Some(Err(MessagesError::new(
                                        MessagesErrorKind::PushBasedConsumer,
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
                    return std::task::Poll::Pending;
                }
            }
        }
    }
}

/// Used for building configuration for a [Stream]. Created by a [Consumer::stream] on a [Consumer].
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error>  {
/// use futures::StreamExt;
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
    batch: usize,
    max_bytes: usize,
    heartbeat: Duration,
    expires: Duration,
    consumer: &'a Consumer<Config>,
}

impl<'a> StreamBuilder<'a> {
    pub fn new(consumer: &'a Consumer<Config>) -> Self {
        StreamBuilder {
            consumer,
            batch: 200,
            max_bytes: 0,
            expires: Duration::from_secs(30),
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
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
        Stream::stream(
            BatchConfig {
                batch: self.batch,
                expires: Some(self.expires),
                no_wait: false,
                max_bytes: self.max_bytes,
                idle_heartbeat: self.heartbeat,
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
/// use futures::StreamExt;
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
    consumer: &'a Consumer<Config>,
}

impl<'a> FetchBuilder<'a> {
    pub fn new(consumer: &'a Consumer<Config>) -> Self {
        FetchBuilder {
            consumer,
            batch: 200,
            max_bytes: 0,
            expires: None,
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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

    /// Creates actual [Stream] with provided configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error>  {
    /// use async_nats::jetstream::consumer::PullConsumer;
    /// use futures::StreamExt;
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
/// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
    /// use futures::StreamExt;
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
        Batch::batch(
            BatchConfig {
                batch: self.batch,
                expires: Some(self.expires),
                no_wait: false,
                max_bytes: self.max_bytes,
                idle_heartbeat: self.heartbeat,
            },
            self.consumer,
        )
        .await
    }
}

/// Used for next Pull Request for Pull Consumer
#[derive(Debug, Default, Serialize, Clone, Copy, PartialEq, Eq)]
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
    #[serde(default, skip_serializing_if = "is_default")]
    pub rate_limit: u64,
    /// What percentage of acknowledgments should be samples for observability, 0-100
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
        }
    }
}
impl FromConsumer for Config {
    fn try_from_consumer_config(config: consumer::Config) -> Result<Self, crate::Error> {
        if config.deliver_subject.is_some() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
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

    let config = Consumer {
        config: config.clone().into(),
        context: context.clone(),
        info: consumer.info,
    };

    trace!("create iterator");
    let stream = tokio::time::timeout(
        Duration::from_secs(5),
        Stream::stream(
            BatchConfig {
                batch: 500,
                expires: Some(Duration::from_secs(30)),
                no_wait: false,
                max_bytes: 0,
                idle_heartbeat: Duration::from_secs(15),
            },
            &config,
        ),
    )
    .await
    .map_err(|err| ConsumerRecreateError::with_source(ConsumerRecreateErrorKind::TimedOut, err))?
    .map_err(|err| ConsumerRecreateError::with_source(ConsumerRecreateErrorKind::Recreate, err));
    trace!("recreated consumer");
    stream
}
