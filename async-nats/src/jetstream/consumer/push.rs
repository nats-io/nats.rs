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

use super::{
    AckPolicy, Consumer, DeliverPolicy, FromConsumer, IntoConsumerConfig, ReplayPolicy,
    StreamError, StreamErrorKind,
};
use crate::{
    connection::State,
    error::Error,
    jetstream::{self, Context, Message},
    StatusCode, Subscriber,
};

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use portable_atomic::AtomicU64;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server_2_10")]
use std::collections::HashMap;
use std::task::{self, Poll};
use std::{
    io::{self, ErrorKind},
    pin::Pin,
    sync::Arc,
};
use std::{sync::atomic::Ordering, time::Duration};
use tokio::{sync::oneshot::error::TryRecvError, task::JoinHandle};
use tracing::{debug, trace};

const ORDERED_IDLE_HEARTBEAT: Duration = Duration::from_secs(5);

impl Consumer<Config> {
    /// Returns a stream of messages for Push Consumer.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer::PushConsumer;
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
    /// let consumer: PushConsumer = stream
    ///     .get_or_create_consumer(
    ///         "consumer",
    ///         async_nats::jetstream::consumer::push::Config {
    ///             durable_name: Some("consumer".to_string()),
    ///             deliver_subject: "deliver".to_string(),
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
    pub async fn messages(&self) -> Result<Messages, StreamError> {
        let deliver_subject = self.info.config.deliver_subject.clone().unwrap();
        let subscriber = if let Some(ref group) = self.info.config.deliver_group {
            self.context
                .client
                .queue_subscribe(deliver_subject, group.to_owned())
                .await
                .map_err(|err| StreamError::with_source(StreamErrorKind::Other, err))?
        } else {
            self.context
                .client
                .subscribe(deliver_subject)
                .await
                .map_err(|err| StreamError::with_source(StreamErrorKind::Other, err))?
        };

        Ok(Messages {
            context: self.context.clone(),
            config: self.config.clone(),
            subscriber,
            heartbeat_sleep: None,
        })
    }
}

pub struct Messages {
    context: Context,
    subscriber: Subscriber,
    config: Config,
    heartbeat_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl futures::Stream for Messages {
    type Item = Result<Message, MessagesError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.config.idle_heartbeat.is_zero() {
            let heartbeat_sleep = self.config.idle_heartbeat.saturating_mul(2);
            match self
                .heartbeat_sleep
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(heartbeat_sleep)))
                .poll_unpin(cx)
            {
                Poll::Ready(_) => {
                    self.heartbeat_sleep = None;
                    return Poll::Ready(Some(Err(MessagesError::new(
                        MessagesErrorKind::MissingHeartbeat,
                    ))));
                }
                Poll::Pending => (),
            }
        }
        loop {
            match self.subscriber.receiver.poll_recv(cx) {
                Poll::Ready(maybe_message) => {
                    self.heartbeat_sleep = None;
                    match maybe_message {
                        Some(message) => match message.status {
                            Some(StatusCode::IDLE_HEARTBEAT) => {
                                if let Some(subject) = message.reply {
                                    // TODO store pending_publish as a future and return errors from it
                                    let client = self.context.client.clone();
                                    tokio::task::spawn(async move {
                                        client
                                            .publish(subject, Bytes::from_static(b""))
                                            .await
                                            .unwrap();
                                    });
                                }

                                continue;
                            }
                            Some(_) => {
                                continue;
                            }
                            None => {
                                return Poll::Ready(Some(Ok(jetstream::Message {
                                    context: self.context.clone(),
                                    message,
                                })))
                            }
                        },
                        None => return Poll::Ready(None),
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Configuration for consumers. From a high level, the
/// `durable_name` and `deliver_subject` fields have a particularly
/// strong influence on the consumer's overall behavior.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Config {
    /// The delivery subject used by the push consumer.
    #[serde(default)]
    pub deliver_subject: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Deliver group to use.
    pub deliver_group: Option<String>,
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
    /// Enable flow control messages
    #[serde(default, skip_serializing_if = "is_default")]
    pub flow_control: bool,
    /// Enable idle heartbeat messages
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub idle_heartbeat: Duration,
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
    /// Threshold for consumer inactivity
    #[serde(default, with = "serde_nanos", skip_serializing_if = "is_default")]
    pub inactive_threshold: Duration,
}

impl FromConsumer for Config {
    fn try_from_consumer_config(config: super::Config) -> Result<Self, crate::Error> {
        if config.deliver_subject.is_none() {
            return Err(Box::new(io::Error::new(
                ErrorKind::Other,
                "push consumer must have delivery subject",
            )));
        }

        Ok(Config {
            deliver_subject: config.deliver_subject.unwrap(),
            durable_name: config.durable_name,
            name: config.name,
            description: config.description,
            deliver_group: config.deliver_group,
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
            flow_control: config.flow_control,
            idle_heartbeat: config.idle_heartbeat,
            num_replicas: config.num_replicas,
            memory_storage: config.memory_storage,
            #[cfg(feature = "server_2_10")]
            metadata: config.metadata,
            backoff: config.backoff,
            inactive_threshold: config.inactive_threshold,
        })
    }
}

impl IntoConsumerConfig for Config {
    fn into_consumer_config(self) -> jetstream::consumer::Config {
        jetstream::consumer::Config {
            deliver_subject: Some(self.deliver_subject),
            durable_name: self.durable_name,
            name: self.name,
            description: self.description,
            deliver_group: self.deliver_group,
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
            flow_control: self.flow_control,
            idle_heartbeat: self.idle_heartbeat,
            max_batch: 0,
            max_bytes: 0,
            max_expires: Duration::default(),
            inactive_threshold: self.inactive_threshold,
            num_replicas: self.num_replicas,
            memory_storage: self.memory_storage,
            #[cfg(feature = "server_2_10")]
            metadata: self.metadata,
            backoff: self.backoff,
        }
    }
}
impl IntoConsumerConfig for &Config {
    fn into_consumer_config(self) -> jetstream::consumer::Config {
        self.clone().into_consumer_config()
    }
}
fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}

/// Configuration for consumers. From a high level, the
/// `durable_name` and `deliver_subject` fields have a particularly
/// strong influence on the consumer's overall behavior.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OrderedConfig {
    /// The delivery subject used by the push consumer.
    #[serde(default)]
    pub deliver_subject: String,
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
}

impl FromConsumer for OrderedConfig {
    fn try_from_consumer_config(
        config: crate::jetstream::consumer::Config,
    ) -> Result<Self, crate::Error>
    where
        Self: Sized,
    {
        if config.deliver_subject.is_none() {
            return Err(Box::new(io::Error::new(
                ErrorKind::Other,
                "push consumer must have delivery subject",
            )));
        }
        Ok(OrderedConfig {
            name: config.name,
            deliver_subject: config.deliver_subject.unwrap(),
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
        })
    }
}

impl IntoConsumerConfig for OrderedConfig {
    fn into_consumer_config(self) -> super::Config {
        jetstream::consumer::Config {
            deliver_subject: Some(self.deliver_subject),
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
            flow_control: true,
            idle_heartbeat: ORDERED_IDLE_HEARTBEAT,
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

impl Consumer<OrderedConfig> {
    pub async fn messages<'a>(self) -> Result<Ordered, StreamError> {
        let subscriber = self
            .context
            .client
            .subscribe(self.info.config.deliver_subject.clone().unwrap())
            .await
            .map_err(|err| StreamError::with_source(StreamErrorKind::Other, err))?;

        let last_sequence = Arc::new(AtomicU64::new(0));
        let consumer_sequence = Arc::new(AtomicU64::new(0));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::task::spawn({
            let stream_name = self.info.stream_name.clone();
            let config = self.config.clone();
            let mut context = self.context.clone();
            let last_sequence = last_sequence.clone();
            let consumer_sequence = consumer_sequence.clone();
            let state = self.context.client.state.clone();
            async move {
                loop {
                    let current_state = state.borrow().to_owned();

                    context.client.state.changed().await.unwrap();
                    // State change notification received within the timeout
                    if state.borrow().to_owned() != State::Connected
                        || current_state == State::Connected
                    {
                        continue;
                    }
                    debug!("reconnected. trigger consumer recreation");

                    debug!(
                        "idle heartbeats expired. recreating consumer s: {},  {:?}",
                        stream_name, config
                    );
                    let consumer = tryhard::retry_fn(|| {
                        recreate_ephemeral_consumer(
                            context.clone(),
                            config.clone(),
                            stream_name.clone(),
                            last_sequence.load(Ordering::Relaxed),
                        )
                    })
                    .retries(5)
                    .exponential_backoff(Duration::from_millis(500))
                    .await;
                    if let Err(err) = consumer {
                        shutdown_tx.send(err).unwrap();
                        break;
                    }
                    debug!("resetting consume sequence to 0");
                    consumer_sequence.store(0, Ordering::Relaxed);
                }
            }
        });

        Ok(Ordered {
            context: self.context.clone(),
            consumer: self,
            subscriber: Some(subscriber),
            subscriber_future: None,
            stream_sequence: last_sequence,
            consumer_sequence,
            shutdown: shutdown_rx,
            handle,
            heartbeat_sleep: None,
        })
    }
}

pub struct Ordered {
    context: Context,
    consumer: Consumer<OrderedConfig>,
    subscriber: Option<Subscriber>,
    subscriber_future: Option<BoxFuture<'static, Result<Subscriber, ConsumerRecreateError>>>,
    stream_sequence: Arc<AtomicU64>,
    consumer_sequence: Arc<AtomicU64>,
    shutdown: tokio::sync::oneshot::Receiver<ConsumerRecreateError>,
    handle: JoinHandle<()>,
    heartbeat_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Drop for Ordered {
    fn drop(&mut self) {
        // Stop trying to recreate the consumer
        self.handle.abort()
    }
}

impl futures::Stream for Ordered {
    type Item = Result<Message, OrderedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        match self
            .heartbeat_sleep
            .get_or_insert_with(|| {
                Box::pin(tokio::time::sleep(ORDERED_IDLE_HEARTBEAT.saturating_mul(2)))
            })
            .poll_unpin(cx)
        {
            Poll::Ready(_) => {
                self.heartbeat_sleep = None;
                return Poll::Ready(Some(Err(OrderedError::new(
                    OrderedErrorKind::MissingHeartbeat,
                ))));
            }
            Poll::Pending => (),
        }

        loop {
            match self.shutdown.try_recv() {
                Ok(err) => {
                    return Poll::Ready(Some(Err(OrderedError::with_source(
                        OrderedErrorKind::Other,
                        err,
                    ))))
                }
                Err(TryRecvError::Closed) => {
                    return Poll::Ready(Some(Err(OrderedError::with_source(
                        OrderedErrorKind::Other,
                        "consumer task closed",
                    ))))
                }
                Err(TryRecvError::Empty) => {}
            }
            if self.subscriber.is_none() {
                match self.subscriber_future.as_mut() {
                    None => {
                        trace!(
                            "subscriber and subscriber future are None. Recreating the consumer"
                        );
                        let context = self.context.clone();
                        let sequence = self.stream_sequence.clone();
                        let config = self.consumer.config.clone();
                        let stream_name = self.consumer.info.stream_name.clone();
                        let subscriber_future =
                            self.subscriber_future.insert(Box::pin(async move {
                                recreate_consumer_and_subscription(
                                    context,
                                    config,
                                    stream_name,
                                    sequence.load(Ordering::Relaxed),
                                )
                                .await
                            }));
                        match subscriber_future.as_mut().poll(cx) {
                            Poll::Ready(subscriber) => {
                                self.subscriber_future = None;
                                self.consumer_sequence.store(0, Ordering::Relaxed);
                                self.subscriber = Some(subscriber.map_err(|err| {
                                    OrderedError::with_source(OrderedErrorKind::Recreate, err)
                                })?);
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    }
                    Some(subscriber) => match subscriber.as_mut().poll(cx) {
                        Poll::Ready(subscriber) => {
                            self.subscriber_future = None;
                            self.consumer_sequence.store(0, Ordering::Relaxed);
                            self.subscriber = Some(subscriber.map_err(|err| {
                                OrderedError::with_source(OrderedErrorKind::Recreate, err)
                            })?);
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    },
                }
            }
            if let Some(subscriber) = self.subscriber.as_mut() {
                match subscriber.receiver.poll_recv(cx) {
                    Poll::Ready(maybe_message) => match maybe_message {
                        Some(message) => {
                            self.heartbeat_sleep = None;
                            match message.status {
                                Some(StatusCode::IDLE_HEARTBEAT) => {
                                    debug!("received idle heartbeats");
                                    if let Some(headers) = message.headers.as_ref() {
                                        if let Some(sequence) =
                                            headers.get_last(crate::header::NATS_LAST_CONSUMER)
                                        {
                                            let sequence: u64 =
                                                sequence.as_str().parse().map_err(|err| {
                                                    OrderedError::with_source(
                                                        OrderedErrorKind::Other,
                                                        err,
                                                    )
                                                })?;

                                            let last_sequence =
                                                self.consumer_sequence.load(Ordering::Relaxed);

                                            if sequence != last_sequence {
                                                debug!("hearbeats sequence mismatch. got {}, expected {}, resetting consumer", sequence, last_sequence);
                                                self.subscriber = None;
                                            }
                                        }
                                    }
                                    // flow control.
                                    if let Some(subject) = message.reply.clone() {
                                        trace!("received flow control message");
                                        let client = self.context.client.clone();
                                        tokio::task::spawn(async move {
                                            client
                                                .publish(subject, Bytes::from_static(b""))
                                                .await
                                                .ok();
                                        });
                                    }
                                    continue;
                                }
                                Some(status) => {
                                    debug!("received status message: {}", status);
                                    continue;
                                }
                                None => {
                                    trace!("received a message");
                                    let jetstream_message = jetstream::message::Message {
                                        message,
                                        context: self.context.clone(),
                                    };

                                    let info = jetstream_message.info().map_err(|err| {
                                        OrderedError::with_source(OrderedErrorKind::Other, err)
                                    })?;
                                    trace!("consumer sequence: {:?}, stream sequence {:?}, consumer sequence in message: {:?} stream sequence in message: {:?}",
                                               self.consumer_sequence,
                                               self.stream_sequence,
                                               info.consumer_sequence,
                                               info.stream_sequence);
                                    if info.consumer_sequence
                                        != self.consumer_sequence.load(Ordering::Relaxed) + 1
                                    {
                                        debug!(
                                            "ordered consumer mismatch. current {}, info: {}",
                                            self.consumer_sequence.load(Ordering::Relaxed),
                                            info.consumer_sequence
                                        );
                                        self.subscriber = None;
                                        self.consumer_sequence.store(0, Ordering::Relaxed);
                                        continue;
                                    }
                                    self.stream_sequence
                                        .store(info.stream_sequence, Ordering::Relaxed);
                                    self.consumer_sequence
                                        .store(info.consumer_sequence, Ordering::Relaxed);
                                    return Poll::Ready(Some(Ok(jetstream_message)));
                                }
                            }
                        }
                        None => {
                            return Poll::Ready(None);
                        }
                    },
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum OrderedErrorKind {
    MissingHeartbeat,
    ConsumerDeleted,
    PullBasedConsumer,
    Recreate,
    Other,
}

impl std::fmt::Display for OrderedErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeartbeat => write!(f, "missed idle heartbeat"),
            Self::ConsumerDeleted => write!(f, "consumer deleted"),
            Self::Other => write!(f, "error"),
            Self::PullBasedConsumer => write!(f, "cannot use with push consumer"),
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
            MessagesErrorKind::PullBasedConsumer => {
                OrderedError::new(OrderedErrorKind::PullBasedConsumer)
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
    PullBasedConsumer,
    Other,
}

impl std::fmt::Display for MessagesErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeartbeat => write!(f, "missed idle heartbeat"),
            Self::ConsumerDeleted => write!(f, "consumer deleted"),
            Self::Other => write!(f, "error"),
            Self::PullBasedConsumer => write!(f, "cannot use with pull consumer"),
        }
    }
}

pub type MessagesError = Error<MessagesErrorKind>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConsumerRecreateErrorKind {
    GetStream,
    Subscription,
    Recreate,
    TimedOut,
}

impl std::fmt::Display for ConsumerRecreateErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GetStream => write!(f, "error getting stream"),
            Self::Recreate => write!(f, "consumer creation failed"),
            Self::TimedOut => write!(f, "timed out"),
            Self::Subscription => write!(f, "failed to resubscribe"),
        }
    }
}

pub type ConsumerRecreateError = Error<ConsumerRecreateErrorKind>;

async fn recreate_consumer_and_subscription(
    context: Context,
    mut config: OrderedConfig,
    stream_name: String,
    sequence: u64,
) -> Result<Subscriber, ConsumerRecreateError> {
    let delivery_subject = context.client.new_inbox();
    config.deliver_subject = delivery_subject;

    let subscriber = context
        .client
        .subscribe(config.deliver_subject.clone())
        .await
        .map_err(|err| {
            ConsumerRecreateError::with_source(ConsumerRecreateErrorKind::Subscription, err)
        })?;

    recreate_ephemeral_consumer(context, config, stream_name, sequence).await?;
    Ok(subscriber)
}
async fn recreate_ephemeral_consumer(
    context: Context,
    config: OrderedConfig,
    stream_name: String,
    sequence: u64,
) -> Result<(), ConsumerRecreateError> {
    let strategy =
        tryhard::RetryFutureConfig::new(5).exponential_backoff(Duration::from_millis(500));

    let stream = tryhard::retry_fn(|| context.get_stream(stream_name.clone()))
        .with_config(strategy)
        .await
        .map_err(|err| {
            ConsumerRecreateError::with_source(ConsumerRecreateErrorKind::GetStream, err)
        })?;

    let deliver_policy = {
        if sequence == 0 {
            DeliverPolicy::All
        } else {
            DeliverPolicy::ByStartSequence {
                start_sequence: sequence + 1,
            }
        }
    };

    tryhard::retry_fn(|| {
        let config = config.clone();
        tokio::time::timeout(
            Duration::from_secs(5),
            stream.create_consumer(jetstream::consumer::push::OrderedConfig {
                deliver_policy,
                ..config
            }),
        )
    })
    .with_config(strategy)
    .await
    .map_err(|_| ConsumerRecreateError::new(ConsumerRecreateErrorKind::TimedOut))?
    .map_err(|err| ConsumerRecreateError::with_source(ConsumerRecreateErrorKind::Recreate, err))?;

    Ok(())
}
