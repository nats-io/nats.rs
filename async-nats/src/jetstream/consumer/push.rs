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

use super::{AckPolicy, Consumer, DeliverPolicy, FromConsumer, IntoConsumerConfig, ReplayPolicy};
use crate::{
    connection::State,
    jetstream::{self, Context, Message},
    Error, StatusCode, Subscriber,
};

use bytes::Bytes;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
    io::{self, ErrorKind},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};
use std::{
    sync::atomic::AtomicU64,
    task::{self, Poll},
};
use std::{sync::atomic::Ordering, time::Duration};
use tokio::sync::oneshot::error::TryRecvError;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tracing::{debug, trace};

impl Consumer<Config> {
    /// Returns a stream of messages for Push Consumer.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
    /// use async_nats::jetstream::consumer::PushConsumer;
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
    /// let consumer: PushConsumer = stream.get_or_create_consumer("consumer", async_nats::jetstream::consumer::push::Config {
    ///     durable_name: Some("consumer".to_string()),
    ///     deliver_subject: "deliver".to_string(),
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let mut messages = consumer.messages().await?.take(100);
    /// while let Some(Ok(message)) = messages.next().await {
    ///   println!("got message {:?}", message);
    ///   message.ack().await?;
    /// }
    /// Ok(())
    /// # }
    /// ```
    pub async fn messages(&self) -> Result<Messages, Error> {
        let deliver_subject = self.info.config.deliver_subject.clone().unwrap();
        let subscriber = self.context.client.subscribe(deliver_subject).await?;

        Ok(Messages {
            context: self.context.clone(),
            subscriber,
        })
    }
}

pub struct Messages {
    context: Context,
    subscriber: Subscriber,
}

impl futures::Stream for Messages {
    type Item = Result<Message, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.subscriber.receiver.poll_recv(cx) {
                Poll::Ready(maybe_message) => match maybe_message {
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
                },
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
}

impl FromConsumer for Config {
    fn try_from_consumer_config(config: super::Config) -> Result<Self, Error> {
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
            replay_policy: self.replay_policy,
            rate_limit: self.rate_limit,
            sample_frequency: self.sample_frequency,
            max_waiting: self.max_waiting,
            max_ack_pending: self.max_ack_pending,
            headers_only: self.headers_only,
            flow_control: self.flow_control,
            idle_heartbeat: self.idle_heartbeat,
            max_batch: 0,
            max_expires: Duration::default(),
            inactive_threshold: Duration::default(),
            num_replicas: self.num_replicas,
            memory_storage: self.memory_storage,
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
}

impl FromConsumer for OrderedConfig {
    fn try_from_consumer_config(config: crate::jetstream::consumer::Config) -> Result<Self, Error>
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
            replay_policy: config.replay_policy,
            rate_limit: config.rate_limit,
            sample_frequency: config.sample_frequency,
            headers_only: config.headers_only,
            deliver_policy: config.deliver_policy,
            max_waiting: config.max_waiting,
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
            ack_wait: Duration::from_secs(60 * 60 * 22),
            max_deliver: 1,
            filter_subject: self.filter_subject,
            replay_policy: self.replay_policy,
            rate_limit: self.rate_limit,
            sample_frequency: self.sample_frequency,
            max_waiting: self.max_waiting,
            max_ack_pending: 0,
            headers_only: self.headers_only,
            flow_control: true,
            idle_heartbeat: Duration::from_secs(5),
            max_batch: 0,
            max_expires: Duration::default(),
            inactive_threshold: Duration::from_secs(30),
            num_replicas: 1,
            memory_storage: true,
        }
    }
}

impl Consumer<OrderedConfig> {
    pub async fn messages<'a>(self) -> Result<Ordered<'a>, Error> {
        let subscriber = self
            .context
            .client
            .subscribe(self.info.config.deliver_subject.clone().unwrap())
            .await?;

        let last_seen = Arc::new(Mutex::new(Instant::now()));
        let last_sequence = Arc::new(AtomicU64::new(0));
        let consumer_sequence = Arc::new(AtomicU64::new(0));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::task::spawn({
            let last_seen = last_seen.clone();
            let stream_name = self.info.stream_name.clone();
            let config = self.config.clone();
            let mut context = self.context.clone();
            let last_sequence = last_sequence.clone();
            let consumer_sequence = consumer_sequence.clone();
            let state = self.context.client.state.clone();
            async move {
                loop {
                    let current_state = state.borrow().to_owned();
                    tokio::select! {
                        _ = context.client.state.changed() => {
                            if state.borrow().to_owned() != State::Connected  || current_state == State::Connected {
                               continue;
                            }
                            debug!("reconnected. trigger consumer recreation");
                        },
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {
                            debug!("hearbeat check");

                            if !last_seen
                                .lock()
                                .unwrap()
                                .elapsed()
                                .gt(&Duration::from_secs(10)) {
                                    println!("last seen ok. wait");
                                    continue;
                                    }
                        println!("last seen not ok");
                                }
                    }
                    debug!(
                        "idle hearbeats expired. recreating consumer s: {},  {:?}",
                        stream_name, config
                    );
                    let retry_strategy = ExponentialBackoff::from_millis(500).take(5);
                    let consumer = Retry::spawn(retry_strategy, || {
                        recreate_ephemeral_consumer(
                            context.clone(),
                            config.clone(),
                            stream_name.clone(),
                            last_sequence.load(Ordering::Relaxed),
                        )
                    })
                    .await;
                    if let Err(err) = consumer {
                        shutdown_tx.send(err).unwrap();
                        break;
                    }
                    *last_seen.lock().unwrap() = Instant::now();
                    debug!("resseting consume sequence to 0");
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
            last_seen,
            shutdown: shutdown_rx,
        })
    }
}

pub struct Ordered<'a> {
    context: Context,
    consumer: Consumer<OrderedConfig>,
    subscriber: Option<Subscriber>,
    subscriber_future: Option<BoxFuture<'a, Result<Subscriber, Error>>>,
    stream_sequence: Arc<AtomicU64>,
    consumer_sequence: Arc<AtomicU64>,
    last_seen: Arc<Mutex<Instant>>,
    shutdown: tokio::sync::oneshot::Receiver<Error>,
}

impl<'a> futures::Stream for Ordered<'a> {
    type Item = Result<Message, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.shutdown.try_recv() {
                Ok(err) => return Poll::Ready(Some(Err(err))),
                Err(TryRecvError::Closed) => {
                    return Poll::Ready(Some(Err(Box::from(io::Error::new(
                        ErrorKind::Other,
                        "push consumer task closed",
                    )))))
                }
                Err(TryRecvError::Empty) => {}
            }
            if self.subscriber.is_none() {
                match self.subscriber_future.as_mut() {
                    None => {
                        let context = self.context.clone();
                        let sequence = self.stream_sequence.clone();
                        let config = self.consumer.config.clone();
                        let stream_name = self.consumer.info.stream_name.clone();
                        self.subscriber_future = Some(Box::pin(async move {
                            recreate_consumer_and_subscription(
                                context,
                                config,
                                stream_name,
                                sequence.load(Ordering::Relaxed),
                            )
                            .await
                        }));
                        match self.subscriber_future.as_mut().unwrap().as_mut().poll(cx) {
                            Poll::Ready(subscriber) => {
                                self.subscriber_future = None;
                                self.subscriber = Some(subscriber?);
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
                            self.subscriber = Some(subscriber?);
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    },
                }
            }
            if let Some(subscriber) = self.subscriber.as_mut() {
                match subscriber.receiver.poll_recv(cx) {
                    Poll::Ready(maybe_message) => {
                        match maybe_message {
                            Some(message) => {
                                *self.last_seen.lock().unwrap() = Instant::now();
                                match message.status {
                                    Some(StatusCode::IDLE_HEARTBEAT) => {
                                        debug!("received idle hearbeats");
                                        if let Some(headers) = message.headers.as_ref() {
                                            if let Some(sequence) =
                                                headers.get(crate::header::NATS_LAST_STREAM)
                                            {
                                                let sequence: u64 = sequence
                                                    .iter().next().unwrap()
                                                    .parse()
                                                    .map_err(|err|
                                                           Box::new(io::Error::new(
                                                                   ErrorKind::Other,
                                                                   format!("could not parse header into u64: {}", err))
                                                               ))?;

                                                if sequence
                                                    != self.stream_sequence.load(Ordering::Relaxed)
                                                {
                                                    self.subscriber = None;
                                                }
                                            }
                                        }
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
                                        let jetstream_message = jetstream::message::Message {
                                            message,
                                            context: self.context.clone(),
                                        };

                                        let info = jetstream_message.info()?;
                                        trace!("consumer sequence: {:?}, stream sequence {:?}, consumer sequence in message: {:?} stream sequence in message: {:?}",
                                               self.consumer_sequence,
                                               self.stream_sequence,
                                               info.consumer_sequence,
                                               info.stream_sequence);
                                        if info.consumer_sequence
                                            != self.consumer_sequence.load(Ordering::Relaxed) + 1
                                            && info.stream_sequence
                                                != self.stream_sequence.load(Ordering::Relaxed) + 1
                                        {
                                            debug!(
                                                "ordered consumer mismatch. current {}, info: {}",
                                                self.consumer_sequence.load(Ordering::Relaxed),
                                                info.consumer_sequence
                                            );
                                            self.subscriber = None;
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
                                debug!("received None from subscription");
                                return Poll::Ready(None);
                            }
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }
}

async fn recreate_consumer_and_subscription(
    context: Context,
    config: OrderedConfig,
    stream_name: String,
    sequence: u64,
) -> Result<Subscriber, Error> {
    let subscriber = context
        .client
        .subscribe(config.deliver_subject.clone())
        .await?;

    recreate_ephemeral_consumer(context, config, stream_name, sequence).await?;
    Ok(subscriber)
}
async fn recreate_ephemeral_consumer(
    context: Context,
    config: OrderedConfig,
    stream_name: String,
    sequence: u64,
) -> Result<(), Error> {
    let stream = context.get_stream(stream_name.clone()).await?;

    let deliver_policy = {
        if sequence == 0 {
            DeliverPolicy::All
        } else {
            DeliverPolicy::ByStartSequence {
                start_sequence: sequence + 1,
            }
        }
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        stream.create_consumer(jetstream::consumer::push::OrderedConfig {
            deliver_policy,
            ..config
        }),
    )
    .await
    .map_err(|_| io::Error::new(ErrorKind::TimedOut, "timed out"))??;
    Ok(())
}
