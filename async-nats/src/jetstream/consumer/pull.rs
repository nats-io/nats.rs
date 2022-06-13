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

use std::{task::Poll, time::Duration};

use async_stream::try_stream;
use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::{
    jetstream::{self, Context, JetStreamMessage},
    Error, StatusCode, Subscriber,
};

use super::{AckPolicy, Consumer, DeliverPolicy, FromConsumer, IntoConsumerConfig, ReplayPolicy};

use futures::StreamExt;

use jetstream::consumer;

impl Consumer<Config> {
    pub async fn request_batch<I: Into<BatchConfig>>(
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

    pub async fn fetch(&mut self, limit: usize) -> Result<Batch, Error> {
        Batch::batch(
            BatchConfig {
                limit,
                expires: None,
                no_wait: true,
                ..Default::default()
            },
            self,
        )
        .await
    }

    pub async fn batch(&mut self, limit: usize, expires: Option<usize>) -> Result<Batch, Error> {
        Batch::batch(
            BatchConfig {
                limit,
                expires,
                no_wait: false,
                idle_heartbeat: Duration::default(),
                ..Default::default()
            },
            self,
        )
        .await
    }

    pub fn process(
        &'_ mut self,
        limit: usize,
    ) -> impl Stream<Item = Result<Option<JetStreamMessage>, Error>> + '_ {
        try_stream! {
            let inbox = self.context.client.new_inbox();
            let mut sub = self.context.client.subscribe(inbox.clone()).await?;

            self.request_batch(BatchConfig {
                limit,
                expires: None,
                no_wait: false,
                ..Default::default()
            }, inbox.clone())
            .await?;
            let mut remaining = limit;
            while let Some(mut message) = sub.next().await {
                message.status = match message.status.unwrap_or(StatusCode::OK) {
                    StatusCode::TIMEOUT => {
                        remaining = limit;
                        self.request_batch(BatchConfig {
                            limit,
                            expires: None,
                            no_wait: false,
                            ..Default::default()
                        }, inbox.clone())
                        .await?;
                        continue;
                    }
                    StatusCode::IDLE_HEARBEAT => {
                        continue;
                    }
                    status => Some(status),
                };

                remaining -= 1;
                yield Some(JetStreamMessage {
                    context: self.context.clone(),
                    message: message,
                });

                if remaining == 0 {
                    self.request_batch(BatchConfig {
                        limit,
                        expires: None,
                        no_wait: false,
                        ..Default::default()
                    }, inbox.clone())
                    .await?;
                    remaining = limit;
                }
            }
        }
    }
}

pub struct Batch {
    pending_messages: usize,
    subscriber: Subscriber,
    context: Context,
}

impl<'a> Batch {
    async fn batch(batch: BatchConfig, consumer: &'a mut Consumer<Config>) -> Result<Batch, Error> {
        let inbox = consumer.context.client.new_inbox();
        let subscription = consumer.context.client.subscribe(inbox.clone()).await?;
        consumer.request_batch(batch, inbox.clone()).await?;

        Ok(Batch {
            pending_messages: batch.limit,
            subscriber: subscription,
            context: consumer.context.clone(),
        })
    }
}

impl Stream for Batch {
    type Item = Result<JetStreamMessage, Error>;

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
                    _ => {
                        self.pending_messages -= 1;
                        Poll::Ready(Some(Ok(JetStreamMessage {
                            context: self.context.clone(),
                            message,
                        })))
                    }
                },
                None => Poll::Ready(None),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// Used for next Pull Request for Pull Consumer
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct BatchConfig {
    /// The number of messages that are being requested to be delivered.
    #[serde(rename = "batch")]
    pub limit: usize,
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
