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
//
//! Manage operations on a [Stream], create/delete/update [Consumer].

#[cfg(feature = "server_2_10")]
use std::collections::HashMap;
use std::{
    fmt::{self, Debug, Display},
    future::IntoFuture,
    io::{self, ErrorKind},
    pin::Pin,
    str::FromStr,
    task::Poll,
    time::Duration,
};

use crate::{
    error::Error, header::HeaderName, is_valid_subject, HeaderMap, HeaderValue, StatusCode,
};
use base64::engine::general_purpose::STANDARD;
use base64::engine::Engine;
use bytes::Bytes;
use futures::{future::BoxFuture, TryFutureExt};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;
use time::{serde::rfc3339, OffsetDateTime};

use super::{
    consumer::{self, Consumer, FromConsumer, IntoConsumerConfig},
    context::{RequestError, RequestErrorKind, StreamsError, StreamsErrorKind},
    errors::ErrorCode,
    response::Response,
    Context, Message,
};

pub type InfoError = RequestError;

#[derive(Clone, Debug, PartialEq)]
pub enum DirectGetErrorKind {
    NotFound,
    InvalidSubject,
    TimedOut,
    Request,
    ErrorResponse(StatusCode, String),
    Other,
}

impl Display for DirectGetErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSubject => write!(f, "invalid subject"),
            Self::NotFound => write!(f, "message not found"),
            Self::ErrorResponse(status, description) => {
                write!(f, "unable to get message: {} {}", status, description)
            }
            Self::Other => write!(f, "error getting message"),
            Self::TimedOut => write!(f, "timed out"),
            Self::Request => write!(f, "request failed"),
        }
    }
}

pub type DirectGetError = Error<DirectGetErrorKind>;

impl From<crate::RequestError> for DirectGetError {
    fn from(err: crate::RequestError) -> Self {
        match err.kind() {
            crate::RequestErrorKind::TimedOut => DirectGetError::new(DirectGetErrorKind::TimedOut),
            crate::RequestErrorKind::NoResponders => DirectGetError::new(DirectGetErrorKind::Other),
            crate::RequestErrorKind::Other => {
                DirectGetError::with_source(DirectGetErrorKind::Other, err)
            }
        }
    }
}

impl From<serde_json::Error> for DirectGetError {
    fn from(err: serde_json::Error) -> Self {
        DirectGetError::with_source(DirectGetErrorKind::Other, err)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DeleteMessageErrorKind {
    Request,
    TimedOut,
    JetStream(super::errors::Error),
}

impl Display for DeleteMessageErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request => write!(f, "request failed"),
            Self::TimedOut => write!(f, "timed out"),
            Self::JetStream(err) => write!(f, "JetStream error: {}", err),
        }
    }
}

pub type DeleteMessageError = Error<DeleteMessageErrorKind>;

/// Handle to operations that can be performed on a `Stream`.
#[derive(Debug, Clone)]
pub struct Stream {
    pub(crate) info: Info,
    pub(crate) context: Context,
}

impl Stream {
    /// Retrieves `info` about [Stream] from the server, updates the cached `info` inside
    /// [Stream] and returns it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let mut stream = jetstream.get_stream("events").await?;
    ///
    /// let info = stream.info().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn info(&mut self) -> Result<&Info, InfoError> {
        let subject = format!("STREAM.INFO.{}", self.info.config.name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok::<Info>(info) => {
                self.info = info;
                Ok(&self.info)
            }
            Response::Err { error } => Err(error.into()),
        }
    }

    /// Returns cached [Info] for the [Stream].
    /// Cache is either from initial creation/retrieval of the [Stream] or last call to
    /// [Stream::info].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    ///
    /// let info = stream.cached_info();
    /// # Ok(())
    /// # }
    /// ```
    pub fn cached_info(&self) -> &Info {
        &self.info
    }

    /// Gets next message for a [Stream].
    ///
    /// Requires a [Stream] with `allow_direct` set to `true`.
    /// This is different from [Stream::get_raw_message], as it can fetch [Message]
    /// from any replica member. This means read after write is possible,
    /// as that given replica might not yet catch up with the leader.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         subjects: vec!["events.>".to_string()],
    ///         allow_direct: true,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// jetstream.publish("events.data", "data".into()).await?;
    /// let pub_ack = jetstream.publish("events.data", "data".into()).await?;
    ///
    /// let message = stream
    ///     .direct_get_next_for_subject("events.data", Some(pub_ack.await?.sequence))
    ///     .await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn direct_get_next_for_subject<T: AsRef<str>>(
        &self,
        subject: T,
        sequence: Option<u64>,
    ) -> Result<Message, DirectGetError> {
        if !is_valid_subject(&subject) {
            return Err(DirectGetError::new(DirectGetErrorKind::InvalidSubject));
        }
        let request_subject = format!(
            "{}.DIRECT.GET.{}",
            &self.context.prefix, &self.info.config.name
        );
        let payload;
        if let Some(sequence) = sequence {
            payload = json!({
                "seq": sequence,
                "next_by_subj": subject.as_ref(),
            });
        } else {
            payload = json!({
                 "next_by_subj": subject.as_ref(),
            });
        }

        let response = self
            .context
            .client
            .request(
                request_subject,
                serde_json::to_vec(&payload).map(Bytes::from)?,
            )
            .await
            .map(|message| Message {
                message,
                context: self.context.clone(),
            })?;
        if let Some(status) = response.status {
            if let Some(ref description) = response.description {
                match status {
                    StatusCode::NOT_FOUND => {
                        return Err(DirectGetError::new(DirectGetErrorKind::NotFound))
                    }
                    // 408 is used in Direct Message for bad/empty payload.
                    StatusCode::TIMEOUT => {
                        return Err(DirectGetError::new(DirectGetErrorKind::InvalidSubject))
                    }
                    _ => {
                        return Err(DirectGetError::new(DirectGetErrorKind::ErrorResponse(
                            status,
                            description.to_string(),
                        )));
                    }
                }
            }
        }
        Ok(response)
    }

    /// Gets first message from [Stream].
    ///
    /// Requires a [Stream] with `allow_direct` set to `true`.
    /// This is different from [Stream::get_raw_message], as it can fetch [Message]
    /// from any replica member. This means read after write is possible,
    /// as that given replica might not yet catch up with the leader.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         subjects: vec!["events.>".to_string()],
    ///         allow_direct: true,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// let pub_ack = jetstream.publish("events.data", "data".into()).await?;
    ///
    /// let message = stream.direct_get_first_for_subject("events.data").await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn direct_get_first_for_subject<T: AsRef<str>>(
        &self,
        subject: T,
    ) -> Result<Message, DirectGetError> {
        if !is_valid_subject(&subject) {
            return Err(DirectGetError::new(DirectGetErrorKind::InvalidSubject));
        }
        let request_subject = format!(
            "{}.DIRECT.GET.{}",
            &self.context.prefix, &self.info.config.name
        );
        let payload = json!({
            "next_by_subj": subject.as_ref(),
        });

        let response = self
            .context
            .client
            .request(
                request_subject,
                serde_json::to_vec(&payload).map(Bytes::from)?,
            )
            .await
            .map(|message| Message {
                message,
                context: self.context.clone(),
            })?;
        if let Some(status) = response.status {
            if let Some(ref description) = response.description {
                match status {
                    StatusCode::NOT_FOUND => {
                        return Err(DirectGetError::new(DirectGetErrorKind::NotFound))
                    }
                    // 408 is used in Direct Message for bad/empty payload.
                    StatusCode::TIMEOUT => {
                        return Err(DirectGetError::new(DirectGetErrorKind::InvalidSubject))
                    }
                    _ => {
                        return Err(DirectGetError::new(DirectGetErrorKind::ErrorResponse(
                            status,
                            description.to_string(),
                        )));
                    }
                }
            }
        }
        Ok(response)
    }

    /// Gets message from [Stream] with given `sequence id`.
    ///
    /// Requires a [Stream] with `allow_direct` set to `true`.
    /// This is different from [Stream::get_raw_message], as it can fetch [Message]
    /// from any replica member. This means read after write is possible,
    /// as that given replica might not yet catch up with the leader.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         subjects: vec!["events.>".to_string()],
    ///         allow_direct: true,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// let pub_ack = jetstream.publish("events.data", "data".into()).await?;
    ///
    /// let message = stream.direct_get(pub_ack.await?.sequence).await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn direct_get(&self, sequence: u64) -> Result<Message, DirectGetError> {
        let subject = format!(
            "{}.DIRECT.GET.{}",
            &self.context.prefix, &self.info.config.name
        );
        let payload = json!({
            "seq": sequence,
        });

        let response = self
            .context
            .client
            .request(subject, serde_json::to_vec(&payload).map(Bytes::from)?)
            .await
            .map(|message| Message {
                context: self.context.clone(),
                message,
            })?;

        if let Some(status) = response.status {
            if let Some(ref description) = response.description {
                match status {
                    StatusCode::NOT_FOUND => {
                        return Err(DirectGetError::new(DirectGetErrorKind::NotFound))
                    }
                    // 408 is used in Direct Message for bad/empty payload.
                    StatusCode::TIMEOUT => {
                        return Err(DirectGetError::new(DirectGetErrorKind::InvalidSubject))
                    }
                    _ => {
                        return Err(DirectGetError::new(DirectGetErrorKind::ErrorResponse(
                            status,
                            description.to_string(),
                        )));
                    }
                }
            }
        }
        Ok(response)
    }

    /// Gets last message for a given `subject`.
    ///
    /// Requires a [Stream] with `allow_direct` set to `true`.
    /// This is different from [Stream::get_raw_message], as it can fetch [Message]
    /// from any replica member. This means read after write is possible,
    /// as that given replica might not yet catch up with the leader.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         subjects: vec!["events.>".to_string()],
    ///         allow_direct: true,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// jetstream.publish("events.data", "data".into()).await?;
    ///
    /// let message = stream.direct_get_last_for_subject("events.data").await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn direct_get_last_for_subject<T: AsRef<str>>(
        &self,
        subject: T,
    ) -> Result<Message, DirectGetError> {
        let subject = format!(
            "{}.DIRECT.GET.{}.{}",
            &self.context.prefix,
            &self.info.config.name,
            subject.as_ref()
        );

        let response = self
            .context
            .client
            .request(subject, "".into())
            .await
            .map(|message| Message {
                context: self.context.clone(),
                message,
            })?;
        if let Some(status) = response.status {
            if let Some(ref description) = response.description {
                match status {
                    StatusCode::NOT_FOUND => {
                        return Err(DirectGetError::new(DirectGetErrorKind::NotFound))
                    }
                    // 408 is used in Direct Message for bad/empty payload.
                    StatusCode::TIMEOUT => {
                        return Err(DirectGetError::new(DirectGetErrorKind::InvalidSubject))
                    }
                    _ => {
                        return Err(DirectGetError::new(DirectGetErrorKind::ErrorResponse(
                            status,
                            description.to_string(),
                        )));
                    }
                }
            }
        }
        Ok(response)
    }
    /// Get a raw message from the stream.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let context = async_nats::jetstream::new(client);
    ///
    /// let stream = context
    ///     .get_or_create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         max_messages: 10_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// let publish_ack = context.publish("events", "data".into()).await?;
    /// let raw_message = stream.get_raw_message(publish_ack.await?.sequence).await?;
    /// println!("Retrieved raw message {:?}", raw_message);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_raw_message(&self, sequence: u64) -> Result<RawMessage, crate::Error> {
        let subject = format!("STREAM.MSG.GET.{}", &self.info.config.name);
        let payload = json!({
            "seq": sequence,
        });

        let response: Response<GetRawMessage> = self.context.request(subject, &payload).await?;
        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!("nats: error while getting message: {}", error),
            ))),
            Response::Ok(value) => Ok(value.message),
        }
    }

    /// Get the last raw message from the stream by subject.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// #[tokio::main]
    /// # async fn mains() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use futures::TryStreamExt;
    ///
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let context = async_nats::jetstream::new(client);
    ///
    /// let stream = context
    ///     .get_or_create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         max_messages: 10_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// let publish_ack = context.publish("events", "data".into()).await?;
    /// let raw_message = stream.get_last_raw_message_by_subject("events").await?;
    /// println!("Retrieved raw message {:?}", raw_message);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_last_raw_message_by_subject(
        &self,
        stream_subject: &str,
    ) -> Result<RawMessage, LastRawMessageError> {
        let subject = format!("STREAM.MSG.GET.{}", &self.info.config.name);
        let payload = json!({
            "last_by_subj":  stream_subject,
        });

        let response: Response<GetRawMessage> = self
            .context
            .request(subject, &payload)
            .map_err(|err| LastRawMessageError::with_source(LastRawMessageErrorKind::Other, err))
            .await?;
        match response {
            Response::Err { error } => {
                if error.error_code() == ErrorCode::NO_MESSAGE_FOUND {
                    Err(LastRawMessageError::new(
                        LastRawMessageErrorKind::NoMessageFound,
                    ))
                } else {
                    Err(LastRawMessageError::new(
                        LastRawMessageErrorKind::JetStream(error),
                    ))
                }
            }
            Response::Ok(value) => Ok(value.message),
        }
    }

    /// Delete a message from the stream.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let context = async_nats::jetstream::new(client);
    ///
    /// let stream = context
    ///     .get_or_create_stream(async_nats::jetstream::stream::Config {
    ///         name: "events".to_string(),
    ///         max_messages: 10_000,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    ///
    /// let publish_ack = context.publish("events", "data".into()).await?;
    /// stream.delete_message(publish_ack.await?.sequence).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_message(&self, sequence: u64) -> Result<bool, DeleteMessageError> {
        let subject = format!("STREAM.MSG.DELETE.{}", &self.info.config.name);
        let payload = json!({
            "seq": sequence,
        });

        let response: Response<DeleteStatus> = self
            .context
            .request(subject, &payload)
            .map_err(|err| match err.kind() {
                RequestErrorKind::TimedOut => {
                    DeleteMessageError::new(DeleteMessageErrorKind::TimedOut)
                }
                _ => DeleteMessageError::with_source(DeleteMessageErrorKind::Request, err),
            })
            .await?;

        match response {
            Response::Err { error } => Err(DeleteMessageError::new(
                DeleteMessageErrorKind::JetStream(error),
            )),
            Response::Ok(value) => Ok(value.success),
        }
    }

    /// Purge `Stream` messages.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// stream.purge().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn purge(&self) -> Purge<No, No> {
        Purge::build(self)
    }

    /// Purge `Stream` messages for a matching subject.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # #[allow(deprecated)]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// stream.purge_subject("data").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(
        since = "0.25.0",
        note = "Overloads have been replaced with an into_future based builder. Use Stream::purge().filter(subject) instead."
    )]
    pub async fn purge_subject<T>(&self, subject: T) -> Result<PurgeResponse, PurgeError>
    where
        T: Into<String>,
    {
        self.purge().filter(subject).await
    }

    /// Create or update `Durable` or `Ephemeral` Consumer (if `durable_name` was not provided) and
    /// returns the info from the server about created [Consumer]
    /// If you want a strict update or create, use [Stream::create_consumer_strict] or [Stream::update_consumer].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// let info = stream
    ///     .create_consumer(consumer::pull::Config {
    ///         durable_name: Some("pull".to_string()),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_consumer<C: IntoConsumerConfig + FromConsumer>(
        &self,
        config: C,
    ) -> Result<Consumer<C>, ConsumerError> {
        self.context
            .create_consumer_on_stream(config, self.info.config.name.clone())
            .await
    }

    /// Update an existing consumer.
    /// This call will fail if the consumer does not exist.
    /// returns the info from the server about updated [Consumer].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// let info = stream
    ///     .update_consumer(consumer::pull::Config {
    ///         durable_name: Some("pull".to_string()),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "server_2_10")]
    pub async fn update_consumer<C: IntoConsumerConfig + FromConsumer>(
        &self,
        config: C,
    ) -> Result<Consumer<C>, ConsumerUpdateError> {
        self.context
            .update_consumer_on_stream(config, self.info.config.name.clone())
            .await
    }

    /// Create consumer, but only if it does not exist or the existing config is exactly
    /// the same.
    /// This method will fail if consumer is already present with different config.
    /// returns the info from the server about created [Consumer].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// let info = stream
    ///     .create_consumer_strict(consumer::pull::Config {
    ///         durable_name: Some("pull".to_string()),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "server_2_10")]
    pub async fn create_consumer_strict<C: IntoConsumerConfig + FromConsumer>(
        &self,
        config: C,
    ) -> Result<Consumer<C>, ConsumerCreateStrictError> {
        self.context
            .create_consumer_strict_on_stream(config, self.info.config.name.clone())
            .await
    }

    /// Retrieve [Info] about [Consumer] from the server.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// let info = stream.consumer_info("pull").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn consumer_info<T: AsRef<str>>(
        &self,
        name: T,
    ) -> Result<consumer::Info, crate::Error> {
        let name = name.as_ref();

        let subject = format!("CONSUMER.INFO.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok(info) => Ok(info),
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!("nats: error while getting consumer info: {}", error),
            ))),
        }
    }

    /// Get [Consumer] from the the server. [Consumer] iterators can be used to retrieve
    /// [Messages][crate::jetstream::Message] for a given [Consumer].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// let consumer: consumer::PullConsumer = stream.get_consumer("pull").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_consumer<T: FromConsumer + IntoConsumerConfig>(
        &self,
        name: &str,
    ) -> Result<Consumer<T>, crate::Error> {
        let info = self.consumer_info(name).await?;

        Ok(Consumer::new(
            T::try_from_consumer_config(info.config.clone())?,
            info,
            self.context.clone(),
        ))
    }

    /// Create a [Consumer] with the given configuration if it is not present on the server. Returns a handle to the [Consumer].
    ///
    /// Note: This does not validate if the [Consumer] on the server is compatible with the configuration passed in except Push/Pull compatibility.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// let consumer = stream
    ///     .get_or_create_consumer(
    ///         "pull",
    ///         consumer::pull::Config {
    ///             durable_name: Some("pull".to_string()),
    ///             ..Default::default()
    ///         },
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_or_create_consumer<T: FromConsumer + IntoConsumerConfig>(
        &self,
        name: &str,
        config: T,
    ) -> Result<Consumer<T>, ConsumerError> {
        let subject = format!("CONSUMER.INFO.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Err { error } if error.code() == 404 => self.create_consumer(config).await,
            Response::Err { error } => Err(error.into()),
            Response::Ok::<consumer::Info>(info) => Ok(Consumer::new(
                T::try_from_consumer_config(info.config.clone()).map_err(|err| {
                    ConsumerError::with_source(ConsumerErrorKind::InvalidConsumerType, err)
                })?,
                info,
                self.context.clone(),
            )),
        }
    }

    /// Delete a [Consumer] from the server.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::consumer;
    /// use futures::StreamExt;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// jetstream
    ///     .get_stream("events")
    ///     .await?
    ///     .delete_consumer("pull")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_consumer(&self, name: &str) -> Result<DeleteStatus, ConsumerError> {
        let subject = format!("CONSUMER.DELETE.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok(delete_status) => Ok(delete_status),
            Response::Err { error } => Err(error.into()),
        }
    }

    /// Lists names of all consumers for current stream.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::TryStreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let stream = jetstream.get_stream("stream").await?;
    /// let mut names = stream.consumer_names();
    /// while let Some(consumer) = names.try_next().await? {
    ///     println!("consumer: {stream:?}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn consumer_names(&self) -> ConsumerNames {
        ConsumerNames {
            context: self.context.clone(),
            stream: self.info.config.name.clone(),
            offset: 0,
            page_request: None,
            consumers: Vec::new(),
            done: false,
        }
    }

    /// Lists all consumers info for current stream.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::TryStreamExt;
    /// let client = async_nats::connect("demo.nats.io:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    /// let stream = jetstream.get_stream("stream").await?;
    /// let mut consumers = stream.consumers();
    /// while let Some(consumer) = consumers.try_next().await? {
    ///     println!("consumer: {consumer:?}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn consumers(&self) -> Consumers {
        Consumers {
            context: self.context.clone(),
            stream: self.info.config.name.clone(),
            offset: 0,
            page_request: None,
            consumers: Vec::new(),
            done: false,
        }
    }
}

/// `StreamConfig` determines the properties for a stream.
/// There are sensible defaults for most. If no subjects are
/// given the name will be used as the only subject.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Config {
    /// A name for the Stream. Must not have spaces, tabs or period `.` characters
    pub name: String,
    /// How large the Stream may become in total bytes before the configured discard policy kicks in
    pub max_bytes: i64,
    /// How large the Stream may become in total messages before the configured discard policy kicks in
    #[serde(rename = "max_msgs")]
    pub max_messages: i64,
    /// Maximum amount of messages to keep per subject
    #[serde(rename = "max_msgs_per_subject")]
    pub max_messages_per_subject: i64,
    /// When a Stream has reached its configured `max_bytes` or `max_msgs`, this policy kicks in.
    /// `DiscardPolicy::New` refuses new messages or `DiscardPolicy::Old` (default) deletes old messages to make space
    pub discard: DiscardPolicy,
    /// Prevents a message from being added to a stream if the max_msgs_per_subject limit for the subject has been reached
    #[serde(default, skip_serializing_if = "is_default")]
    pub discard_new_per_subject: bool,
    /// Which NATS subjects to populate this stream with. Supports wildcards. Defaults to just the
    /// configured stream `name`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subjects: Vec<String>,
    /// How message retention is considered, `Limits` (default), `Interest` or `WorkQueue`
    pub retention: RetentionPolicy,
    /// How many Consumers can be defined for a given Stream, -1 for unlimited
    pub max_consumers: i32,
    /// Maximum age of any message in the stream, expressed in nanoseconds
    #[serde(with = "serde_nanos")]
    pub max_age: Duration,
    /// The largest message that will be accepted by the Stream
    #[serde(default, skip_serializing_if = "is_default", rename = "max_msg_size")]
    pub max_message_size: i32,
    /// The type of storage backend, `File` (default) and `Memory`
    pub storage: StorageType,
    /// How many replicas to keep for each message in a clustered JetStream, maximum 5
    pub num_replicas: usize,
    /// Disables acknowledging messages that are received by the Stream
    #[serde(default, skip_serializing_if = "is_default")]
    pub no_ack: bool,
    /// The window within which to track duplicate messages.
    #[serde(default, skip_serializing_if = "is_default", with = "serde_nanos")]
    pub duplicate_window: Duration,
    /// The owner of the template associated with this stream.
    #[serde(default, skip_serializing_if = "is_default")]
    pub template_owner: String,
    /// Indicates the stream is sealed and cannot be modified in any way
    #[serde(default, skip_serializing_if = "is_default")]
    pub sealed: bool,
    /// A short description of the purpose of this stream.
    #[serde(default, skip_serializing_if = "is_default")]
    pub description: Option<String>,
    #[serde(
        default,
        rename = "allow_rollup_hdrs",
        skip_serializing_if = "is_default"
    )]
    /// Indicates if rollups will be allowed or not.
    pub allow_rollup: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    /// Indicates deletes will be denied or not.
    pub deny_delete: bool,
    /// Indicates if purges will be denied or not.
    #[serde(default, skip_serializing_if = "is_default")]
    pub deny_purge: bool,

    /// Optional republish config.
    #[serde(default, skip_serializing_if = "is_default")]
    pub republish: Option<Republish>,

    /// Enables direct get, which would get messages from
    /// non-leader.
    #[serde(default, skip_serializing_if = "is_default")]
    pub allow_direct: bool,

    /// Enable direct access also for mirrors.
    #[serde(default, skip_serializing_if = "is_default")]
    pub mirror_direct: bool,

    /// Stream mirror configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror: Option<Source>,

    /// Sources configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<Source>>,

    #[cfg(feature = "server_2_10")]
    /// Additional stream metadata.
    #[serde(default, skip_serializing_if = "is_default")]
    pub metadata: HashMap<String, String>,

    #[cfg(feature = "server_2_10")]
    /// Allow applying a subject transform to incoming messages
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_transform: Option<SubjectTransform>,

    #[cfg(feature = "server_2_10")]
    /// Override compression config for this stream.
    /// Wrapping enum that has `None` type with [Option] is there
    /// because [Stream] can override global compression set to [Compression::S2]
    /// to [Compression::None], which is different from not overriding global config with anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    #[cfg(feature = "server_2_10")]
    /// Set limits on consumers that are created on this stream.
    #[serde(default, deserialize_with = "default_consumer_limits_as_none")]
    pub consumer_limits: Option<ConsumerLimits>,

    #[cfg(feature = "server_2_10")]
    /// Sets the first sequence for the stream.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "first_seq")]
    pub first_sequence: Option<u64>,

    /// Placement configuration for clusters and tags.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<Placement>,
}

impl From<&Config> for Config {
    fn from(sc: &Config) -> Config {
        sc.clone()
    }
}

impl From<&str> for Config {
    fn from(s: &str) -> Config {
        Config {
            name: s.to_string(),
            ..Default::default()
        }
    }
}

#[cfg(feature = "server_2_10")]
fn default_consumer_limits_as_none<'de, D>(
    deserializer: D,
) -> Result<Option<ConsumerLimits>, D::Error>
where
    D: Deserializer<'de>,
{
    let consumer_limits = ConsumerLimits::deserialize(deserializer)?;
    if consumer_limits == ConsumerLimits::default() {
        Ok(None)
    } else {
        Ok(Some(consumer_limits))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ConsumerLimits {
    /// Sets the maximum [crate::jetstream::consumer::Config::inactive_threshold] that can be set on the consumer.
    #[serde(default, with = "serde_nanos")]
    pub inactive_threshold: std::time::Duration,
    /// Sets the maximum [crate::jetstream::consumer::Config::max_ack_pending] that can be set on the consumer.
    #[serde(default)]
    pub max_ack_pending: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum Compression {
    #[serde(rename = "s2")]
    S2,
    #[serde(rename = "none")]
    None,
}

// SubjectTransform is for applying a subject transform (to matching messages) when a new message is received
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct SubjectTransform {
    #[serde(rename = "src")]
    pub source: String,

    #[serde(rename = "dest")]
    pub destination: String,
}

// Republish is for republishing messages once committed to a stream.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Republish {
    /// Subject that should be republished.
    #[serde(rename = "src")]
    pub source: String,
    /// Subject where messages will be republished.
    #[serde(rename = "dest")]
    pub destination: String,
    /// If true, only headers should be republished.
    #[serde(default)]
    pub headers_only: bool,
}

/// Placement describes on which cluster or tags the stream should be placed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Placement {
    // Cluster where the stream should be placed.
    #[serde(default, skip_serializing_if = "is_default")]
    pub cluster: Option<String>,
    // Matching tags for stream placement.
    #[serde(default, skip_serializing_if = "is_default")]
    pub tags: Vec<String>,
}

/// `DiscardPolicy` determines how we proceed when limits of messages or bytes are hit. The default, `Old` will
/// remove older messages. `New` will fail to store the new message.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DiscardPolicy {
    /// will remove older messages when limits are hit.
    #[default]
    #[serde(rename = "old")]
    Old = 0,
    /// will error on a StoreMsg call when limits are hit
    #[serde(rename = "new")]
    New = 1,
}

/// `RetentionPolicy` determines how messages in a set are retained.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetentionPolicy {
    /// `Limits` (default) means that messages are retained until any given limit is reached.
    /// This could be one of messages, bytes, or age.
    #[default]
    #[serde(rename = "limits")]
    Limits = 0,
    /// `Interest` specifies that when all known observables have acknowledged a message it can be removed.
    #[serde(rename = "interest")]
    Interest = 1,
    /// `WorkQueue` specifies that when the first worker or subscriber acknowledges the message it can be removed.
    #[serde(rename = "workqueue")]
    WorkQueue = 2,
}

/// determines how messages are stored for retention.
#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageType {
    /// Stream data is kept in files. This is the default.
    #[default]
    #[serde(rename = "file")]
    File = 0,
    /// Stream data is kept only in memory.
    #[serde(rename = "memory")]
    Memory = 1,
}

/// Shows config and current state for this stream.
#[derive(Debug, Deserialize, Clone)]
pub struct Info {
    /// The configuration associated with this stream.
    pub config: Config,
    /// The time that this stream was created.
    #[serde(with = "rfc3339")]
    pub created: time::OffsetDateTime,
    /// Various metrics associated with this stream.
    pub state: State,
    /// Information about leader and replicas.
    pub cluster: Option<ClusterInfo>,
    /// Information about mirror config if present.
    #[serde(default)]
    pub mirror: Option<SourceInfo>,
    /// Information about sources configs if present.
    #[serde(default)]
    pub sources: Vec<SourceInfo>,
}

#[derive(Deserialize)]
pub struct DeleteStatus {
    pub success: bool,
}

/// information about the given stream.
#[derive(Debug, Deserialize, Clone, Copy)]
pub struct State {
    /// The number of messages contained in this stream
    pub messages: u64,
    /// The number of bytes of all messages contained in this stream
    pub bytes: u64,
    /// The lowest sequence number still present in this stream
    #[serde(rename = "first_seq")]
    pub first_sequence: u64,
    /// The time associated with the oldest message still present in this stream
    #[serde(with = "rfc3339", rename = "first_ts")]
    pub first_timestamp: time::OffsetDateTime,
    /// The last sequence number assigned to a message in this stream
    #[serde(rename = "last_seq")]
    pub last_sequence: u64,
    /// The time that the last message was received by this stream
    #[serde(with = "rfc3339", rename = "last_ts")]
    pub last_timestamp: time::OffsetDateTime,
    /// The number of consumers configured to consume this stream
    pub consumer_count: usize,
}

/// A raw stream message in the representation it is stored.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RawMessage {
    /// Subject of the message.
    #[serde(rename = "subject")]
    pub subject: String,

    /// Sequence of the message.
    #[serde(rename = "seq")]
    pub sequence: u64,

    /// Raw payload of the message as a base64 encoded string.
    #[serde(default, rename = "data")]
    pub payload: String,

    /// Raw header string, if any.
    #[serde(default, rename = "hdrs")]
    pub headers: Option<String>,

    /// The time the message was published.
    #[serde(rename = "time", with = "rfc3339")]
    pub time: time::OffsetDateTime,
}

impl TryFrom<RawMessage> for crate::Message {
    type Error = crate::Error;

    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        let decoded_payload = STANDARD
            .decode(value.payload)
            .map_err(|err| Box::new(std::io::Error::new(ErrorKind::Other, err)))?;
        let decoded_headers = value
            .headers
            .map(|header| STANDARD.decode(header))
            .map_or(Ok(None), |v| v.map(Some))?;

        let length = decoded_headers
            .as_ref()
            .map_or_else(|| 0, |headers| headers.len())
            + decoded_payload.len()
            + value.subject.len();

        let (headers, status, description) =
            decoded_headers.map_or_else(|| Ok((None, None, None)), |h| parse_headers(&h))?;

        Ok(crate::Message {
            subject: value.subject.into(),
            reply: None,
            payload: decoded_payload.into(),
            headers,
            status,
            description,
            length,
        })
    }
}

fn is_continuation(c: char) -> bool {
    c == ' ' || c == '\t'
}
const HEADER_LINE: &str = "NATS/1.0";

#[allow(clippy::type_complexity)]
fn parse_headers(
    buf: &[u8],
) -> Result<(Option<HeaderMap>, Option<StatusCode>, Option<String>), crate::Error> {
    let mut headers = HeaderMap::new();
    let mut maybe_status: Option<StatusCode> = None;
    let mut maybe_description: Option<String> = None;
    let mut lines = if let Ok(line) = std::str::from_utf8(buf) {
        line.lines().peekable()
    } else {
        return Err(Box::new(std::io::Error::new(
            ErrorKind::Other,
            "invalid header",
        )));
    };

    if let Some(line) = lines.next() {
        let line = line
            .strip_prefix(HEADER_LINE)
            .ok_or_else(|| {
                Box::new(std::io::Error::new(
                    ErrorKind::Other,
                    "version line does not start with NATS/1.0",
                ))
            })?
            .trim();

        match line.split_once(' ') {
            Some((status, description)) => {
                if !status.is_empty() {
                    maybe_status = Some(status.parse()?);
                }

                if !description.is_empty() {
                    maybe_description = Some(description.trim().to_string());
                }
            }
            None => {
                if !line.is_empty() {
                    maybe_status = Some(line.parse()?);
                }
            }
        }
    } else {
        return Err(Box::new(std::io::Error::new(
            ErrorKind::Other,
            "expected header information not found",
        )));
    };

    while let Some(line) = lines.next() {
        if line.is_empty() {
            continue;
        }

        if let Some((k, v)) = line.split_once(':').to_owned() {
            let mut s = String::from(v.trim());
            while let Some(v) = lines.next_if(|s| s.starts_with(is_continuation)).to_owned() {
                s.push(' ');
                s.push_str(v.trim());
            }

            headers.insert(
                HeaderName::from_str(k)?,
                HeaderValue::from_str(&s)
                    .map_err(|err| Box::new(io::Error::new(ErrorKind::Other, err)))?,
            );
        } else {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "malformed header line",
            )));
        }
    }

    if headers.is_empty() {
        Ok((None, maybe_status, maybe_description))
    } else {
        Ok((Some(headers), maybe_status, maybe_description))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GetRawMessage {
    pub(crate) message: RawMessage,
}

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}
/// Information about the stream's, consumer's associated `JetStream` cluster
#[derive(Debug, Default, Deserialize, Clone, PartialEq, Eq)]
pub struct ClusterInfo {
    /// The cluster name.
    pub name: Option<String>,
    /// The server name of the RAFT leader.
    pub leader: Option<String>,
    /// The members of the RAFT cluster.
    #[serde(default)]
    pub replicas: Vec<PeerInfo>,
}

/// The members of the RAFT cluster
#[derive(Debug, Default, Deserialize, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    /// The server name of the peer.
    pub name: String,
    /// Indicates if the server is up to date and synchronized.
    pub current: bool,
    /// Nanoseconds since this peer was last seen.
    #[serde(with = "serde_nanos")]
    pub active: Duration,
    /// Indicates the node is considered offline by the group.
    #[serde(default)]
    pub offline: bool,
    /// How many uncommitted operations this peer is behind the leader.
    pub lag: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceInfo {
    /// Source name.
    pub name: String,
    /// Number of messages this source is lagging behind.
    pub lag: u64,
    /// Last time the source was seen active.
    #[serde(deserialize_with = "negative_duration_as_none")]
    pub active: Option<std::time::Duration>,
    /// Filtering for the source.
    #[serde(default)]
    pub filter_subject: Option<String>,
    /// Source destination subject.
    #[serde(default)]
    pub subject_transform_dest: Option<String>,
    /// List of transforms.
    #[serde(default)]
    pub subject_transforms: Vec<SubjectTransform>,
}

fn negative_duration_as_none<'de, D>(
    deserializer: D,
) -> Result<Option<std::time::Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let n = i64::deserialize(deserializer)?;
    if n.is_negative() {
        Ok(None)
    } else {
        Ok(Some(std::time::Duration::from_nanos(n as u64)))
    }
}

/// The response generated by trying to purge a stream.
#[derive(Debug, Deserialize, Clone, Copy)]
pub struct PurgeResponse {
    /// Whether the purge request was successful.
    pub success: bool,
    /// The number of purged messages in a stream.
    pub purged: u64,
}
/// The payload used to generate a purge request.
#[derive(Default, Debug, Serialize, Clone)]
pub struct PurgeRequest {
    /// Purge up to but not including sequence.
    #[serde(default, rename = "seq", skip_serializing_if = "is_default")]
    pub sequence: Option<u64>,

    /// Subject to match against messages for the purge command.
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter: Option<String>,

    /// Number of messages to keep.
    #[serde(default, skip_serializing_if = "is_default")]
    pub keep: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct Source {
    /// Name of the stream source.
    pub name: String,
    /// Optional source start sequence.
    #[serde(default, rename = "opt_start_seq", skip_serializing_if = "is_default")]
    pub start_sequence: Option<u64>,
    #[serde(
        default,
        rename = "opt_start_time",
        skip_serializing_if = "is_default",
        with = "rfc3339::option"
    )]
    /// Optional source start time.
    pub start_time: Option<OffsetDateTime>,
    /// Optional additional filter subject.
    #[serde(default, skip_serializing_if = "is_default")]
    pub filter_subject: Option<String>,
    /// Optional config for sourcing streams from another prefix, used for cross-account.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external: Option<External>,
    /// Optional config to set a domain, if source is residing in different one.
    #[serde(default, skip_serializing_if = "is_default")]
    pub domain: Option<String>,
    /// Subject transforms for Stream.
    #[cfg(feature = "server_2_10")]
    #[serde(default, skip_serializing_if = "is_default")]
    pub subject_transforms: Vec<SubjectTransform>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct External {
    /// Api prefix of external source.
    #[serde(rename = "api")]
    pub api_prefix: String,
    /// Optional configuration of delivery prefix.
    #[serde(rename = "deliver", skip_serializing_if = "is_default")]
    pub delivery_prefix: Option<String>,
}

use std::marker::PhantomData;

#[derive(Debug, Default)]
pub struct Yes;
#[derive(Debug, Default)]
pub struct No;

pub trait ToAssign: Debug {}

impl ToAssign for Yes {}
impl ToAssign for No {}

#[derive(Debug)]
pub struct Purge<'a, SEQUENCE, KEEP>
where
    SEQUENCE: ToAssign,
    KEEP: ToAssign,
{
    stream: &'a Stream,
    inner: PurgeRequest,
    sequence_set: PhantomData<SEQUENCE>,
    keep_set: PhantomData<KEEP>,
}

impl<'a, SEQUENCE, KEEP> Purge<'a, SEQUENCE, KEEP>
where
    SEQUENCE: ToAssign,
    KEEP: ToAssign,
{
    /// Adds subject filter to [PurgeRequest]
    pub fn filter<T: Into<String>>(mut self, filter: T) -> Purge<'a, SEQUENCE, KEEP> {
        self.inner.filter = Some(filter.into());
        self
    }
}

impl<'a> Purge<'a, No, No> {
    pub(crate) fn build(stream: &'a Stream) -> Purge<'a, No, No> {
        Purge {
            stream,
            inner: Default::default(),
            sequence_set: PhantomData {},
            keep_set: PhantomData {},
        }
    }
}

impl<'a, KEEP> Purge<'a, No, KEEP>
where
    KEEP: ToAssign,
{
    /// Creates a new [PurgeRequest].
    /// `keep` and `sequence` are exclusive, enforced compile time by generics.
    pub fn keep(self, keep: u64) -> Purge<'a, No, Yes> {
        Purge {
            stream: self.stream,
            sequence_set: PhantomData {},
            keep_set: PhantomData {},
            inner: PurgeRequest {
                keep: Some(keep),
                ..self.inner
            },
        }
    }
}
impl<'a, SEQUENCE> Purge<'a, SEQUENCE, No>
where
    SEQUENCE: ToAssign,
{
    /// Creates a new [PurgeRequest].
    /// `keep` and `sequence` are exclusive, enforces compile time by generics.
    pub fn sequence(self, sequence: u64) -> Purge<'a, Yes, No> {
        Purge {
            stream: self.stream,
            sequence_set: PhantomData {},
            keep_set: PhantomData {},
            inner: PurgeRequest {
                sequence: Some(sequence),
                ..self.inner
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PurgeErrorKind {
    Request,
    TimedOut,
    JetStream(super::errors::Error),
}

impl Display for PurgeErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request => write!(f, "request failed"),
            Self::TimedOut => write!(f, "timed out"),
            Self::JetStream(err) => write!(f, "JetStream error: {}", err),
        }
    }
}

pub type PurgeError = Error<PurgeErrorKind>;

impl<'a, S, K> IntoFuture for Purge<'a, S, K>
where
    S: ToAssign + std::marker::Send,
    K: ToAssign + std::marker::Send,
{
    type Output = Result<PurgeResponse, PurgeError>;

    type IntoFuture = BoxFuture<'a, Result<PurgeResponse, PurgeError>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(std::future::IntoFuture::into_future(async move {
            let request_subject = format!("STREAM.PURGE.{}", self.stream.info.config.name);
            let response: Response<PurgeResponse> = self
                .stream
                .context
                .request(request_subject, &self.inner)
                .map_err(|err| match err.kind() {
                    RequestErrorKind::TimedOut => PurgeError::new(PurgeErrorKind::TimedOut),
                    _ => PurgeError::with_source(PurgeErrorKind::Request, err),
                })
                .await?;

            match response {
                Response::Err { error } => Err(PurgeError::new(PurgeErrorKind::JetStream(error))),
                Response::Ok(response) => Ok(response),
            }
        }))
    }
}

#[derive(Deserialize, Debug)]
struct ConsumerPage {
    total: usize,
    consumers: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
struct ConsumerInfoPage {
    total: usize,
    consumers: Option<Vec<super::consumer::Info>>,
}

type ConsumerNamesErrorKind = StreamsErrorKind;
type ConsumerNamesError = StreamsError;
type PageRequest = BoxFuture<'static, Result<ConsumerPage, RequestError>>;

pub struct ConsumerNames {
    context: Context,
    stream: String,
    offset: usize,
    page_request: Option<PageRequest>,
    consumers: Vec<String>,
    done: bool,
}

impl futures::Stream for ConsumerNames {
    type Item = Result<String, ConsumerNamesError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.page_request.as_mut() {
            Some(page) => match page.try_poll_unpin(cx) {
                std::task::Poll::Ready(page) => {
                    self.page_request = None;
                    let page = page.map_err(|err| {
                        ConsumerNamesError::with_source(ConsumerNamesErrorKind::Other, err)
                    })?;

                    if let Some(consumers) = page.consumers {
                        self.offset += consumers.len();
                        self.consumers = consumers;
                        if self.offset >= page.total {
                            self.done = true;
                        }
                        match self.consumers.pop() {
                            Some(stream) => Poll::Ready(Some(Ok(stream))),
                            None => Poll::Ready(None),
                        }
                    } else {
                        Poll::Ready(None)
                    }
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            None => {
                if let Some(stream) = self.consumers.pop() {
                    Poll::Ready(Some(Ok(stream)))
                } else {
                    if self.done {
                        return Poll::Ready(None);
                    }
                    let context = self.context.clone();
                    let offset = self.offset;
                    let stream = self.stream.clone();
                    self.page_request = Some(Box::pin(async move {
                        match context
                            .request(
                                format!("CONSUMER.NAMES.{stream}"),
                                &json!({
                                    "offset": offset,
                                }),
                            )
                            .await?
                        {
                            Response::Err { error } => Err(RequestError::with_source(
                                super::context::RequestErrorKind::Other,
                                error,
                            )),
                            Response::Ok(page) => Ok(page),
                        }
                    }));
                    self.poll_next(cx)
                }
            }
        }
    }
}

pub type ConsumersErrorKind = StreamsErrorKind;
pub type ConsumersError = StreamsError;
type PageInfoRequest = BoxFuture<'static, Result<ConsumerInfoPage, RequestError>>;

pub struct Consumers {
    context: Context,
    stream: String,
    offset: usize,
    page_request: Option<PageInfoRequest>,
    consumers: Vec<super::consumer::Info>,
    done: bool,
}

impl futures::Stream for Consumers {
    type Item = Result<super::consumer::Info, ConsumersError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.page_request.as_mut() {
            Some(page) => match page.try_poll_unpin(cx) {
                std::task::Poll::Ready(page) => {
                    self.page_request = None;
                    let page = page.map_err(|err| {
                        ConsumersError::with_source(ConsumersErrorKind::Other, err)
                    })?;
                    if let Some(consumers) = page.consumers {
                        self.offset += consumers.len();
                        self.consumers = consumers;
                        if self.offset >= page.total {
                            self.done = true;
                        }
                        match self.consumers.pop() {
                            Some(consumer) => Poll::Ready(Some(Ok(consumer))),
                            None => Poll::Ready(None),
                        }
                    } else {
                        Poll::Ready(None)
                    }
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            None => {
                if let Some(stream) = self.consumers.pop() {
                    Poll::Ready(Some(Ok(stream)))
                } else {
                    if self.done {
                        return Poll::Ready(None);
                    }
                    let context = self.context.clone();
                    let offset = self.offset;
                    let stream = self.stream.clone();
                    self.page_request = Some(Box::pin(async move {
                        match context
                            .request(
                                format!("CONSUMER.LIST.{stream}"),
                                &json!({
                                    "offset": offset,
                                }),
                            )
                            .await?
                        {
                            Response::Err { error } => Err(RequestError::with_source(
                                super::context::RequestErrorKind::Other,
                                error,
                            )),
                            Response::Ok(page) => Ok(page),
                        }
                    }));
                    self.poll_next(cx)
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum LastRawMessageErrorKind {
    NoMessageFound,
    JetStream(super::errors::Error),
    Other,
}

impl Display for LastRawMessageErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoMessageFound => write!(f, "no message found"),
            Self::Other => write!(f, "failed to get last raw message"),
            Self::JetStream(err) => write!(f, "JetStream error: {}", err),
        }
    }
}

pub type LastRawMessageError = Error<LastRawMessageErrorKind>;

#[derive(Clone, Debug, PartialEq)]
pub enum ConsumerErrorKind {
    //TODO: get last should have timeout, which should be mapped here.
    TimedOut,
    Request,
    InvalidConsumerType,
    InvalidName,
    JetStream(super::errors::Error),
    Other,
}

impl Display for ConsumerErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out"),
            Self::Request => write!(f, "request failed"),
            Self::JetStream(err) => write!(f, "JetStream error: {}", err),
            Self::Other => write!(f, "consumer error"),
            Self::InvalidConsumerType => write!(f, "invalid consumer type"),
            Self::InvalidName => write!(f, "invalid consumer name"),
        }
    }
}

pub type ConsumerError = Error<ConsumerErrorKind>;

#[derive(Clone, Debug, PartialEq)]
pub enum ConsumerCreateStrictErrorKind {
    //TODO: get last should have timeout, which should be mapped here.
    TimedOut,
    Request,
    InvalidConsumerType,
    InvalidName,
    AlreadyExists,
    JetStream(super::errors::Error),
    Other,
}

impl Display for ConsumerCreateStrictErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out"),
            Self::Request => write!(f, "request failed"),
            Self::JetStream(err) => write!(f, "JetStream error: {}", err),
            Self::Other => write!(f, "consumer error"),
            Self::InvalidConsumerType => write!(f, "invalid consumer type"),
            Self::InvalidName => write!(f, "invalid consumer name"),
            Self::AlreadyExists => write!(f, "consumer already exists"),
        }
    }
}

pub type ConsumerCreateStrictError = Error<ConsumerCreateStrictErrorKind>;

#[derive(Clone, Debug, PartialEq)]
pub enum ConsumerUpdateErrorKind {
    //TODO: get last should have timeout, which should be mapped here.
    TimedOut,
    Request,
    InvalidConsumerType,
    InvalidName,
    DoesNotExist,
    JetStream(super::errors::Error),
    Other,
}

impl Display for ConsumerUpdateErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out"),
            Self::Request => write!(f, "request failed"),
            Self::JetStream(err) => write!(f, "JetStream error: {}", err),
            Self::Other => write!(f, "consumer error"),
            Self::InvalidConsumerType => write!(f, "invalid consumer type"),
            Self::InvalidName => write!(f, "invalid consumer name"),
            Self::DoesNotExist => write!(f, "consumer does not exist"),
        }
    }
}

pub type ConsumerUpdateError = Error<ConsumerUpdateErrorKind>;

impl From<super::errors::Error> for ConsumerError {
    fn from(err: super::errors::Error) -> Self {
        ConsumerError::new(ConsumerErrorKind::JetStream(err))
    }
}
impl From<super::errors::Error> for ConsumerCreateStrictError {
    fn from(err: super::errors::Error) -> Self {
        if err.error_code() == super::errors::ErrorCode::CONSUMER_ALREADY_EXISTS {
            ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::AlreadyExists)
        } else {
            ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::JetStream(err))
        }
    }
}
impl From<super::errors::Error> for ConsumerUpdateError {
    fn from(err: super::errors::Error) -> Self {
        if err.error_code() == super::errors::ErrorCode::CONSUMER_DOES_NOT_EXIST {
            ConsumerUpdateError::new(ConsumerUpdateErrorKind::DoesNotExist)
        } else {
            ConsumerUpdateError::new(ConsumerUpdateErrorKind::JetStream(err))
        }
    }
}
impl From<ConsumerError> for ConsumerUpdateError {
    fn from(err: ConsumerError) -> Self {
        match err.kind() {
            ConsumerErrorKind::JetStream(err) => {
                if err.error_code() == super::errors::ErrorCode::CONSUMER_DOES_NOT_EXIST {
                    ConsumerUpdateError::new(ConsumerUpdateErrorKind::DoesNotExist)
                } else {
                    ConsumerUpdateError::new(ConsumerUpdateErrorKind::JetStream(err))
                }
            }
            ConsumerErrorKind::Request => {
                ConsumerUpdateError::new(ConsumerUpdateErrorKind::Request)
            }
            ConsumerErrorKind::TimedOut => {
                ConsumerUpdateError::new(ConsumerUpdateErrorKind::TimedOut)
            }
            ConsumerErrorKind::InvalidConsumerType => {
                ConsumerUpdateError::new(ConsumerUpdateErrorKind::InvalidConsumerType)
            }
            ConsumerErrorKind::InvalidName => {
                ConsumerUpdateError::new(ConsumerUpdateErrorKind::InvalidName)
            }
            ConsumerErrorKind::Other => ConsumerUpdateError::new(ConsumerUpdateErrorKind::Other),
        }
    }
}

impl From<ConsumerError> for ConsumerCreateStrictError {
    fn from(err: ConsumerError) -> Self {
        match err.kind() {
            ConsumerErrorKind::JetStream(err) => {
                if err.error_code() == super::errors::ErrorCode::CONSUMER_ALREADY_EXISTS {
                    ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::AlreadyExists)
                } else {
                    ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::JetStream(err))
                }
            }
            ConsumerErrorKind::Request => {
                ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::Request)
            }
            ConsumerErrorKind::TimedOut => {
                ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::TimedOut)
            }
            ConsumerErrorKind::InvalidConsumerType => {
                ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::InvalidConsumerType)
            }
            ConsumerErrorKind::InvalidName => {
                ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::InvalidName)
            }
            ConsumerErrorKind::Other => {
                ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::Other)
            }
        }
    }
}

impl From<super::context::RequestError> for ConsumerError {
    fn from(err: super::context::RequestError) -> Self {
        match err.kind() {
            RequestErrorKind::TimedOut => ConsumerError::new(ConsumerErrorKind::TimedOut),
            _ => ConsumerError::with_source(ConsumerErrorKind::Request, err),
        }
    }
}
impl From<super::context::RequestError> for ConsumerUpdateError {
    fn from(err: super::context::RequestError) -> Self {
        match err.kind() {
            RequestErrorKind::TimedOut => {
                ConsumerUpdateError::new(ConsumerUpdateErrorKind::TimedOut)
            }
            _ => ConsumerUpdateError::with_source(ConsumerUpdateErrorKind::Request, err),
        }
    }
}
impl From<super::context::RequestError> for ConsumerCreateStrictError {
    fn from(err: super::context::RequestError) -> Self {
        match err.kind() {
            RequestErrorKind::TimedOut => {
                ConsumerCreateStrictError::new(ConsumerCreateStrictErrorKind::TimedOut)
            }
            _ => {
                ConsumerCreateStrictError::with_source(ConsumerCreateStrictErrorKind::Request, err)
            }
        }
    }
}
