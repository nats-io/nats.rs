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
//! Manage operations on a [Stream], create/delete/update [Consumer][crate::jetstream::consumer::Consumer].

use std::{
    fmt::Debug,
    io::{self, ErrorKind},
    str::FromStr,
    time::Duration,
};

use crate::{header::HeaderName, HeaderMap, HeaderValue};
use crate::{Error, StatusCode};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::{serde::rfc3339, OffsetDateTime};

use super::{
    consumer::{self, Consumer, FromConsumer, IntoConsumerConfig},
    response::Response,
    Context, Message,
};

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
    /// let mut stream = jetstream
    ///     .get_stream("events").await?;
    ///
    /// let info = stream.info().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn info(&mut self) -> Result<&Info, Error> {
        let subject = format!("STREAM.INFO.{}", self.info.config.name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok::<Info>(info) => {
                self.info = info;
                Ok(&self.info)
            }
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting stream info: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
        }
    }

    /// Returns cached [Info] for the [Stream].
    /// Cache is either from initial creation/retrieval of the [Stream] or last call to
    /// [Stream::Info].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream
    ///     .get_stream("events").await?;
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
    /// This is different from [get_raw_message], as it can fetch [Message]
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
    /// let stream = jetstream.create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     subjects: vec!["events.>".to_string()],
    ///     allow_direct: true,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// jetstream.publish("events.data".into(), "data".into()).await?;
    /// let pub_ack = jetstream.publish("events.data".into(), "data".into()).await?;
    ///
    /// let message =  stream
    ///     .direct_get_next_for_subject("events.data", Some(pub_ack.await?.sequence)).await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn direct_get_next_for_subject<T: AsRef<str>>(
        &self,
        subject: T,
        sequence: Option<u64>,
    ) -> Result<Message, Error> {
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
                return Err(Box::from(std::io::Error::new(
                    ErrorKind::Other,
                    format!("{} {}", status, description),
                )));
            }
        }
        Ok(response)
    }

    /// Gets first message from [Stream].
    ///
    /// Requires a [Stream] with `allow_direct` set to `true`.
    /// This is different from [get_raw_message], as it can fetch [Message]
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
    /// let stream = jetstream.create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     subjects: vec!["events.>".to_string()],
    ///     allow_direct: true,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let pub_ack = jetstream.publish("events.data".into(), "data".into()).await?;
    ///
    /// let message =  stream.direct_get_first_for_subject("events.data").await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn direct_get_first_for_subject<T: AsRef<str>>(
        &self,
        subject: T,
    ) -> Result<Message, Error> {
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
                return Err(Box::from(std::io::Error::new(
                    ErrorKind::Other,
                    format!("{} {}", status, description),
                )));
            }
        }
        Ok(response)
    }

    /// Gets message from [Stream] with given `sequence id`.
    ///
    /// Requires a [Stream] with `allow_direct` set to `true`.
    /// This is different from [get_raw_message], as it can fetch [Message]
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
    /// let stream = jetstream.create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     subjects: vec!["events.>".to_string()],
    ///     allow_direct: true,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let pub_ack = jetstream.publish("events.data".into(), "data".into()).await?;
    ///
    /// let message =  stream.direct_get(pub_ack.await?.sequence).await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn direct_get(&self, sequence: u64) -> Result<Message, Error> {
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
                return Err(Box::from(std::io::Error::new(
                    ErrorKind::Other,
                    format!("{} {}", status, description),
                )));
            }
        }
        Ok(response)
    }

    /// Gets last message for a given `subject`.
    ///
    /// Requires a [Stream] with `allow_direct` set to `true`.
    /// This is different from [get_raw_message], as it can fetch [Message]
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
    /// let stream = jetstream.create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     subjects: vec!["events.>".to_string()],
    ///     allow_direct: true,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// jetstream.publish("events.data".into(), "data".into()).await?;
    ///
    /// let message =  stream.direct_get_last_for_subject("events.data").await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn direct_get_last_for_subject<T: AsRef<str>>(
        &self,
        subject: T,
    ) -> Result<Message, Error> {
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
                        return Err(Box::from(std::io::Error::new(
                            ErrorKind::NotFound,
                            "message not found in stream",
                        )))
                    }
                    // 408 is used in Direct Message for bad/empty payload.
                    StatusCode::TIMEOUT => {
                        return Err(Box::from(std::io::Error::new(
                            ErrorKind::Other,
                            "empty or invalid request",
                        )))
                    }
                    other => {
                        return Err(Box::from(std::io::Error::new(
                            ErrorKind::Other,
                            format!("{}: {}", other, description),
                        )))
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
    /// let stream = context.get_or_create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let publish_ack = context.publish("events".to_string(), "data".into()).await?;
    /// let raw_message = stream.get_raw_message(publish_ack.await?.sequence).await?;
    /// println!("Retrieved raw message {:?}", raw_message);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_raw_message(&self, sequence: u64) -> Result<RawMessage, Error> {
        let subject = format!("STREAM.MSG.GET.{}", &self.info.config.name);
        let payload = json!({
            "seq": sequence,
        });

        let response: Response<GetRawMessage> = self.context.request(subject, &payload).await?;
        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting message: {}, {}",
                    error.code, error.description
                ),
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
    /// let stream = context.get_or_create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let publish_ack = context.publish("events".to_string(), "data".into()).await?;
    /// let raw_message = stream.get_last_raw_message_by_subject("events").await?;
    /// println!("Retrieved raw message {:?}", raw_message);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_last_raw_message_by_subject(
        &self,
        stream_subject: &str,
    ) -> Result<RawMessage, Error> {
        let subject = format!("STREAM.MSG.GET.{}", &self.info.config.name);
        let payload = json!({
            "last_by_subj":  stream_subject,
        });

        let response: Response<GetRawMessage> = self.context.request(subject, &payload).await?;
        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(ErrorKind::Other, error))),
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
    /// let stream = context.get_or_create_stream(async_nats::jetstream::stream::Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    ///
    /// let publish_ack = context.publish("events".to_string(), "data".into()).await?;
    /// stream.delete_message(publish_ack.await?.sequence).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_message(&self, sequence: u64) -> Result<bool, Error> {
        let subject = format!("STREAM.MSG.DELETE.{}", &self.info.config.name);
        let payload = json!({
            "seq": sequence,
        });

        let response: Response<DeleteStatus> = self.context.request(subject, &payload).await?;

        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while deleting message: {}, {}",
                    error.code, error.status
                ),
            ))),
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
    pub async fn purge(&self) -> Result<PurgeResponse, Error> {
        let subject = format!("STREAM.PURGE.{}", self.info.config.name);

        let response: Response<PurgeResponse> = self.context.request(subject, &()).await?;
        match response {
            Response::Err { error } => Err(Box::new(io::Error::new(
                ErrorKind::Other,
                format!(
                    "error while purging stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(response) => Ok(response),
        }
    }

    /// Purge `Stream` messages for a matching subject.
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
    /// stream.purge_subject("data").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge_subject<T>(&self, subject: T) -> Result<PurgeResponse, Error>
    where
        T: Into<String>,
    {
        let request_subject = format!("STREAM.PURGE.{}", self.info.config.name);

        let response: Response<PurgeResponse> = self
            .context
            .request(
                request_subject,
                &PurgeRequest {
                    filter: Some(subject.into()),
                    ..Default::default()
                },
            )
            .await?;
        match response {
            Response::Err { error } => Err(Box::new(io::Error::new(
                ErrorKind::Other,
                format!(
                    "error while purging stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(response) => Ok(response),
        }
    }

    /// Create a new `Durable` or `Ephemeral` Consumer (if `durable_name` was not provided) and
    /// returns the info from the server about created [Consumer][Consumer]
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
    /// let info = stream.create_consumer(consumer::pull::Config {
    ///     durable_name: Some("pull".to_string()),
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_consumer<C: IntoConsumerConfig + FromConsumer>(
        &self,
        config: C,
    ) -> Result<Consumer<C>, Error> {
        let config = config.into_consumer_config();

        let subject = {
            if self.context.client.is_server_compatible(2, 9, 0) {
                let filter = if config.filter_subject.is_empty() {
                    "".to_string()
                } else {
                    format!(".{}", config.filter_subject)
                };
                config
                    .name
                    .as_ref()
                    .or(config.durable_name.as_ref())
                    .map(|name| {
                        format!(
                            "CONSUMER.CREATE.{}.{}{}",
                            self.info.config.name, name, filter
                        )
                    })
                    .unwrap_or_else(|| format!("CONSUMER.CREATE.{}", self.info.config.name))
            } else if config.name.is_some() {
                return Err(Box::new(std::io::Error::new(
                    ErrorKind::Other,
                    "can't use consumer name with server below version 2.9",
                )));
            } else if let Some(ref durable_name) = config.durable_name {
                format!(
                    "CONSUMER.DURABLE.CREATE.{}.{}",
                    self.info.config.name, durable_name
                )
            } else {
                format!("CONSUMER.CREATE.{}", self.info.config.name)
            }
        };

        match self
            .context
            .request(
                subject,
                &json!({"stream_name": self.info.config.name.clone(), "config": config}),
            )
            .await?
        {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while creating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok::<consumer::Info>(info) => Ok(Consumer::new(
                FromConsumer::try_from_consumer_config(info.clone().config)?,
                info,
                self.context.clone(),
            )),
        }
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
    pub async fn consumer_info<T: AsRef<str>>(&self, name: T) -> Result<consumer::Info, Error> {
        let name = name.as_ref();

        let subject = format!("CONSUMER.INFO.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok(info) => Ok(info),
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting consumer info: {}, {}, {}",
                    error.code, error.status, error.description
                ),
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
    ) -> Result<Consumer<T>, Error> {
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
    /// let consumer = stream.get_or_create_consumer("pull", consumer::pull::Config {
    ///     durable_name: Some("pull".to_string()),
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_or_create_consumer<T: FromConsumer + IntoConsumerConfig>(
        &self,
        name: &str,
        config: T,
    ) -> Result<Consumer<T>, Error> {
        let subject = format!("CONSUMER.INFO.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Err { error } if error.status == 404 => self.create_consumer(config).await,
            Response::Err { error } => Err(Box::new(io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting or creating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok::<consumer::Info>(info) => Ok(Consumer::new(
                T::try_from_consumer_config(info.config.clone())?,
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
    /// jetstream.get_stream("events").await?
    ///     .delete_consumer("pull").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_consumer(&self, name: &str) -> Result<DeleteStatus, Error> {
        let subject = format!("CONSUMER.DELETE.{}.{}", self.info.config.name, name);

        match self.context.request(subject, &json!({})).await? {
            Response::Ok(delete_status) => Ok(delete_status),
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while deleting consumer: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
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
    #[serde(default, skip_serializing_if = "is_default")]
    pub duplicate_window: i64,
    /// The owner of the template associated with this stream.
    #[serde(default, skip_serializing_if = "is_default")]
    pub template_owner: String,
    /// Indicates the stream is sealed and cannot be modified in any way
    #[serde(default, skip_serializing_if = "is_default")]
    pub sealed: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    /// A short description of the purpose of this stream.
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

/// `DiscardPolicy` determines how we proceed when limits of messages or bytes are hit. The default, `Old` will
/// remove older messages. `New` will fail to store the new message.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DiscardPolicy {
    /// will remove older messages when limits are hit.
    #[serde(rename = "old")]
    Old = 0,
    /// will error on a StoreMsg call when limits are hit
    #[serde(rename = "new")]
    New = 1,
}

impl Default for DiscardPolicy {
    fn default() -> DiscardPolicy {
        DiscardPolicy::Old
    }
}

/// `RetentionPolicy` determines how messages in a set are retained.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetentionPolicy {
    /// `Limits` (default) means that messages are retained until any given limit is reached.
    /// This could be one of messages, bytes, or age.
    #[serde(rename = "limits")]
    Limits = 0,
    /// `Interest` specifies that when all known observables have acknowledged a message it can be removed.
    #[serde(rename = "interest")]
    Interest = 1,
    /// `WorkQueue` specifies that when the first worker or subscriber acknowledges the message it can be removed.
    #[serde(rename = "workqueue")]
    WorkQueue = 2,
}

impl Default for RetentionPolicy {
    fn default() -> RetentionPolicy {
        RetentionPolicy::Limits
    }
}

/// determines how messages are stored for retention.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageType {
    /// Stream data is kept in files. This is the default.
    #[serde(rename = "file")]
    File = 0,
    /// Stream data is kept only in memory.
    #[serde(rename = "memory")]
    Memory = 1,
}

impl Default for StorageType {
    fn default() -> StorageType {
        StorageType::File
    }
}

/// Shows config and current state for this stream.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Info {
    /// The configuration associated with this stream
    pub config: Config,
    /// The time that this stream was created
    #[serde(with = "rfc3339")]
    pub created: time::OffsetDateTime,
    /// Various metrics associated with this stream
    pub state: State,

    ///information about leader and replicas
    #[serde(default)]
    pub cluster: Option<ClusterInfo>,
}

#[derive(Deserialize)]
pub struct DeleteStatus {
    pub success: bool,
}

/// information about the given stream.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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
    type Error = Error;

    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        let decoded_payload = base64::decode(value.payload)
            .map_err(|err| Box::new(std::io::Error::new(ErrorKind::Other, err)))?;
        let decoded_headers = value
            .headers
            .map(base64::decode)
            .map_or(Ok(None), |v| v.map(Some))?;

        let length = decoded_headers
            .as_ref()
            .map_or_else(|| 0, |headers| headers.len())
            + decoded_paylaod.len()
            + value.subject.len();

        let (headers, status, description) =
            decoded_headers.map_or_else(|| Ok((None, None, None)), |h| parse_headers(&h))?;

        Ok(crate::Message {
            subject: value.subject,
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
const HEADER_LINE_LEN: usize = HEADER_LINE.len();

#[allow(clippy::type_complexity)]
fn parse_headers(
    buf: &[u8],
) -> Result<(Option<HeaderMap>, Option<StatusCode>, Option<String>), Error> {
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
        if !line.starts_with(HEADER_LINE) {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "version lie does not start with NATS/1.0",
            )));
        }

        // TODO: return this as description to be consistent?
        if let Some(slice) = line.get(HEADER_LINE_LEN..).map(|s| s.trim()) {
            match slice.split_once(' ') {
                Some((status, description)) => {
                    if !status.is_empty() {
                        maybe_status = Some(status.trim().parse()?);
                    }

                    if !description.is_empty() {
                        maybe_description = Some(description.trim().to_string());
                    }
                }
                None => {
                    if !slice.is_empty() {
                        maybe_status = Some(slice.trim().parse()?);
                    }
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
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ClusterInfo {
    /// The cluster name.
    #[serde(default)]
    pub name: Option<String>,
    /// The server name of the RAFT leader.
    #[serde(default)]
    pub leader: Option<String>,
    /// The members of the RAFT cluster.
    #[serde(default)]
    pub replicas: Vec<PeerInfo>,
}

/// The members of the RAFT cluster
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
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

/// The response generated by trying to purge a stream.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct PurgeResponse {
    /// Whether the purge request was successful.
    pub success: bool,
    /// The number of purged messages in a stream.
    pub purged: u64,
}
/// The payload used to generate a purge request.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
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
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct External {
    /// API prefix of external source.
    #[serde(rename = "api")]
    pub api_prefix: String,
    /// Optional configuration of delivery prefix.
    #[serde(rename = "deliver", skip_serializing_if = "is_default")]
    pub delivery_prefix: Option<String>,
}
