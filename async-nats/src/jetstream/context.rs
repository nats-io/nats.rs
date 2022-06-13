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

use std::io::{self, ErrorKind};

use crate::jetstream::publish::PublishAck;
use crate::jetstream::response::Response;
use crate::{Client, Error};
use bytes::Bytes;
use http::HeaderMap;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{self, json};

use super::stream::{Config, DeleteStatus, Stream, StreamInfo};

/// A context which can perform jetstream scoped requests.
#[derive(Debug, Clone)]
pub struct Context {
    pub(crate) client: Client,
    pub(crate) prefix: String,
}

impl Context {
    pub(crate) fn new(client: Client) -> Context {
        Context {
            client,
            prefix: "$JS.API".to_string(),
        }
    }

    pub(crate) fn with_prefix<T: ToString>(client: Client, prefix: T) -> Context {
        Context {
            client,
            prefix: prefix.to_string(),
        }
    }

    pub(crate) fn with_domain<T: AsRef<str>>(client: Client, domain: T) -> Context {
        Context {
            client,
            prefix: format!("$JS.{}.API", domain.as_ref()),
        }
    }

    /// Publish a message to a given subject associated with a stream and returns an acknowledgment from
    /// the server that the message has been successfully delivered.
    ///
    /// If the stream does not exist, `no responders` error will be returned.
    ///
    /// # Examples:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let ack = jetstream.publish("events".to_string(), "data".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish(&self, subject: String, payload: Bytes) -> Result<PublishAck, Error> {
        let message = self.client.request(subject, payload).await?;
        let response = serde_json::from_slice(message.payload.as_ref())?;

        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while publishing message: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),

            Response::Ok(publish_ack) => Ok(publish_ack),
        }
    }

    /// Publish a message with headers to a given subject associated with a stream and returns an acknowledgment from
    /// the server that the message has been successfully delivered.
    ///
    /// If the stream does not exist, `no responders` error will be returned.
    ///
    /// # Examples:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let mut headers = async_nats::HeaderMap::new();
    /// headers.append("X-key", b"Value".as_ref().try_into()?);
    /// let ack = jetstream.publish_with_headers("events".to_string(), headers, "data".into()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_with_headers(
        &self,
        subject: String,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<PublishAck, Error> {
        let message = self
            .client
            .request_with_headers(subject, headers, payload)
            .await?;
        let response = serde_json::from_slice(message.payload.as_ref())?;

        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while publishing message: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),

            Response::Ok(publish_ack) => Ok(publish_ack),
        }
    }

    /// Create a JetStream [Stream] with given config and return a handle to it.
    /// That handle can be used to manage and use [Consumer][crate::jetstream::consumer::Consumer].
    ///
    /// # Examples:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::stream::Config;
    /// use async_nats::jetstream::stream::DiscardPolicy;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.create_stream(Config {
    ///     name: "events".to_string(),
    ///     max_messages: 100_000,
    ///     discard: DiscardPolicy::Old,
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_stream<S>(&self, stream_config: S) -> Result<Stream, Error>
    where
        Config: From<S>,
    {
        let config: Config = stream_config.into();
        if config.name.is_empty() {
            return Err(Box::new(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            )));
        }
        let subject = format!("STREAM.CREATE.{}", config.name);
        let response: Response<StreamInfo> = self.request(subject, &config).await?;

        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while creating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

    /// Checks for [Stream] existence on the server and returns handle to it.
    /// That handle can be used to manage and use [Consumer][crate::jetstream::consumer::Consumer].
    ///
    /// # Examples:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_stream("events").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_stream<T: AsRef<str>>(&self, stream: T) -> Result<Stream, Error> {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(Box::new(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            )));
        }

        let subject = format!("STREAM.INFO.{}", stream);
        let request: Response<StreamInfo> = self.request(subject, &()).await?;
        match request {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

    /// Create a stream with the given configuration on the server if it is not present. Returns a handle to the stream  on the server.
    ///
    /// Note: This does not validate if the Stream on the server is compatible with the configuration passed in.
    ///
    /// # Examples:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::stream::Config;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.get_or_create_stream(Config {
    ///     name: "events".to_string(),
    ///     max_messages: 10_000,
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_or_create_stream<S>(&self, stream_config: S) -> Result<Stream, Error>
    where
        S: Into<Config>,
    {
        let config: Config = stream_config.into();
        let subject = format!("STREAM.INFO.{}", config.name);

        let request: Response<StreamInfo> = self.request(subject, &()).await?;
        match request {
            Response::Err { error } if error.status == 404 => self.create_stream(&config).await,
            Response::Err { error } => Err(Box::new(io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting or creating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

    /// Deletes a [Stream] with a given name.
    ///
    /// # Examples:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::stream::Config;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.delete_stream("events").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_stream<T: AsRef<str>>(&self, stream: T) -> Result<DeleteStatus, Error> {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(Box::new(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            )));
        }
        let subject = format!("STREAM.DELETE.{}", stream);
        match self.request(subject, &json!({})).await? {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while deleting stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(delete_response) => Ok(delete_response),
        }
    }

    /// Updates a [Stream] with a given config. If specific field cannot be updated,
    /// error is returned.
    ///
    /// # Examples:
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::jetstream::stream::Config;
    /// use async_nats::jetstream::stream::DiscardPolicy;
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let stream = jetstream.update_stream(&Config {
    ///     name: "events".to_string(),
    ///     discard: DiscardPolicy::New,
    ///     max_messages: 50_000,
    ///     ..Default::default()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_stream(&self, config: &Config) -> Result<StreamInfo, Error> {
        let subject = format!("STREAM.UPDATE.{}", config.name);
        match self.request(subject, config).await? {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while updating stream: {}, {}, {}",
                    error.code, error.status, error.description
                ),
            ))),
            Response::Ok(info) => Ok(info),
        }
    }

    /// Send a request to the jetstream JSON API.
    ///
    /// This is a low level API used mostly internally, that should be used only in
    /// specific cases when this crate API on [Consumer][crate::jetstream::consumer::Consumer] or [Stream] does not provide needed functionality.
    ///
    /// # Examples:
    ///
    /// ```
    /// # use async_nats::jetstream::stream::StreamInfo;
    /// # use async_nats::jetstream::response::Response;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("localhost:4222").await?;
    /// let jetstream = async_nats::jetstream::new(client);
    ///
    /// let response: Response<StreamInfo> = jetstream
    /// .request("STREAM.INFO.events".to_string(), &()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request<T, V>(&self, subject: String, payload: &T) -> Result<Response<V>, Error>
    where
        T: ?Sized + Serialize,
        V: DeserializeOwned,
    {
        let request = serde_json::to_vec(&payload).map(Bytes::from)?;

        let message = self
            .client
            .request(format!("{}.{}", self.prefix, subject), request)
            .await?;
        let response = serde_json::from_slice(message.payload.as_ref())?;

        Ok(response)
    }
}
