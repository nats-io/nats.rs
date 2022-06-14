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
use crate::status::StatusCode;
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

    /// Publish a message
    pub async fn publish(&self, subject: String, payload: Bytes) -> Result<PublishAck, Error> {
        let message = self.client.request(subject, payload).await?;
        let response = serde_json::from_slice(message.payload.as_ref())?;

        match response {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while publishing message: {}, {}",
                    error.code, error.description
                ),
            ))),

            Response::Ok(publish_ack) => Ok(publish_ack),
        }
    }

    /// Publish a message with headers
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
                    "nats: error while publishing message: {}, {}",
                    error.code, error.description
                ),
            ))),

            Response::Ok(publish_ack) => Ok(publish_ack),
        }
    }

    /// Send a request to the jetstream JSON API.
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

        let status = message.status.unwrap_or(StatusCode::OK);
        if status.is_server_error() {
            // TODO(caspervonb) return jetstream::Error with a status code when that type lands.
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!("nats: request failed with status code {:?}", status),
            )));
        }

        let response = serde_json::from_slice(message.payload.as_ref())?;

        Ok(response)
    }

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
                    "nats: error while creating stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

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
                    "nats: error while getting stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

    pub async fn get_or_create_stream<S>(&self, stream_config: S) -> Result<Stream, Error>
    where
        S: Into<Config>,
    {
        let config: Config = stream_config.into();
        let subject = format!("STREAM.INFO.{}", config.name);

        let request: Response<StreamInfo> = self.request(subject, &()).await?;
        match request {
            Response::Err { error } if error.code == 404 => self.create_stream(&config).await,
            Response::Err { error } => Err(Box::new(io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while getting or creating stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(info) => Ok(Stream {
                context: self.clone(),
                info,
            }),
        }
    }

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
                    "nats: error while deleting stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(delete_response) => Ok(delete_response),
        }
    }

    pub async fn update_stream(&self, config: &Config) -> Result<StreamInfo, Error> {
        let subject = format!("STREAM.UPDATE.{}", config.name);
        match self.request(subject, config).await? {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while updating stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(info) => Ok(info),
        }
    }
}
