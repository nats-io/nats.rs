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

use crate::jetstream::response::Response;
use crate::{Client, Error};
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{self, json};

use super::stream::{DeleteStatus, Stream, StreamConfig, StreamInfo};

/// A context which can perform jetstream scoped requests.
#[derive(Debug, Clone)]
pub struct Context {
    client: Client,
    prefix: String,
}

impl Context {
    pub fn new(client: Client) -> Context {
        Context {
            client,
            prefix: "$JS.API".to_string(),
        }
    }

    /// Send a request to the jetstream JSON API.
    pub async fn request<T, V>(&self, subject: String, payload: &T) -> Result<Response<V>, Error>
    where
        T: ?Sized + Serialize,
        V: DeserializeOwned,
    {
        let request = serde_json::to_vec(&payload).map(Bytes::from)?;

        let message = self.client.request(subject, request).await?;
        let response = serde_json::from_slice(message.payload.as_ref())?;

        Ok(response)
    }

    pub async fn create_stream<S>(&self, stream_config: S) -> Result<StreamInfo, Error>
    where
        StreamConfig: From<S>,
    {
        let config: StreamConfig = stream_config.into();
        if config.name.is_empty() {
            return Err(Box::new(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            )));
        }
        let subject = format!("{}.STREAM.CREATE.{}", self.prefix, config.name);
        match self.request(subject, &config).await? {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!("nats: error while creating stream: {}", error.code),
            ))),
            Response::Ok(info) => Ok(info),
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

        let subject = format!("{}.STREAM.INFO.{}", self.prefix, stream);
        let request: Response<StreamInfo> = self.request(subject, &()).await?;
        match request {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!("nats: error while creating stream: {}", error.code),
            ))),
            Response::Ok(info) => Ok(Stream { info }),
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
        let subject = format!("{}.STREAM.DELETE.{}", self.prefix, stream);
        match self.request(subject, &json!({})).await? {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while deleting a stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(delete_response) => Ok(delete_response),
        }
    }

    pub async fn update_stream(&self, config: &StreamConfig) -> Result<StreamInfo, Error> {
        let subject = format!("{}.STREAM.UPDATE.{}", self.prefix, config.name);
        match self.request(subject, config).await? {
            Response::Err { error } => Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!(
                    "nats: error while deleting a stream: {}, {}",
                    error.code, error.description
                ),
            ))),
            Response::Ok(info) => Ok(info),
        }
    }
}
