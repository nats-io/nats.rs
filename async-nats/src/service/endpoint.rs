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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    task::Poll,
    time::Instant,
};

use futures::{Stream, StreamExt};
use serde::{Deserialize, Deserializer, Serialize};
use tracing::{debug, trace};

use crate::{Client, Subscriber};

use super::{error, Endpoints, Request, ShutdownReceiverFuture};

pub struct Endpoint {
    pub(crate) requests: Subscriber,
    pub(crate) stats: Arc<Mutex<Endpoints>>,
    pub(crate) client: Client,
    pub(crate) endpoint: String,
    pub(crate) shutdown: Option<tokio::sync::broadcast::Receiver<()>>,
    pub(crate) shutdown_future: Option<ShutdownReceiverFuture>,
}

impl Stream for Endpoint {
    type Item = Request;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        trace!("polling for next request");
        if let Some(mut receiver) = self.shutdown.take() {
            // Need to initialize `shutdown_future` on first poll
            self.shutdown_future = Some(Box::pin(async move { receiver.recv().await }));
        }

        if let Some(shutdown) = self.shutdown_future.as_mut() {
            match shutdown.as_mut().poll(cx) {
                Poll::Ready(_result) => {
                    debug!("got stop broadcast");
                    self.requests
                        .sender
                        .try_send(crate::Command::Unsubscribe {
                            sid: self.requests.sid,
                            max: None,
                        })
                        .ok();

                    // Clear future, can't be resumed after completion
                    self.shutdown_future = None;
                }
                Poll::Pending => {
                    trace!("stop broadcast still pending");
                }
            }
        }

        trace!("checking for new messages");
        match self.requests.poll_next_unpin(cx) {
            Poll::Ready(message) => {
                debug!("got next message");
                match message {
                    Some(message) => Poll::Ready(Some(Request {
                        issued: Instant::now(),
                        stats: self.stats.clone(),
                        client: self.client.clone(),
                        message,
                        endpoint: self.endpoint.clone(),
                    })),
                    None => Poll::Ready(None),
                }
            }

            Poll::Pending => {
                trace!("still pending for messages");
                Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl Endpoint {
    /// Stops the [Endpoint] and unsubscribes from the subject.
    pub async fn stop(&mut self) -> Result<(), std::io::Error> {
        self.requests
            .unsubscribe()
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "failed to unsubscribe"))
    }
}

/// Stats of a single endpoint.
/// Right now, there is only one business endpoint, all other are internals.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub(crate) struct Inner {
    // Response type.
    #[serde(rename = "type")]
    pub(crate) kind: String,
    /// Endpoint name.
    pub(crate) name: String,
    /// The subject on which the endpoint is registered
    pub(crate) subject: String,
    /// Endpoint specific metadata
    pub(crate) metadata: HashMap<String, String>,
    /// Number of requests handled.
    #[serde(rename = "num_requests")]
    pub(crate) requests: usize,
    /// Number of errors occurred.
    #[serde(rename = "num_errors")]
    pub(crate) errors: usize,
    /// Total processing time for all requests.
    #[serde(default, with = "serde_nanos")]
    pub(crate) processing_time: std::time::Duration,
    /// Average processing time for request.
    #[serde(default, with = "serde_nanos")]
    pub(crate) average_processing_time: std::time::Duration,
    /// Last error that occurred.
    pub(crate) last_error: Option<error::Error>,
    /// Custom data added by [Config::stats_handler]
    pub(crate) data: Option<serde_json::Value>,
    /// Queue group to which this endpoint is assigned to.
    pub(crate) queue_group: String,
}

impl From<Inner> for Stats {
    fn from(inner: Inner) -> Self {
        Stats {
            name: inner.name,
            subject: inner.subject,
            requests: inner.requests,
            errors: inner.errors,
            processing_time: inner.processing_time,
            average_processing_time: inner.average_processing_time,
            last_error: inner.last_error,
            data: inner.data,
            queue_group: inner.queue_group,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Stats {
    /// Endpoint name.
    pub name: String,
    /// The subject on which the endpoint is registered
    pub subject: String,
    /// Number of requests handled.
    #[serde(rename = "num_requests")]
    pub requests: usize,
    /// Number of errors occurred.
    #[serde(rename = "num_errors")]
    pub errors: usize,
    /// Total processing time for all requests.
    #[serde(default, with = "serde_nanos")]
    pub processing_time: std::time::Duration,
    /// Average processing time for request.
    #[serde(default, with = "serde_nanos")]
    pub average_processing_time: std::time::Duration,
    /// Last error that occurred.
    #[serde(with = "serde_error_string")]
    pub last_error: Option<error::Error>,
    /// Custom data added by [crate::service::Config::stats_handler]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Queue group to which this endpoint is assigned to.
    pub queue_group: String,
}

mod serde_error_string {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::error;

    pub(crate) fn serialize<S>(
        error: &Option<error::Error>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match error {
            Some(error) => serializer.serialize_str(&format!("{}:{}", error.code, error.status)),
            None => serializer.serialize_str(""),
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<error::Error>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        if string.is_empty() {
            Ok(None)
        } else if let Some((code, status)) = &string.split_once(':') {
            let err_code: usize = code.parse().unwrap_or(0);
            let status = if err_code == 0 {
                string.clone()
            } else {
                status.to_string()
            };
            Ok(Some(error::Error {
                code: err_code,
                status,
            }))
        } else {
            Ok(Some(error::Error {
                code: 0,
                status: string,
            }))
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Info {
    /// Name of the endpoint.
    pub name: String,
    /// Endpoint subject.
    pub subject: String,
    /// Queue group to which this endpoint is assigned.
    pub queue_group: String,
    /// Endpoint-specific metadata.
    #[serde(default, deserialize_with = "null_meta_as_default")]
    pub metadata: HashMap<String, String>,
}

pub(crate) fn null_meta_as_default<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let metadata: Option<HashMap<String, String>> = Option::deserialize(deserializer)?;
    Ok(metadata.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_serde() {
        #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
        struct WithOptionalError {
            #[serde(with = "serde_error_string")]
            error: Option<error::Error>,
        }

        // serialize and deserialize error with value.
        let with_error = WithOptionalError {
            error: Some(error::Error {
                code: 500,
                status: "error".to_string(),
            }),
        };

        let serialized = serde_json::to_string(&with_error).unwrap();
        assert_eq!(serialized, r#"{"error":"500:error"}"#);

        let deserialized: WithOptionalError = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, with_error);

        // serialize and deserialize error without value.
        let without_error = WithOptionalError { error: None };
        let serialized = serde_json::to_string(&without_error).unwrap();
        assert_eq!(serialized, r#"{"error":""}"#);

        let deserialized: WithOptionalError = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, without_error);

        // deserialize error without code.
        let serialized = r#"{"error":"error"}"#;
        let deserialized: WithOptionalError = serde_json::from_str(serialized).unwrap();
        assert_eq!(
            deserialized,
            WithOptionalError {
                error: Some(error::Error {
                    code: 0,
                    status: "error".to_string(),
                })
            }
        );

        // deserialize error with invalid code.
        let serialized = r#"{"error":"invalid:error"}"#;
        let deserialized: WithOptionalError = serde_json::from_str(serialized).unwrap();
        assert_eq!(
            deserialized,
            WithOptionalError {
                error: Some(error::Error {
                    code: 0,
                    status: "invalid:error".to_string(),
                })
            }
        );
    }
}
