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

pub mod error;

use std::{
    collections::HashMap,
    fmt::Display,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{
    stream::{self, SelectAll},
    Future, Stream, StreamExt, TryFutureExt,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::serde::rfc3339;
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tracing::debug;

use crate::{Client, Error, HeaderMap, Message, PublishError, Subscriber};

const SERVICE_API_PREFIX: &str = "$SRV";
const QUEUE_GROUP: &str = "q";
pub const NATS_SERVICE_ERROR: &str = "Nats-Service-Error";
pub const NATS_SERVICE_ERROR_CODE: &str = "Nats-Service-Error-Code";

/// Represents stats for all endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    endpoints: HashMap<String, EndpointStats>,
}

/// Response for `STATS` requests.
#[derive(Serialize, Deserialize)]
pub struct StatsResponse {
    pub name: String,
    pub id: String,
    pub version: String,
    #[serde(with = "rfc3339")]
    pub started: OffsetDateTime,
    pub stats: Vec<EndpointStats>,
}

/// Stats of a single endpoint.
/// Right now, there is only one business endpoint, all other are internals.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct EndpointStats {
    pub name: String,
    #[serde(rename = "num_requests")]
    pub requests: usize,
    #[serde(rename = "num_errors")]
    pub errors: usize,
    pub total_processing_time: std::time::Duration,
    pub average_processing_time: std::time::Duration,
}

/// Information about service instance.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Info {
    pub name: String,
    pub id: String,
    pub description: Option<String>,
    pub version: String,
    pub subject: String,
}

/// Schema of requests and responses.
/// Currently, it does not do anything except providing information.
#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    pub request: String,
    pub response: String,
}

/// Endpoint definition.
/// In this iteration, it's just a subject.
#[derive(Clone, Debug)]
pub struct Endpoint {
    pub subject: String,
}

/// Configuration of the [Service].
#[derive(Debug)]
pub struct Config {
    pub name: String,
    pub description: Option<String>,
    pub version: String,
    pub schema: Option<Schema>,
    pub endpoint: String,
}

/// Verbs that can be used to acquire information from the services.
pub enum Verb {
    Ping,
    Stats,
    Info,
    Schema,
}

impl Display for Verb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Verb::Ping => write!(f, "PING"),
            Verb::Stats => write!(f, "STATS"),
            Verb::Info => write!(f, "INFO"),
            Verb::Schema => write!(f, "SCHEMA"),
        }
    }
}

pub trait ServiceExt {
    type Output: Future<Output = Result<Service, crate::Error>>;

    /// Adds a Service instance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.add_service( async_nats::service::Config {
    ///     name: "generator".to_string(),
    ///     version: "1.0.0".to_string(),
    ///     endpoint: "events.>".to_string(),
    ///     schema: None,
    ///     description: None,
    /// }).await?;
    ///
    /// if let Some(request) = service.next().await {
    ///     request.respond(Ok("hello".into())).await?;
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    fn add_service(&self, config: Config) -> Self::Output;
}

impl ServiceExt for crate::Client {
    type Output = Pin<Box<dyn Future<Output = Result<Service, crate::Error>>>>;

    fn add_service(&self, config: Config) -> Self::Output {
        let client = self.clone();
        Box::pin(async { Service::add(client, config).await })
    }
}

/// Service instance.
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// use futures::StreamExt;
/// use async_nats::service::ServiceExt;
/// let client = async_nats::connect("demo.nats.io").await?;
/// let mut service = client.add_service( async_nats::service::Config {
///     name: "generator".to_string(),
///     version: "1.0.0".to_string(),
///     endpoint: "events.>".to_string(),
///     schema: None,
///     description: None,
/// }).await?;
///
/// if let Some(request) = service.next().await {
///     request.respond(Ok("hello".into())).await?;
/// }
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Service {
    stats: Arc<Mutex<Stats>>,
    info: Info,
    client: Client,
    requests: Subscriber,
    handle: JoinHandle<Result<(), Error>>,
}
impl Service {
    async fn add(client: Client, config: Config) -> Result<Service, Error> {
        let id = nuid::next();
        let started = time::OffsetDateTime::now_utc();
        let info = Info {
            name: config.name.clone(),
            id: id.clone(),
            description: config.description.clone(),
            version: config.version.clone(),
            subject: config.endpoint.clone(),
        };
        let request_stats = EndpointStats {
            // FIXME: what should be the name?
            name: "requests".to_string(),
            ..Default::default()
        };

        let mut endpoints = HashMap::new();
        endpoints.insert("requests".to_string(), request_stats);
        let endpoint_stats = Arc::new(Mutex::new(Stats { endpoints }));
        let requests = client
            .queue_subscribe(
                format!("{SERVICE_API_PREFIX}.{}", config.endpoint.clone()),
                QUEUE_GROUP.to_string(),
            )
            .await?;
        debug!(
            "crerated service for endpoint {}.{}",
            SERVICE_API_PREFIX,
            config.endpoint.clone()
        );

        // create subscriptions for all verbs.
        let mut pings =
            verb_subscription(client.clone(), Verb::Ping, config.name.clone(), id.clone()).await?;
        let mut infos =
            verb_subscription(client.clone(), Verb::Info, config.name.clone(), id.clone()).await?;
        let mut schemas = verb_subscription(
            client.clone(),
            Verb::Schema,
            config.name.clone(),
            id.clone(),
        )
        .await?;
        let mut stats =
            verb_subscription(client.clone(), Verb::Stats, config.name.clone(), id.clone()).await?;

        // Start a task for handling verbs subscriptions.
        let handle = tokio::task::spawn({
            let info = info.clone();
            let endpoint_stats = endpoint_stats.clone();
            let client = client.clone();
            let info_json = serde_json::to_vec(&info).map(Bytes::from)?;
            let schema_json = serde_json::to_vec(&json!({
                "name": config.name.clone(),
                "id": id.clone(),
                "version": config.version.clone(),
                "schema": config.schema,
            }))
            .map(Bytes::from)?;
            async move {
                loop {
                    tokio::select! {
                        Some(ping) = pings.next() => {
                            let pong = serde_json::to_vec(&json!({
                                "name": info.name,
                                "id": info.id,
                            }))?;
                            client.publish(ping.reply.unwrap(), pong.into()).await?;
                            endpoint_stats.lock().unwrap().endpoints.entry("ping".to_string()).and_modify(|stat| {
                                stat.requests += 1;
                            }).or_default();

                        },
                        Some(info_request) = infos.next() => {
                            client.publish(info_request.reply.unwrap(), info_json.clone()).await?;
                        },
                        Some(schema_request) = schemas.next() => {
                            client.publish(schema_request.reply.unwrap(), schema_json.clone()).await?;
                        },
                        // FIXME: proper status handling
                        Some(stats_request) = stats.next() => {
                            let stats = serde_json::to_vec(&StatsResponse {
                                name: info.name.clone(),
                                id: info.id.clone(),
                                version: info.version.clone(),
                                started,
                                stats: endpoint_stats.lock().unwrap().endpoints.values().cloned().collect(),
                            })?;
                            client.publish(stats_request.reply.unwrap(), stats.into()).await?;
                        },
                        else => break,
                    }
                }
                Ok(())
            }
        });
        Ok(Service {
            stats: endpoint_stats,
            info,
            client,
            requests,
            handle,
        })
    }
}
async fn verb_subscription(
    client: Client,
    verb: Verb,
    name: String,
    id: String,
) -> Result<futures::stream::Fuse<SelectAll<Subscriber>>, Error> {
    let verb_all = client
        .subscribe(format!("{SERVICE_API_PREFIX}.{verb}"))
        .await?;
    println!("CRERATING SUB: {SERVICE_API_PREFIX}.{verb}.{name}");
    let verb_name = client
        .subscribe(format!("{SERVICE_API_PREFIX}.{verb}.{name}"))
        .await?;
    let verb_id = client
        .subscribe(format!("{SERVICE_API_PREFIX}.{verb}.{name}.{id}"))
        .await?;
    Ok(stream::select_all([verb_all, verb_id, verb_name]).fuse())
}

impl Stream for Service {
    type Item = Request;

    // FIXME: how we implement stats (durations) if it's a stream?
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.requests.poll_next_unpin(cx) {
            std::task::Poll::Ready(message) => match message {
                Some(message) => std::task::Poll::Ready(Some(Request {
                    issued: Instant::now(),
                    stats: self.stats.clone(),
                    client: self.client.clone(),
                    message,
                })),
                None => std::task::Poll::Ready(None),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Service {
    /// Stops this instance of the [Service].
    /// If there are more instances of [Services][Service] with the same name, the [Service] will
    /// be scaled down by one instance. If it was the only running instance, it will effectively
    /// remove the service entirely.
    pub async fn stop(mut self) -> Result<(), Error> {
        self.requests.unsubscribe().map_err(Box::new).await?;
        self.handle.abort();
        Ok(())
    }

    /// Resets [Stats] of the [Service] instance.
    pub async fn reset(&mut self) {
        for value in self.stats.lock().unwrap().endpoints.values_mut() {
            value.errors = 0;
            value.total_processing_time = Duration::default();
            value.requests = 0;
            value.average_processing_time = Duration::default();
        }
    }

    /// Returns [Stats] for this service instance.
    pub async fn stats(&self) -> Stats {
        self.stats.lock().unwrap().clone()
    }

    /// Returns [Info] for this service instance.
    pub async fn info(&self) -> Info {
        self.info.clone()
    }
}

/// Request returned by [Service] [Stream][futures::Stream].
#[derive(Debug)]
pub struct Request {
    issued: Instant,
    client: Client,
    pub message: Message,
    stats: Arc<Mutex<Stats>>,
}

impl Request {
    /// Sends response for the request.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use async_nats::service::ServiceExt;
    /// # let client = async_nats::connect("demo.nats.io").await?;
    /// # let mut service = client.add_service(async_nats::service::Config {
    /// #     name: "generator".to_string(),
    /// #     version: "1.0.0".to_string(),
    /// #     endpoint: "events.>".to_string(),
    /// #     schema: None,
    /// #     description: None,
    /// # }).await?;
    ///
    /// let request = service.next().await.unwrap();
    /// request.respond(Ok("hello".into())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn respond(self, response: Result<Bytes, error::Error>) -> Result<(), PublishError> {
        let reply = self.message.reply.clone().unwrap();
        let result = match response {
            Ok(payload) => self.client.publish(reply, payload).await,
            Err(err) => {
                self.stats
                    .lock()
                    .unwrap()
                    .endpoints
                    .entry("requests".to_string())
                    .and_modify(|stats| stats.errors += 1)
                    .or_default();
                let mut headers = HeaderMap::new();
                headers.insert(NATS_SERVICE_ERROR, err.1.as_str());
                headers.insert(NATS_SERVICE_ERROR_CODE, err.0.to_string().as_str());
                self.client
                    .publish_with_headers(reply, headers, "".into())
                    .await
            }
        };
        let elapsed = self.issued.elapsed();
        let mut stats = self.stats.lock().unwrap();
        let mut stats = stats.endpoints.get_mut("requests").unwrap();
        stats.requests += 1;
        stats.total_processing_time += elapsed;
        stats.average_processing_time = stats.total_processing_time.checked_div(2).unwrap();
        result
    }
}
