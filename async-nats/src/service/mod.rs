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
    task::Poll,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{
    stream::{self, SelectAll},
    Future, Stream, StreamExt,
};
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::serde::rfc3339;
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tracing::{debug, trace};

use crate::{Client, Error, HeaderMap, Message, PublishError, Subscriber};

const SERVICE_API_PREFIX: &str = "$SRV";
const QUEUE_GROUP: &str = "q";
pub const NATS_SERVICE_ERROR: &str = "Nats-Service-Error";
pub const NATS_SERVICE_ERROR_CODE: &str = "Nats-Service-Error-Code";

lazy_static! {
    // uses recommended semver validation expression from
    // https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
    static ref SEMVER: Regex = Regex::new(r#"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"#).unwrap();
    // From ADR-33: Name can only have A-Z, a-z, 0-9, dash, underscore.
    static ref NAME: Regex = Regex::new(r#"^[A-Za-z0-9\-_]+$"#).unwrap();
}

/// Represents stats for all endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    pub endpoints: HashMap<String, EndpointStats>,
}

/// Response for `STATS` requests.
#[derive(Serialize, Deserialize)]
pub struct StatsResponse {
    #[serde(rename = "type")]
    pub response_type: String,
    pub name: String,
    pub id: String,
    pub version: String,
    #[serde(with = "rfc3339")]
    pub started: OffsetDateTime,
    pub endpoints: Vec<EndpointStats>,
}

/// Stats of a single endpoint.
/// Right now, there is only one business endpoint, all other are internals.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct EndpointStats {
    #[serde(rename = "type")]
    pub response_type: String,
    pub name: String,
    #[serde(rename = "num_requests")]
    pub requests: usize,
    #[serde(rename = "num_errors")]
    pub errors: usize,
    #[serde(default, with = "serde_nanos")]
    pub processing_time: std::time::Duration,
    #[serde(default, with = "serde_nanos")]
    pub average_processing_time: std::time::Duration,
    pub last_error: Option<error::Error>,
    pub data: String,
}

/// Information about service instance.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Info {
    #[serde(rename = "type")]
    pub response_type: String,
    pub name: String,
    pub id: String,
    pub description: Option<String>,
    pub version: String,
    pub subjects: Vec<String>,
}

/// Schema of requests and responses.
/// Currently, it does not do anything except providing information.
#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    /// A string/url describing the format of the request payload can be JSON schema etc.
    pub request: String,
    /// A string/url describing the format of the request payload can be JSON schema etc.
    pub response: String,
}

/// Configuration of the [Service].
#[derive(Debug)]
pub struct Config {
    /// Really the kind of the service. Shared by all the services that have the same name.
    /// This name can only have A-Z, a-z, 0-9, dash, underscore
    pub name: String,
    /// a human-readable description about the service
    pub description: Option<String>,
    /// A SemVer valid service version.
    pub version: String,
    /// Request / Response schemas
    pub schema: Option<Schema>,
    /// Custom handler for providing the `EndpointStats.data` value.
    pub stats_handler: Option<StatsHandler>,
}

pub struct ServiceBuilder {
    client: Client,
    description: Option<String>,
    schema: Option<Schema>,
    stats_handler: Option<StatsHandler>,
}

impl ServiceBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            description: None,
            schema: None,
            stats_handler: None,
        }
    }

    /// Adds description for the service.
    pub fn description<S: ToString>(mut self, description: S) -> Self {
        self.description = Some(description.to_string());
        self
    }

    /// Adds schema to the service.
    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Adds hander for custom service statistics.
    pub fn stats_handler<F>(mut self, handler: F) -> Self
    where
        F: FnMut(String, EndpointStats) -> String + Send + Sync + 'static,
    {
        self.stats_handler = Some(StatsHandler(Box::new(handler)));
        self
    }

    /// Stats the service with configured options.
    pub async fn start<S: ToString>(self, name: S, version: S) -> Result<Service, Error> {
        Service::add(
            self.client,
            Config {
                name: name.to_string(),
                version: version.to_string(),
                description: self.description,
                schema: self.schema,
                stats_handler: self.stats_handler,
            },
        )
        .await
    }
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
    ///     schema: None,
    ///     description: None,
    ///     stats_handler: None,
    /// }).await?;
    ///
    /// let mut endpoint = service.endpoint("get").await?;
    ///
    /// if let Some(request) = endpoint.next().await {
    ///     request.respond(Ok("hello".into())).await?;
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    fn add_service(&self, config: Config) -> Self::Output;

    /// Returns Service instance builder.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use futures::StreamExt;
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.service_builder()
    ///     .description("some service")
    ///     .stats_handler(|endpoint, stats| format!("customstats"))
    ///     .start("products","1.0.0").await?;
    ///
    /// let mut endpoint = service.endpoint("get").await?;
    ///
    /// if let Some(request) = endpoint.next().await {
    ///     request.respond(Ok("hello".into())).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    fn service_builder(&self) -> ServiceBuilder;
}

impl ServiceExt for crate::Client {
    type Output = Pin<Box<dyn Future<Output = Result<Service, crate::Error>>>>;

    fn add_service(&self, config: Config) -> Self::Output {
        let client = self.clone();
        Box::pin(async { Service::add(client, config).await })
    }

    fn service_builder(&self) -> ServiceBuilder {
        ServiceBuilder::new(self.clone())
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
///     schema: None,
///     description: None,
///     stats_handler: None,
/// }).await?;
///
/// let mut endpoint = service.endpoint("get").await?;
///
/// if let Some(request) = endpoint.next().await {
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
    handle: JoinHandle<Result<(), Error>>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    subjects: Arc<Mutex<Vec<String>>>,
}

pub struct StatsHandler(pub Box<dyn FnMut(String, EndpointStats) -> String + Send>);

impl std::fmt::Debug for StatsHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stats handler")
    }
}

pub struct Group {
    prefix: String,
    stats: Arc<Mutex<Stats>>,
    client: Client,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

impl Group {
    pub fn group<S: ToString>(&self, prefix: S) -> Group {
        Group {
            prefix: prefix.to_string(),
            stats: self.stats.clone(),
            client: self.client.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }
    pub async fn endpoint<S: ToString>(&self, subject: S) -> Result<Endpoint, Error> {
        let subject = subject.to_string();
        let requests = self
            .client
            .queue_subscribe(
                format!("{}.{subject}", self.prefix),
                QUEUE_GROUP.to_string(),
            )
            .await?;
        debug!("created service for endpoint {}.{subject}", self.prefix);

        let shutdown_rx = self.shutdown_tx.subscribe();

        let mut stats = self.stats.lock().unwrap();
        stats
            .endpoints
            .entry(subject.clone())
            .or_insert(EndpointStats {
                name: subject.clone(),
                ..Default::default()
            });
        Ok(Endpoint {
            requests,
            stats: self.stats.clone(),
            client: self.client.clone(),
            endpoint: subject,
            shutdown_rx: Some(shutdown_rx),
            shutdown: None,
        })
    }
}

impl Service {
    async fn add(client: Client, config: Config) -> Result<Service, Error> {
        // validate service version semver string.
        if !SEMVER.is_match(config.version.as_str()) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "service version is not a valid semver string",
            )));
        }
        // validate service name.
        if !NAME.is_match(config.name.as_str()) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "service name is not a valid string (only A-Z, a-z, 0-9, _, - are allowed)",
            )));
        }
        let id = nuid::next();
        let started = time::OffsetDateTime::now_utc();
        let subjects = Arc::new(Mutex::new(Vec::new()));
        let info = Info {
            response_type: "io.nats.micro.v1.info_response".to_string(),
            name: config.name.clone(),
            id: id.clone(),
            description: config.description.clone(),
            version: config.version.clone(),
            subjects: Vec::default(),
        };

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        let endpoints = HashMap::new();
        let endpoint_stats = Arc::new(Mutex::new(Stats { endpoints }));

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
            let mut stats_callback = config.stats_handler;
            let info = info.clone();
            let subjects = subjects.clone();
            let endpoint_stats = endpoint_stats.clone();
            let client = client.clone();
            let schema_json = serde_json::to_vec(&json!({
                "type": "io.nats.micro.v1.schema_response",
                "name": config.name.clone(),
                "id": id.clone(),
                "version": config.version.clone(),
            }))
            .map(Bytes::from)?;
            async move {
                loop {
                    tokio::select! {
                        Some(ping) = pings.next() => {
                            let pong = serde_json::to_vec(&json!({
                                "type": "io.nats.micro.v1.ping_response",
                                "name": info.name,
                                "id": info.id,
                                "version": info.version,
                            }))?;
                            client.publish(ping.reply.unwrap(), pong.into()).await?;
                        },
                        Some(info_request) = infos.next() => {
                            let subjects = subjects.clone();
                            let info = info.clone();
                            let info = Info {
                                subjects: subjects.lock().unwrap().to_vec(),
                                ..info
                            };
                            let info_json = serde_json::to_vec(&info).map(Bytes::from)?;
                            client.publish(info_request.reply.unwrap(), info_json.clone()).await?;
                        },
                        Some(schema_request) = schemas.next() => {
                            client.publish(schema_request.reply.unwrap(), schema_json.clone()).await?;
                        },
                        Some(stats_request) = stats.next() => {
                            if let Some(stats_callback) = stats_callback.as_mut() {

                                let mut endpoint_stats_locked = endpoint_stats.lock().unwrap();
                                for (key, value) in &mut endpoint_stats_locked.endpoints {
                                    let data = stats_callback.0(key.to_string(), value.clone());
                                    value.data = data;
                                }
                            }

                            let stats = serde_json::to_vec(&StatsResponse {
                                response_type: "io.nats.micro.v1.stats_response".to_string(),
                                name: info.name.clone(),
                                id: info.id.clone(),
                                version: info.version.clone(),
                                started,
                                endpoints: endpoint_stats.lock().unwrap().endpoints.values().cloned().collect(),
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
            handle,
            shutdown_tx,
            subjects,
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
    let verb_name = client
        .subscribe(format!("{SERVICE_API_PREFIX}.{verb}.{name}"))
        .await?;
    let verb_id = client
        .subscribe(format!("{SERVICE_API_PREFIX}.{verb}.{name}.{id}"))
        .await?;
    Ok(stream::select_all([verb_all, verb_id, verb_name]).fuse())
}

type ShutdownReceiverFuture = Pin<
    Box<dyn Future<Output = Result<(), tokio::sync::broadcast::error::RecvError>> + Send + Sync>,
>;

pub struct Endpoint {
    requests: Subscriber,
    stats: Arc<Mutex<Stats>>,
    client: Client,
    endpoint: String,
    shutdown_rx: Option<tokio::sync::broadcast::Receiver<()>>,
    shutdown: Option<ShutdownReceiverFuture>,
}

impl Stream for Endpoint {
    type Item = Request;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        trace!("polling for next request");
        match self.shutdown.as_mut() {
            Some(shutdown) => match shutdown.as_mut().poll(cx) {
                Poll::Ready(_result) => {
                    debug!("got stop broadcast");
                    self.requests
                        .sender
                        .try_send(crate::Command::Unsubscribe {
                            sid: self.requests.sid,
                            max: None,
                        })
                        .ok();
                }
                Poll::Pending => {
                    trace!("stop broadcast still pending");
                }
            },
            None => {
                let mut receiver = self.shutdown_rx.take().unwrap();
                self.shutdown = Some(Box::pin(async move { receiver.recv().await }));
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
    pub async fn stop(&mut self) -> Result<(), std::io::Error> {
        self.requests.unsubscribe().await
    }
}

impl Service {
    /// Stops this instance of the [Service].
    /// If there are more instances of [Services][Service] with the same name, the [Service] will
    /// be scaled down by one instance. If it was the only running instance, it will effectively
    /// remove the service entirely.
    pub async fn stop(self) -> Result<(), Error> {
        self.shutdown_tx.send(())?;
        self.handle.abort();
        Ok(())
    }

    /// Resets [Stats] of the [Service] instance.
    pub async fn reset(&mut self) {
        for value in self.stats.lock().unwrap().endpoints.values_mut() {
            value.errors = 0;
            value.processing_time = Duration::default();
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

    pub fn group<S: ToString>(&self, prefix: S) -> Group {
        Group {
            prefix: prefix.to_string(),
            stats: self.stats.clone(),
            client: self.client.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }
    pub async fn endpoint<S: ToString>(&self, subject: S) -> Result<Endpoint, Error> {
        let subject = subject.to_string();
        let requests = self
            .client
            .queue_subscribe(subject.clone(), QUEUE_GROUP.to_string())
            .await?;
        debug!("created service for endpoint {subject}");

        let shutdown_rx = self.shutdown_tx.subscribe();

        let mut stats = self.stats.lock().unwrap();
        stats
            .endpoints
            .entry(subject.clone())
            .or_insert(EndpointStats {
                name: subject.clone(),
                ..Default::default()
            });
        self.subjects.lock().unwrap().push(subject.clone());
        Ok(Endpoint {
            requests,
            stats: self.stats.clone(),
            client: self.client.clone(),
            endpoint: subject,
            shutdown_rx: Some(shutdown_rx),
            shutdown: None,
        })
    }
}

/// Request returned by [Service] [Stream][futures::Stream].
#[derive(Debug)]
pub struct Request {
    issued: Instant,
    client: Client,
    pub message: Message,
    endpoint: String,
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
    /// #     schema: None,
    /// #     description: None,
    ///     stats_handler: None,
    /// # }).await?;
    ///
    /// let mut endpoint = service.endpoint("endpoint").await?;
    /// let request = endpoint.next().await.unwrap();
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
                    .entry(self.endpoint.clone())
                    .and_modify(|stats| {
                        stats.last_error = Some(err.clone());
                        stats.errors += 1;
                    })
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
        // let mut stats = stats.endpoints.entry(key)
        let mut stats = stats.endpoints.get_mut(self.endpoint.as_str()).unwrap();
        stats.requests += 1;
        stats.processing_time += elapsed;
        stats.average_processing_time = stats.processing_time.checked_div(2).unwrap();
        result
    }
}
