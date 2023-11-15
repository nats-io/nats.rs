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

pub mod error;

use std::{
    collections::HashMap,
    fmt::Display,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
pub mod endpoint;
use futures::{
    stream::{self, SelectAll},
    Future, StreamExt,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use time::serde::rfc3339;
use time::OffsetDateTime;
use tokio::{sync::broadcast::Sender, task::JoinHandle};
use tracing::debug;

use crate::{Client, Error, HeaderMap, Message, PublishError, Subscriber};

use self::endpoint::Endpoint;

const SERVICE_API_PREFIX: &str = "$SRV";
const DEFAULT_QUEUE_GROUP: &str = "q";
pub const NATS_SERVICE_ERROR: &str = "Nats-Service-Error";
pub const NATS_SERVICE_ERROR_CODE: &str = "Nats-Service-Error-Code";

// uses recommended semver validation expression from
// https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
static SEMVER: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$")
        .unwrap()
});
// From ADR-33: Name can only have A-Z, a-z, 0-9, dash, underscore.
static NAME: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[A-Za-z0-9\-_]+$").unwrap());

/// Represents state for all endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Endpoints {
    pub(crate) endpoints: HashMap<String, endpoint::Inner>,
}

/// Response for `PING` requests.
#[derive(Serialize, Deserialize)]
pub struct PingResponse {
    /// Response type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Service name.
    pub name: String,
    /// Service id.
    pub id: String,
    /// Service version.
    pub version: String,
    /// Additional metadata
    #[serde(default, deserialize_with = "endpoint::null_meta_as_default")]
    pub metadata: HashMap<String, String>,
}

/// Response for `STATS` requests.
#[derive(Serialize, Deserialize)]
pub struct Stats {
    /// Response type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Service name.
    pub name: String,
    /// Service id.
    pub id: String,
    // Service version.
    pub version: String,
    #[serde(with = "rfc3339")]
    pub started: OffsetDateTime,
    /// Statistics of all endpoints.
    pub endpoints: Vec<endpoint::Stats>,
}

/// Information about service instance.
/// Service name.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Info {
    /// Response type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Service name.
    pub name: String,
    /// Service id.
    pub id: String,
    /// Service description.
    pub description: String,
    /// Service version.
    pub version: String,
    /// Additional metadata
    #[serde(default, deserialize_with = "endpoint::null_meta_as_default")]
    pub metadata: HashMap<String, String>,
    /// Info about all service endpoints.
    pub endpoints: Vec<endpoint::Info>,
}

/// Configuration of the [Service].
#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    /// Really the kind of the service. Shared by all the services that have the same name.
    /// This name can only have A-Z, a-z, 0-9, dash, underscore
    pub name: String,
    /// a human-readable description about the service
    pub description: Option<String>,
    /// A SemVer valid service version.
    pub version: String,
    /// Custom handler for providing the `EndpointStats.data` value.
    #[serde(skip)]
    pub stats_handler: Option<StatsHandler>,
    /// Additional service metadata
    pub metadata: Option<HashMap<String, String>>,
    /// Custom queue group config
    pub queue_group: Option<String>,
}

pub struct ServiceBuilder {
    client: Client,
    description: Option<String>,
    stats_handler: Option<StatsHandler>,
    metadata: Option<HashMap<String, String>>,
    queue_group: Option<String>,
}

impl ServiceBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            description: None,
            stats_handler: None,
            metadata: None,
            queue_group: None,
        }
    }

    /// Description for the service.
    pub fn description<S: ToString>(mut self, description: S) -> Self {
        self.description = Some(description.to_string());
        self
    }

    /// Handler for custom service statistics.
    pub fn stats_handler<F>(mut self, handler: F) -> Self
    where
        F: FnMut(String, endpoint::Stats) -> serde_json::Value + Send + Sync + 'static,
    {
        self.stats_handler = Some(StatsHandler(Box::new(handler)));
        self
    }

    /// Additional service metadata.
    pub fn metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Custom queue group. Default is `q`.
    pub fn queue_group<S: ToString>(mut self, queue_group: S) -> Self {
        self.queue_group = Some(queue_group.to_string());
        self
    }

    /// Starts the service with configured options.
    pub async fn start<S: ToString>(self, name: S, version: S) -> Result<Service, Error> {
        Service::add(
            self.client,
            Config {
                name: name.to_string(),
                version: version.to_string(),
                description: self.description,
                stats_handler: self.stats_handler,
                metadata: self.metadata,
                queue_group: self.queue_group,
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
    /// use async_nats::service::ServiceExt;
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client
    ///     .add_service(async_nats::service::Config {
    ///         name: "generator".to_string(),
    ///         version: "1.0.0".to_string(),
    ///         description: None,
    ///         stats_handler: None,
    ///         metadata: None,
    ///         queue_group: None,
    ///     })
    ///     .await?;
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
    /// use async_nats::service::ServiceExt;
    /// use futures::StreamExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client
    ///     .service_builder()
    ///     .description("some service")
    ///     .stats_handler(|endpoint, stats| serde_json::json!({ "endpoint": endpoint }))
    ///     .start("products", "1.0.0")
    ///     .await?;
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
    type Output = Pin<Box<dyn Future<Output = Result<Service, crate::Error>> + Send>>;

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
/// use async_nats::service::ServiceExt;
/// use futures::StreamExt;
/// let client = async_nats::connect("demo.nats.io").await?;
/// let mut service = client.service_builder().start("generator", "1.0.0").await?;
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
    endpoints_state: Arc<Mutex<Endpoints>>,
    info: Info,
    client: Client,
    handle: JoinHandle<Result<(), Error>>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    subjects: Arc<Mutex<Vec<String>>>,
    queue_group: String,
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
        let endpoints_state = Arc::new(Mutex::new(Endpoints {
            endpoints: HashMap::new(),
        }));

        let queue_group = config
            .queue_group
            .unwrap_or(DEFAULT_QUEUE_GROUP.to_string());
        let id = nuid::next().to_string();
        let started = time::OffsetDateTime::now_utc();
        let subjects = Arc::new(Mutex::new(Vec::new()));
        let info = Info {
            kind: "io.nats.micro.v1.info_response".to_string(),
            name: config.name.clone(),
            id: id.clone(),
            description: config.description.clone().unwrap_or_default(),
            version: config.version.clone(),
            metadata: config.metadata.clone().unwrap_or_default(),
            endpoints: Vec::new(),
        };

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        // create subscriptions for all verbs.
        let mut pings =
            verb_subscription(client.clone(), Verb::Ping, config.name.clone(), id.clone()).await?;
        let mut infos =
            verb_subscription(client.clone(), Verb::Info, config.name.clone(), id.clone()).await?;
        let mut stats =
            verb_subscription(client.clone(), Verb::Stats, config.name.clone(), id.clone()).await?;

        // Start a task for handling verbs subscriptions.
        let handle = tokio::task::spawn({
            let mut stats_callback = config.stats_handler;
            let info = info.clone();
            let endpoints_state = endpoints_state.clone();
            let client = client.clone();
            async move {
                loop {
                    tokio::select! {
                        Some(ping) = pings.next() => {
                            let pong = serde_json::to_vec(&PingResponse{
                                kind: "io.nats.micro.v1.ping_response".to_string(),
                                name: info.name.clone(),
                                id: info.id.clone(),
                                version: info.version.clone(),
                                metadata: info.metadata.clone(),
                            })?;
                            client.publish(ping.reply.unwrap(), pong.into()).await?;
                        },
                        Some(info_request) = infos.next() => {
                            let info = info.clone();

                            let endpoints: Vec<endpoint::Info> = {
                                endpoints_state.lock().unwrap().endpoints.values().map(|value| {
                                    endpoint::Info {
                                        name: value.name.to_owned(),
                                        subject: value.subject.to_owned(),
                                        queue_group: value.queue_group.to_owned(),
                                        metadata: value.metadata.to_owned()
                                    }
                                }).collect()
                            };
                            let info = Info {
                                endpoints,
                                ..info
                            };
                            let info_json = serde_json::to_vec(&info).map(Bytes::from)?;
                            client.publish(info_request.reply.unwrap(), info_json.clone()).await?;
                        },
                        Some(stats_request) = stats.next() => {
                            if let Some(stats_callback) = stats_callback.as_mut() {
                                let mut endpoint_stats_locked = endpoints_state.lock().unwrap();
                                for (key, value) in &mut endpoint_stats_locked.endpoints {
                                    let data = stats_callback.0(key.to_string(), value.clone().into());
                                    value.data = Some(data);
                                }
                            }
                            let stats = serde_json::to_vec(&Stats {
                                kind: "io.nats.micro.v1.stats_response".to_string(),
                                name: info.name.clone(),
                                id: info.id.clone(),
                                version: info.version.clone(),
                                started,
                                endpoints: endpoints_state.lock().unwrap().endpoints.values().cloned().map(Into::into).collect(),
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
            endpoints_state,
            info,
            client,
            handle,
            shutdown_tx,
            subjects,
            queue_group,
        })
    }
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
        for value in self.endpoints_state.lock().unwrap().endpoints.values_mut() {
            value.errors = 0;
            value.processing_time = Duration::default();
            value.requests = 0;
            value.average_processing_time = Duration::default();
        }
    }

    /// Returns [Stats] for this service instance.
    pub async fn stats(&self) -> HashMap<String, endpoint::Stats> {
        self.endpoints_state
            .lock()
            .unwrap()
            .endpoints
            .iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned().into()))
            .collect()
    }

    /// Returns [Info] for this service instance.
    pub async fn info(&self) -> Info {
        self.info.clone()
    }

    /// Creates a group for endpoints under common prefix.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.service_builder().start("service", "1.0.0").await?;
    ///
    /// let v1 = service.group("v1");
    /// let products = v1.endpoint("products").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn group<S: ToString>(&self, prefix: S) -> Group {
        self.group_with_queue_group(prefix, self.queue_group.clone())
    }

    /// Creates a group for endpoints under common prefix with custom queue group.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.service_builder().start("service", "1.0.0").await?;
    ///
    /// let v1 = service.group("v1");
    /// let products = v1.endpoint("products").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn group_with_queue_group<S: ToString, Z: ToString>(
        &self,
        prefix: S,
        queue_group: Z,
    ) -> Group {
        Group {
            subjects: self.subjects.clone(),
            prefix: prefix.to_string(),
            stats: self.endpoints_state.clone(),
            client: self.client.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            queue_group: queue_group.to_string(),
        }
    }

    /// Builder for customized [Endpoint] creation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.service_builder().start("service", "1.0.0").await?;
    ///
    /// let products = service
    ///     .endpoint_builder()
    ///     .name("api")
    ///     .add("products")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn endpoint_builder(&self) -> EndpointBuilder {
        EndpointBuilder::new(
            self.client.clone(),
            self.endpoints_state.clone(),
            self.shutdown_tx.clone(),
            self.subjects.clone(),
            self.queue_group.clone(),
        )
    }

    /// Adds a new endpoint to the [Service].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.service_builder().start("service", "1.0.0").await?;
    ///
    /// let products = service.endpoint("products").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn endpoint<S: ToString>(&self, subject: S) -> Result<Endpoint, Error> {
        EndpointBuilder::new(
            self.client.clone(),
            self.endpoints_state.clone(),
            self.shutdown_tx.clone(),
            self.subjects.clone(),
            self.queue_group.clone(),
        )
        .add(subject)
        .await
    }
}

pub struct Group {
    prefix: String,
    stats: Arc<Mutex<Endpoints>>,
    client: Client,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    subjects: Arc<Mutex<Vec<String>>>,
    queue_group: String,
}

impl Group {
    /// Creates a group for [Endpoints][Endpoint] under common prefix.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.service_builder().start("service", "1.0.0").await?;
    ///
    /// let v1 = service.group("v1");
    /// let products = v1.endpoint("products").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn group<S: ToString>(&self, prefix: S) -> Group {
        self.group_with_queue_group(prefix, self.queue_group.clone())
    }

    /// Creates a group for [Endpoints][Endpoint] under common prefix with custom queue group.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.service_builder().start("service", "1.0.0").await?;
    ///
    /// let v1 = service.group("v1");
    /// let products = v1.endpoint("products").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn group_with_queue_group<S: ToString, Z: ToString>(
        &self,
        prefix: S,
        queue_group: Z,
    ) -> Group {
        Group {
            prefix: prefix.to_string(),
            stats: self.stats.clone(),
            client: self.client.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            subjects: self.subjects.clone(),
            queue_group: queue_group.to_string(),
        }
    }

    /// Adds a new endpoint to the [Service] under current [Group]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.service_builder().start("service", "1.0.0").await?;
    /// let v1 = service.group("v1");
    ///
    /// let products = v1.endpoint("products").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn endpoint<S: ToString>(&self, subject: S) -> Result<Endpoint, Error> {
        let mut endpoint = EndpointBuilder::new(
            self.client.clone(),
            self.stats.clone(),
            self.shutdown_tx.clone(),
            self.subjects.clone(),
            self.queue_group.clone(),
        );
        endpoint.prefix = Some(self.prefix.clone());
        endpoint.add(subject.to_string()).await
    }

    /// Builder for customized [Endpoint] creation under current [Group]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::service::ServiceExt;
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let mut service = client.service_builder().start("service", "1.0.0").await?;
    /// let v1 = service.group("v1");
    ///
    /// let products = v1.endpoint_builder().name("api").add("products").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn endpoint_builder(&self) -> EndpointBuilder {
        let mut endpoint = EndpointBuilder::new(
            self.client.clone(),
            self.stats.clone(),
            self.shutdown_tx.clone(),
            self.subjects.clone(),
            self.queue_group.clone(),
        );
        endpoint.prefix = Some(self.prefix.clone());
        endpoint
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

/// Request returned by [Service] [Stream][futures::Stream].
#[derive(Debug)]
pub struct Request {
    issued: Instant,
    client: Client,
    pub message: Message,
    endpoint: String,
    stats: Arc<Mutex<Endpoints>>,
}

impl Request {
    /// Sends response for the request.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// use async_nats::service::ServiceExt;
    /// use futures::StreamExt;
    /// # let client = async_nats::connect("demo.nats.io").await?;
    /// # let mut service = client
    /// #    .service_builder().start("serviceA", "1.0.0.1").await?;
    /// let mut endpoint = service.endpoint("endpoint").await?;
    /// let request = endpoint.next().await.unwrap();
    /// request.respond(Ok("hello".into())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn respond(&self, response: Result<Bytes, error::Error>) -> Result<(), PublishError> {
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
                headers.insert(NATS_SERVICE_ERROR, err.status.as_str());
                headers.insert(NATS_SERVICE_ERROR_CODE, err.code.to_string().as_str());
                self.client
                    .publish_with_headers(reply, headers, "".into())
                    .await
            }
        };
        let elapsed = self.issued.elapsed();
        let mut stats = self.stats.lock().unwrap();
        let stats = stats.endpoints.get_mut(self.endpoint.as_str()).unwrap();
        stats.requests += 1;
        stats.processing_time += elapsed;
        stats.average_processing_time = {
            let avg_nanos = (stats.processing_time.as_nanos() / stats.requests as u128) as u64;
            Duration::from_nanos(avg_nanos)
        };
        result
    }
}

#[derive(Debug)]
pub struct EndpointBuilder {
    client: Client,
    stats: Arc<Mutex<Endpoints>>,
    shutdown_tx: Sender<()>,
    name: Option<String>,
    metadata: Option<HashMap<String, String>>,
    subjects: Arc<Mutex<Vec<String>>>,
    queue_group: String,
    prefix: Option<String>,
}

impl EndpointBuilder {
    fn new(
        client: Client,
        stats: Arc<Mutex<Endpoints>>,
        shutdown_tx: Sender<()>,
        subjects: Arc<Mutex<Vec<String>>>,
        queue_group: String,
    ) -> EndpointBuilder {
        EndpointBuilder {
            client,
            stats,
            subjects,
            shutdown_tx,
            name: None,
            metadata: None,
            queue_group,
            prefix: None,
        }
    }

    /// Name of the [Endpoint]. By default subject of the endpoint is used.
    pub fn name<S: ToString>(mut self, name: S) -> EndpointBuilder {
        self.name = Some(name.to_string());
        self
    }

    /// Metadata specific for the [Endpoint].
    pub fn metadata(mut self, metadata: HashMap<String, String>) -> EndpointBuilder {
        self.metadata = Some(metadata);
        self
    }

    /// Custom queue group for the [Endpoint]. Otherwise it will be derived from group or service.
    pub fn queue_group<S: ToString>(mut self, queue_group: S) -> EndpointBuilder {
        self.queue_group = queue_group.to_string();
        self
    }

    /// Finalizes the builder and adds the [Endpoint].
    pub async fn add<S: ToString>(self, subject: S) -> Result<Endpoint, Error> {
        let mut subject = subject.to_string();
        if let Some(prefix) = self.prefix {
            subject = format!("{}.{}", prefix, subject);
        }
        let endpoint_name = self.name.clone().unwrap_or_else(|| subject.clone());
        let name = self
            .name
            .clone()
            .unwrap_or_else(|| subject.clone().replace('.', "-"));
        let requests = self
            .client
            .queue_subscribe(subject.to_owned(), self.queue_group.to_string())
            .await?;
        debug!("created service for endpoint {subject}");

        let shutdown_rx = self.shutdown_tx.subscribe();

        let mut stats = self.stats.lock().unwrap();
        stats
            .endpoints
            .entry(endpoint_name.clone())
            .or_insert(endpoint::Inner {
                name,
                subject: subject.clone(),
                metadata: self.metadata.unwrap_or_default(),
                queue_group: self.queue_group.clone(),
                ..Default::default()
            });
        self.subjects.lock().unwrap().push(subject.clone());
        Ok(Endpoint {
            requests,
            stats: self.stats.clone(),
            client: self.client.clone(),
            endpoint: endpoint_name,
            shutdown: Some(shutdown_rx),
            shutdown_future: None,
        })
    }
}

pub struct StatsHandler(pub Box<dyn FnMut(String, endpoint::Stats) -> serde_json::Value + Send>);

impl std::fmt::Debug for StatsHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stats handler")
    }
}
