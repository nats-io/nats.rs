// Copyright 2020 The NATS Authors
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

//! Experimental `JetStream` support enabled via the `jetstream` feature.
//!
//! # Examples
//!
//! Create a new stream with default options:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::Manager;
//!
//! let nc = nats::connect("my_server::4222")?;
//!
//! let manager = Manager::new(nc);
//!
//! // create_stream converts a str into a
//! // default `StreamConfig`.
//! manager.create_stream("my_stream")?;
//!
//! # Ok(()) }
//! ```
//!
//! Create a new stream with specific options set:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::{Manager, StreamConfig, StorageType};
//!
//! let nc = nats::connect("my_server::4222")?;
//!
//! let manager = Manager::new(nc);
//!
//! manager.create_stream(StreamConfig {
//!     name: "my_memory_stream".to_string(),
//!     max_bytes: 5 * 1024 * 1024 * 1024,
//!     storage: StorageType::Memory,
//!     ..Default::default()
//! })?;
//!
//! # Ok(()) }
//! ```
//!
//! Create and use a new default consumer (defaults to Pull-based, see the docs for `ConsumerConfig` for how this influences behavior)
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::Manager;
//!
//! let nc = nats::connect("my_server::4222")?;
//!
//! let manager = Manager::new(nc);
//!
//! manager.create_stream("my_stream")?;
//!
//! manager.create_consumer("my_stream", "my_consumer")?;
//!
//! # Ok(()) }
//! ```
//!
//! Create and use a new push-based consumer with batched acknowledgements
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::{AckPolicy, ConsumerConfig, Manager};
//!
//! let nc = nats::connect("my_server::4222")?;
//!
//! let manager = Manager::new(nc);
//!
//! manager.create_stream("my_stream")?;
//!
//! manager.create_consumer("my_stream", ConsumerConfig {
//!     durable_name: Some("my_consumer".to_string()),
//!     deliver_subject: Some("my_push_consumer_subject".to_string()),
//!     ack_policy: AckPolicy::All,
//!     ..Default::default()
//! })?;
//!
//! # Ok(()) }
//! ```
//!
//! Consumers can also be created on-the-fly using `Consumer::new`, and later used with
//! `Consumer::existing` if you do not wish to auto-create them.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::{AckPolicy, Consumer, ConsumerConfig};
//!
//! let nc = nats::connect("my_server::4222")?;
//!
//! let consumer_res = Consumer::existing(nc.clone(), "my_stream", "non-existent_consumer");
//!
//! // trying to use this consumer will fail because it hasn't been created yet
//! assert!(consumer_res.is_err());
//!
//! // this will create the consumer if it does not exist already
//! let consumer = Consumer::new(nc, "my_stream", "existing_or_created_consumer")?;
//! # Ok(()) }
//! ```
//!
//! Consumers may be used for processing messages individually, with timeouts, or in batches:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::{AckPolicy, Consumer, ConsumerConfig};
//!
//! let nc = nats::connect("my_server::4222")?;
//!
//! // this will create the consumer if it does not exist already
//! let consumer = Consumer::new(nc, "my_stream", "existing_or_created_consumer")?;
//!
//! // wait indefinitely for the message to arrive
//! let msg_data_len: usize = consumer.process(|msg| {
//!     println!("got message {:?}", msg);
//!     msg.data.len()
//! })?;
//!
//! // wait until the consumer's `timeout` field for the message to arrive.
//! // This can be set manually, and has a very low default of 5ms.
//! let msg_data_len: usize = consumer.process_timeout(|msg| {
//!     println!("got message {:?}", msg);
//!     msg.data.len()
//! })?;
//!
//! // wait indefinitely for the first message in a batch, then process
//! // more messages until the configured timeout is expired
//! let batch_size = 128;
//! let results: Vec<usize> = consumer.process_batch(batch_size, |msg| {
//!     println!("got message {:?}", msg);
//!     msg.data.len()
//! })?;
//!
//! # Ok(()) }
//! ```
//!

use std::{
    collections::VecDeque,
    convert::TryFrom,
    fmt::Debug,
    io::{self, Error, ErrorKind},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub use crate::jetstream_types::*;

use crate::{Connection as NatsClient, Message};

/// `ApiResponse` is a standard response from the `JetStream` JSON Api
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ApiResponse<T> {
    Ok(T),
    Err { r#type: String, error: ApiError },
}

/// `ApiError` is included in all Api responses if there was an error.
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize, Clone)]
struct ApiError {
    code: usize,
    description: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct PagedRequest {
    offset: i64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct PagedResponse<T> {
    pub r#type: String,

    #[serde(alias = "streams", alias = "consumers")]
    pub items: Option<VecDeque<T>>,

    // related to paging
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

/// An iterator over paged `JetStream` API operations.
#[derive(Debug)]
pub struct PagedIterator<'a, T> {
    manager: &'a NatsClient,
    subject: String,
    offset: i64,
    items: VecDeque<T>,
    done: bool,
}

impl<'a, T> std::iter::FusedIterator for PagedIterator<'a, T> where
    T: DeserializeOwned + Debug
{
}

impl<'a, T> Iterator for PagedIterator<'a, T>
where
    T: DeserializeOwned + Debug,
{
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<io::Result<T>> {
        if self.done {
            return None;
        }
        if !self.items.is_empty() {
            return Some(Ok(self.items.pop_front().unwrap()));
        }
        let req = serde_json::ser::to_vec(&PagedRequest {
            offset: self.offset,
        })
        .unwrap();

        let res: io::Result<PagedResponse<T>> =
            self.manager.js_request(&self.subject, &req);

        let mut page = match res {
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
            Ok(page) => page,
        };

        if page.items.is_none() {
            self.done = true;
            return None;
        }

        let items = page.items.take().unwrap();

        self.offset += i64::try_from(items.len()).unwrap();
        self.items = items;

        if self.items.is_empty() {
            self.done = true;
            None
        } else {
            Some(Ok(self.items.pop_front().unwrap()))
        }
    }
}

impl NatsClient {
    /// Create a `JetStream` stream.
    ///
    /// Requires the `jetstream` feature.
    pub fn create_stream<S>(&self, stream_config: S) -> io::Result<StreamInfo>
    where
        StreamConfig: From<S>,
    {
        let cfg: StreamConfig = stream_config.into();
        if cfg.name.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject: String =
            format!("{}STREAM.CREATE.{}", self.api_prefix(), cfg.name);
        let req = serde_json::ser::to_vec(&cfg)?;
        self.js_request(&subject, &req)
    }

    /// Update a `JetStream` stream.
    ///
    /// Requires the `jetstream` feature.
    pub fn update_stream(
        &self,
        stream_config: StreamConfig,
    ) -> io::Result<StreamInfo> {
        let cfg: StreamConfig = stream_config.into();
        if cfg.name.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject: String =
            format!("{}STREAM.UPDATE.{}", self.api_prefix(), cfg.name);
        let req = serde_json::ser::to_vec(&cfg)?;
        self.js_request(&subject, &req)
    }

    /// List all `JetStream` stream names. If you also want stream information,
    /// use the `list_streams` method instead.
    ///
    /// Requires the `jetstream` feature.
    pub fn stream_names(&self) -> PagedIterator<'_, String> {
        PagedIterator {
            subject: format!("{}STREAM.NAMES", self.api_prefix()),
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        }
    }

    /// List all `JetStream` streams.
    ///
    /// Requires the `jetstream` feature.
    pub fn list_streams(&self) -> PagedIterator<'_, StreamInfo> {
        PagedIterator {
            subject: format!("{}STREAM.LIST", self.api_prefix()),
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        }
    }

    /// List `JetStream` consumers for a stream.
    ///
    /// Requires the `jetstream` feature.
    pub fn list_consumers<S>(
        &self,
        stream: S,
    ) -> io::Result<PagedIterator<'_, ConsumerInfo>>
    where
        S: AsRef<str>,
    {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject: String =
            format!("{}CONSUMER.LIST.{}", self.api_prefix(), stream);

        Ok(PagedIterator {
            subject,
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        })
    }

    /// Query `JetStream` stream information.
    ///
    /// Requires the `jetstream` feature.
    pub fn stream_info<S: AsRef<str>>(
        &self,
        stream: S,
    ) -> io::Result<StreamInfo> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject: String =
            format!("{}STREAM.INFO.{}", self.api_prefix(), stream);
        self.js_request(&subject, b"")
    }

    /// Purge `JetStream` stream messages.
    ///
    /// Requires the `jetstream` feature.
    pub fn purge_stream<S: AsRef<str>>(
        &self,
        stream: S,
    ) -> io::Result<PurgeResponse> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject = format!("{}STREAM.PURGE.{}", self.api_prefix(), stream);
        self.js_request(&subject, b"")
    }

    /// Delete message in a `JetStream` stream.
    ///
    /// Requires the `jetstream` feature.
    pub fn delete_message<S: AsRef<str>>(
        &self,
        stream: S,
        sequence_number: u64,
    ) -> io::Result<bool> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }

        let req = serde_json::ser::to_vec(&DeleteRequest {
            seq: sequence_number,
        })
        .unwrap();

        let subject =
            format!("{}STREAM.MSG.DELETE.{}", self.api_prefix(), stream);

        self.js_request::<DeleteResponse>(&subject, &req)
            .map(|dr| dr.success)
    }

    /// Delete `JetStream` stream.
    ///
    /// Requires the `jetstream` feature.
    pub fn delete_stream<S: AsRef<str>>(&self, stream: S) -> io::Result<bool> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }

        let subject = format!("{}STREAM.DELETE.{}", self.api_prefix(), stream);
        self.js_request::<DeleteResponse>(&subject, b"")
            .map(|dr| dr.success)
    }

    /// Create a `JetStream` consumer.
    ///
    /// Requires the `jetstream` feature.
    pub fn create_consumer<S, C>(
        &self,
        stream: S,
        cfg: C,
    ) -> io::Result<Consumer>
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let mut config = ConsumerConfig::from(cfg);
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }

        let subject = if let Some(durable_name) = &config.durable_name {
            if durable_name.is_empty() {
                config.durable_name = None;
                format!("{}CONSUMER.CREATE.{}", self.api_prefix(), stream)
            } else {
                format!(
                    "{}CONSUMER.DURABLE.CREATE.{}.{}",
                    self.api_prefix(),
                    stream,
                    durable_name
                )
            }
        } else {
            format!("{}CONSUMER.CREATE.{}", self.api_prefix(), stream)
        };

        let req = CreateConsumerRequest {
            stream_name: stream.into(),
            config: config.clone(),
        };

        let ser_req = serde_json::ser::to_vec(&req)?;

        let _info: ConsumerInfo = self.js_request(&subject, &ser_req)?;

        Consumer::existing::<&str, ConsumerConfig>(self.clone(), stream, config)
    }

    /// Delete a `JetStream` consumer.
    ///
    /// Requires the `jetstream` feature.
    pub fn delete_consumer<S, C>(
        &self,
        stream: S,
        consumer: C,
    ) -> io::Result<bool>
    where
        S: AsRef<str>,
        C: AsRef<str>,
    {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let consumer = consumer.as_ref();
        if consumer.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the consumer name must not be empty",
            ));
        }

        let subject = format!(
            "{}CONSUMER.DELETE.{}.{}",
            self.api_prefix(),
            stream,
            consumer
        );

        self.js_request::<DeleteResponse>(&subject, b"")
            .map(|dr| dr.success)
    }

    /// Query `JetStream` consumer information.
    ///
    /// Requires the `jetstream` feature.
    pub fn consumer_info<S, C>(
        &self,
        stream: S,
        consumer: C,
    ) -> io::Result<ConsumerInfo>
    where
        S: AsRef<str>,
        C: AsRef<str>,
    {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let consumer: &str = consumer.as_ref();
        let subject: String = format!(
            "{}CONSUMER.INFO.{}.{}",
            self.api_prefix(),
            stream,
            consumer
        );
        self.js_request(&subject, b"")
    }

    /// Query `JetStream` account information.
    ///
    /// Requires the `jetstream` feature.
    pub fn account_info(&self) -> io::Result<AccountInfo> {
        self.js_request(&format!("{}INFO", self.api_prefix()), b"")
    }

    fn js_request<Res>(&self, subject: &str, req: &[u8]) -> io::Result<Res>
    where
        Res: DeserializeOwned,
    {
        let res_msg = self.request(subject, req)?;
        println!("got response: {:?}", std::str::from_utf8(&res_msg.data));
        let res: ApiResponse<Res> = serde_json::de::from_slice(&res_msg.data)?;
        match res {
            ApiResponse::Ok(stream_info) => Ok(stream_info),
            ApiResponse::Err { error, .. } => {
                if let Some(desc) = error.description {
                    Err(Error::new(ErrorKind::Other, desc))
                } else {
                    Err(Error::new(ErrorKind::Other, "unknown"))
                }
            }
        }
    }

    fn api_prefix(&self) -> &str {
        &self.0.client.options.jetstream_prefix
    }
}

/// `JetStream` reliable consumption functionality
pub struct Consumer {
    /// The underlying NATS client
    pub nc: NatsClient,

    /// The stream that this `Consumer` is interested in
    pub stream: String,

    /// The backing configuration for this `Consumer`
    pub cfg: ConsumerConfig,

    /// The backing `Subscription` used if this is a
    /// push-based consumer.
    pub push_subscriber: Option<crate::Subscription>,

    /// The amount of time that is waited before erroring
    /// out during `process` and `process_batch`. Defaults
    /// to 5ms, which is likely to be far too low for
    /// workloads spanning distances..
    pub timeout: std::time::Duration,
}

impl Consumer {
    /// Instantiate a `JetStream` `Consumer` from an existing
    /// `ConsumerInfo` that may have been returned
    /// from the `nats::Connection::list_consumers`
    /// iterator.
    ///
    /// Requires the `jetstream` feature.
    pub fn from_consumer_info(
        ci: ConsumerInfo,
        nc: NatsClient,
    ) -> io::Result<Consumer> {
        Consumer::existing::<String, ConsumerConfig>(
            nc,
            ci.stream_name,
            ci.config,
        )
    }

    /// Instantiate a `JetStream` `Consumer`. Performs a check to see if the consumer
    /// already exists, and creates it if not. If you want to use an existing
    /// `Consumer` without this check and creation, use the `Consumer::existing`
    /// method.
    ///
    /// Requires the `jetstream` feature.
    pub fn create_if_absent<S, C>(
        nc: NatsClient,
        stream: S,
        cfg: C,
    ) -> io::Result<Consumer>
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let stream = stream.as_ref().to_string();
        let cfg = ConsumerConfig::from(cfg);

        if let Some(durable_name) = &cfg.durable_name {
            // attempt to create a durable config if it does not yet exist
            let consumer_info = nc.consumer_info(&stream, durable_name);
            if let Err(e) = consumer_info {
                if e.kind() == std::io::ErrorKind::Other {
                    nc.create_consumer::<&str, &ConsumerConfig>(&stream, &cfg)?;
                }
            }
        } else {
            // ephemeral consumer
            nc.create_consumer::<&str, &ConsumerConfig>(&stream, &cfg)?;
        }

        Consumer::existing::<String, ConsumerConfig>(nc, stream, cfg)
    }

    /// Use an existing `JetStream` `Consumer`
    ///
    /// Requires the `jetstream` feature.
    pub fn existing<S, C>(
        nc: NatsClient,
        stream: S,
        cfg: C,
    ) -> io::Result<Consumer>
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let stream = stream.as_ref().to_string();
        let cfg = ConsumerConfig::from(cfg);

        let push_subscriber =
            if let Some(deliver_subject) = &cfg.deliver_subject {
                Some(nc.subscribe(deliver_subject)?)
            } else {
                None
            };

        Ok(Consumer {
            nc,
            stream,
            cfg,
            push_subscriber,
            timeout: std::time::Duration::from_millis(5),
        })
    }

    /// Process a batch of messages. If `AckPolicy::All` is set,
    /// this will send a single acknowledgement at the end of
    /// the batch.
    ///
    /// This will wait indefinitely for the first message to arrive,
    /// but then for subsequent messages it will time out after the
    /// `Consumer`'s configured `timeout`. If a partial batch is received,
    /// returning the partial set of processed and acknowledged
    /// messages.
    ///
    /// Requires the `jetstream` feature.
    pub fn process_batch<R, F: FnMut(&Message) -> R>(
        &self,
        batch_size: usize,
        mut f: F,
    ) -> io::Result<Vec<R>> {
        if self.cfg.durable_name.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "process and process_batch are only usable from \
                Pull-based Consumers with a durable_name set",
            ));
        }

        let subject = format!(
            "{}CONSUMER.MSG.NEXT.{}.{}",
            self.api_prefix(),
            self.stream,
            self.cfg.durable_name.as_ref().unwrap()
        );

        let mut _sub_opt = None;
        let responses = if let Some(ps) = self.push_subscriber.as_ref() {
            ps
        } else {
            _sub_opt =
                Some(self.nc.request_multi(&subject, batch_size.to_string())?);
            _sub_opt.as_ref().unwrap()
        };

        let mut rets = Vec::with_capacity(batch_size);
        let mut last = None;
        let start = std::time::Instant::now();

        let mut received = 0;
        while let Ok(msg) = responses.next_timeout(if received == 0 {
            // wait "forever" for first message
            std::time::Duration::new(std::u64::MAX >> 2, 0)
        } else {
            self.timeout
                .checked_sub(start.elapsed())
                .unwrap_or_default()
        }) {
            let ret = f(&msg);

            if self.cfg.ack_policy == AckPolicy::Explicit {
                msg.respond(b"")?;
            }

            rets.push(ret);
            last = Some(msg);
            received += 1;
            if received == batch_size {
                break;
            }
        }

        if let Some(msg) = last {
            if self.cfg.ack_policy == AckPolicy::All {
                msg.respond(b"")?
            }
        }

        Ok(rets)
    }

    /// Process and acknowledge a single message, waiting indefinitely
    ///
    /// Requires the `jetstream` feature.
    pub fn process<R, F: Fn(&Message) -> R>(&self, f: F) -> io::Result<R> {
        if self.cfg.durable_name.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "process and process_batch are only usable from \
                Pull-based Consumers with a durable_name set",
            ));
        }

        let subject = format!(
            "{}CONSUMER.MSG.NEXT.{}.{}",
            self.api_prefix(),
            self.stream,
            self.cfg.durable_name.as_ref().unwrap()
        );

        let next = if let Some(ps) = &self.push_subscriber {
            ps.next().unwrap()
        } else {
            self.nc.request(&subject, b"")?
        };
        let ret = f(&next);
        if self.cfg.ack_policy != AckPolicy::None {
            next.respond(b"")?;
        }
        Ok(ret)
    }

    /// Process and acknowledge a single message, waiting up to the `Consumer`'s
    /// configured `timeout` before returning a timeout error.
    ///
    /// Requires the `jetstream` feature.
    pub fn process_timeout<R, F: Fn(&Message) -> R>(
        &self,
        f: F,
    ) -> io::Result<R> {
        if self.cfg.durable_name.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "process and process_batch are only usable from \
                Pull-based Consumers with a durable_name set",
            ));
        }

        let subject = format!(
            "{}CONSUMER.MSG.NEXT.{}.{}",
            self.api_prefix(),
            self.stream,
            self.cfg.durable_name.as_ref().unwrap()
        );

        let next = if let Some(ps) = &self.push_subscriber {
            ps.next_timeout(self.timeout)?
        } else {
            self.nc.request(&subject, b"")?
        };
        let ret = f(&next);
        if self.cfg.ack_policy != AckPolicy::None {
            next.respond(b"")?;
        }
        Ok(ret)
    }

    fn api_prefix(&self) -> &str {
        &self.nc.0.client.options.jetstream_prefix
    }
}
