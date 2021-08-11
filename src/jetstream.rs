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

//! Support for the `JetStream` at-least-once messaging system.
//!
//! # Examples
//!
//! Create a new stream with default options:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let nc = nats::connect("my_server::4222")?;
//!
//! // create_stream converts a str into a
//! // default `StreamConfig`.
//! nc.create_stream("my_stream")?;
//!
//! # Ok(()) }
//! ```
//!
//! Create a new stream with specific options set:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::{StreamConfig, StorageType};
//!
//! let nc = nats::connect("my_server::4222")?;
//!
//! nc.create_stream(StreamConfig {
//!     name: "my_memory_stream".to_string(),
//!     max_bytes: 5 * 1024 * 1024 * 1024,
//!     storage: StorageType::Memory,
//!     ..Default::default()
//! })?;
//!
//! # Ok(()) }
//! ```
//!
//! Create and use a new default consumer (defaults to Pull-based, see the docs for [`ConsumerConfig`](struct.ConsumerConfig.html) for how this influences behavior)
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let nc = nats::connect("my_server::4222")?;
//!
//! nc.create_stream("my_stream")?;
//!
//! let consumer: nats::jetstream::Consumer = nc.create_consumer("my_stream", "my_consumer")?;
//!
//! # Ok(()) }
//! ```
//!
//! Create and use a new push-based consumer with batched acknowledgements
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::{AckPolicy, ConsumerConfig};
//!
//! let nc = nats::connect("my_server::4222")?;
//!
//! nc.create_stream("my_stream")?;
//!
//! let consumer: nats::jetstream::Consumer = nc.create_consumer("my_stream", ConsumerConfig {
//!     durable_name: Some("my_consumer".to_string()),
//!     deliver_subject: Some("my_push_consumer_subject".to_string()),
//!     ack_policy: AckPolicy::All,
//!     ..Default::default()
//! })?;
//!
//! # Ok(()) }
//! ```
//!
//! Consumers can also be created on-the-fly using `Consumer::create_or_open`, and later used with
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
//! let consumer = Consumer::create_or_open(nc, "my_stream", "existing_or_created_consumer")?;
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
//! // this will create the consumer if it does not exist already.
//! // consumer must be mut because the `process*` methods perform
//! // message deduplication using an interval tree, which is
//! // also publicly accessible via the `Consumer`'s `dedupe_window`
//! // field.
//! let mut consumer = Consumer::create_or_open(nc, "my_stream", "existing_or_created_consumer")?;
//!
//! // The `Consumer::process` method executes a closure
//! // on both push- and pull-based consumers, and if
//! // the closure returns `Ok` then the message is acked.
//! // If no message is available, it will wait forever
//! // for one to arrive.
//! let msg_data_len: usize = consumer.process(|msg| {
//!     println!("got message {:?}", msg);
//!     Ok(msg.data.len())
//! })?;
//!
//! // Similar to `Consumer::process` except wait until the
//! // consumer's `timeout` field for the message to arrive.
//! // This can and should be set manually, as it has a low
//! // default of 5ms.
//! let msg_data_len: usize = consumer.process_timeout(|msg| {
//!     println!("got message {:?}", msg);
//!     Ok(msg.data.len())
//! })?;
//!
//! // For consumers operating with `AckPolicy::All`, batch
//! // processing can provide nice throughput optimizations.
//! // `Consumer::process_batch` will wait indefinitely for
//! // the first message in a batch, then process
//! // more messages until the configured timeout is expired.
//! // It will batch acks if running with `AckPolicy::All`.
//! // If there is an error with acking, the last item in the
//! // returned `Vec` will be the io error. Terminates early
//! // without acking if the closure returns an `Err`, which
//! // is included in the final element of the `Vec`. If a
//! // Timeout happens before the batch size is reached, then
//! // there will be no errors included in the response `Vec`.
//! let batch_size = 128;
//! let results: Vec<std::io::Result<usize>> = consumer.process_batch(batch_size, |msg| {
//!     println!("got message {:?}", msg);
//!     Ok(msg.data.len())
//! });
//! let flipped: std::io::Result<Vec<usize>> = results.into_iter().collect();
//! let sizes: Vec<usize> = flipped?;
//!
//! // For lower-level control for use cases that are not
//! // well-served by the high-level process* methods,
//! // there are a number of lower level primitives that
//! // can be used, such as `Consumer::pull` for pull-based
//! // consumers and `Message::ack` for manually acking things:
//! let msg = consumer.pull()?;
//!
//! // --- process message ---
//!
//! // tell the server the message has been processed
//! msg.ack()?;
//!
//! # Ok(()) }
//! ```

use std::{
    collections::VecDeque,
    convert::TryFrom,
    fmt::Debug,
    io::{self, Error, ErrorKind},
    iter::DoubleEndedIterator,
    time::Duration,
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
    pub fn create_stream<S>(&self, stream_config: S) -> io::Result<StreamInfo>
    where
        StreamConfig: From<S>,
    {
        let cfg: StreamConfig = stream_config.into();
        if cfg.name.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String =
            format!("{}STREAM.CREATE.{}", self.api_prefix(), cfg.name);
        let req = serde_json::ser::to_vec(&cfg)?;
        self.js_request(&subject, &req)
    }

    /// Update a `JetStream` stream.
    pub fn update_stream(&self, cfg: &StreamConfig) -> io::Result<StreamInfo> {
        if cfg.name.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
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
                ErrorKind::InvalidInput,
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
    pub fn stream_info<S: AsRef<str>>(
        &self,
        stream: S,
    ) -> io::Result<StreamInfo> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String =
            format!("{}STREAM.INFO.{}", self.api_prefix(), stream);
        self.js_request(&subject, b"")
    }

    /// Purge `JetStream` stream messages.
    pub fn purge_stream<S: AsRef<str>>(
        &self,
        stream: S,
    ) -> io::Result<PurgeResponse> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject = format!("{}STREAM.PURGE.{}", self.api_prefix(), stream);
        self.js_request(&subject, b"")
    }

    /// Delete message in a `JetStream` stream.
    pub fn delete_message<S: AsRef<str>>(
        &self,
        stream: S,
        sequence_number: u64,
    ) -> io::Result<bool> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
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
    pub fn delete_stream<S: AsRef<str>>(&self, stream: S) -> io::Result<bool> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let subject = format!("{}STREAM.DELETE.{}", self.api_prefix(), stream);
        self.js_request::<DeleteResponse>(&subject, b"")
            .map(|dr| dr.success)
    }

    /// Create a `JetStream` consumer.
    pub fn create_consumer<S, C>(
        &self,
        stream: S,
        cfg: C,
    ) -> io::Result<Consumer>
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let config = ConsumerConfig::from(cfg);
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let subject = if let Some(ref durable_name) = config.durable_name {
            format!(
                "{}CONSUMER.DURABLE.CREATE.{}.{}",
                self.api_prefix(),
                stream,
                durable_name
            )
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
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let consumer = consumer.as_ref();
        if consumer.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
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
                ErrorKind::InvalidInput,
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
    pub fn account_info(&self) -> io::Result<AccountInfo> {
        self.js_request(&format!("{}INFO", self.api_prefix()), b"")
    }

    fn js_request<Res>(&self, subject: &str, req: &[u8]) -> io::Result<Res>
    where
        Res: DeserializeOwned,
    {
        let res_msg = self.request(subject, req)?;
        let res: ApiResponse<Res> = serde_json::de::from_slice(&res_msg.data)?;
        match res {
            ApiResponse::Ok(stream_info) => Ok(stream_info),
            ApiResponse::Err { error, .. } => {
                log::error!(
                    "failed to parse API response: {:?}",
                    std::str::from_utf8(&res_msg.data)
                );
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

/// `JetStream` reliable consumption functionality.
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
    /// workloads crossing physical sites.
    pub timeout: Duration,

    /// Contains ranges of processed messages that will be
    /// filtered out upon future receipt.
    dedupe_window: IntervalTree,
}

impl Consumer {
    /// Instantiate a `JetStream` `Consumer` from an existing
    /// `ConsumerInfo` that may have been returned
    /// from the `nats::Connection::list_consumers`
    /// iterator.
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
    pub fn create_or_open<S, C>(
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

        if let Some(ref durable_name) = cfg.durable_name {
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
            if let Some(ref deliver_subject) = cfg.deliver_subject {
                Some(nc.subscribe(deliver_subject)?)
            } else {
                None
            };

        let mut dedupe_window = IntervalTree::default();

        // JetStream starts indexing at 1
        dedupe_window.mark_processed(0);

        Ok(Consumer {
            nc,
            stream,
            cfg,
            push_subscriber,
            timeout: Duration::from_millis(5),
            dedupe_window,
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
    /// If the closure returns `Err`, the batch processing will stop,
    /// and the returned vector will contain this error as the final
    /// element. The message that caused this error will not be acknowledged
    /// to the `JetStream` server, but all previous messages will be.
    /// If an error is encountered while subscribing or acking messages
    /// that may have returned `Ok` from the closure, that Ok will be
    /// present in the returned vector but the last item in the vector
    /// will be the encountered error. If the consumer's timeout expires
    /// before the entire batch is processed, there will be no error
    /// pushed to the returned `Vec`, it will just be shorter than the
    /// specified batch size.
    ///
    /// All messages are deduplicated using the `Consumer`'s built-in
    /// `dedupe_window` before being fed to the provided closure. If
    /// a message that has already been processed is received, it will
    /// be acked and skipped. Errors for acking deduplicated messages
    /// are not included in the returned `Vec`.
    pub fn process_batch<R, F: FnMut(&Message) -> io::Result<R>>(
        &mut self,
        batch_size: usize,
        mut f: F,
    ) -> Vec<io::Result<R>> {
        let mut _sub_opt = None;
        let responses = if let Some(ps) = self.push_subscriber.as_ref() {
            ps
        } else {
            if self.cfg.durable_name.is_none() {
                return vec![Err(Error::new(
                    ErrorKind::InvalidInput,
                    "process and process_batch are only usable from \
                    Pull-based Consumers if there is a durable_name set",
                ))];
            }

            let subject = format!(
                "{}CONSUMER.MSG.NEXT.{}.{}",
                self.api_prefix(),
                self.stream,
                self.cfg.durable_name.as_ref().unwrap()
            );

            let sub =
                match self.nc.request_multi(&subject, batch_size.to_string()) {
                    Ok(sub) => sub,
                    Err(e) => return vec![Err(e)],
                };
            _sub_opt = Some(sub);
            _sub_opt.as_ref().unwrap()
        };

        let mut rets = Vec::with_capacity(batch_size);
        let mut last = None;
        let start = std::time::Instant::now();

        let mut received = 0;

        while let Some(next) = {
            if received == 0 {
                responses.next()
            } else {
                let timeout = self
                    .timeout
                    .checked_sub(start.elapsed())
                    .unwrap_or_default();
                responses.next_timeout(timeout).ok()
            }
        } {
            let next_id = next.jetstream_message_info().unwrap().stream_seq;

            if self.dedupe_window.already_processed(next_id) {
                let _dont_care_about_success = next.ack();
                continue;
            }

            let ret = f(&next);

            let is_err = ret.is_err();
            rets.push(ret);

            if is_err {
                // we will still try to ack all messages before this one
                // if our ack policy is `All`, after breaking.
                break;
            } else if self.cfg.ack_policy == AckPolicy::Explicit {
                self.dedupe_window.mark_processed(next_id);
                let res = next.ack();
                if let Err(e) = res {
                    rets.push(Err(e));
                }
            }

            last = Some(next);
            received += 1;
            if received == batch_size {
                break;
            }
        }

        if let Some(last) = last {
            if self.cfg.ack_policy == AckPolicy::All {
                let res = last.ack();
                if let Err(e) = res {
                    rets.push(Err(e));
                }
            }
        }

        rets
    }

    /// Process and acknowledge a single message, waiting indefinitely for
    /// one to arrive.
    ///
    /// Does not ack the processed message if the internal closure returns an `Err`.
    ///
    /// All messages are deduplicated using the `Consumer`'s built-in
    /// `dedupe_window` before being fed to the provided closure. If
    /// a message that has already been processed is received, it will
    /// be acked and skipped.
    ///
    /// Does not return an `Err` if acking the message is unsuccessful,
    /// but the message is still marked in the dedupe window. If you
    /// require stronger processing guarantees, you can manually call
    /// the `double_ack` method of the argument message. If you require
    /// both the returned `Ok` from the closure and the `Err` from a
    /// failed ack, use `process_batch` instead.
    pub fn process<R, F: Fn(&Message) -> io::Result<R>>(
        &mut self,
        f: F,
    ) -> io::Result<R> {
        loop {
            let next = if let Some(ps) = &self.push_subscriber {
                ps.next().unwrap()
            } else {
                if self.cfg.durable_name.is_none() {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "process and process_batch are only usable from \
                        Pull-based Consumers if there is a durable_name set",
                    ));
                }

                let subject = format!(
                    "{}CONSUMER.MSG.NEXT.{}.{}",
                    self.api_prefix(),
                    self.stream,
                    self.cfg.durable_name.as_ref().unwrap()
                );

                self.nc.request(&subject, AckKind::Ack)?
            };

            let next_id = if let Some(jmi) = next.jetstream_message_info() {
                jmi.stream_seq
            } else {
                return Err(Error::new(
                    ErrorKind::Other,
                    "failed to process jetstream message info \
                    from message reply subject. Is your nats-server up to date?"
                ));
            };

            if self.dedupe_window.already_processed(next_id) {
                let _dont_care = next.ack();
                continue;
            }

            let ret = f(&next)?;
            if self.cfg.ack_policy != AckPolicy::None {
                let _dont_care = next.ack();
            }

            self.dedupe_window.mark_processed(next_id);
            return Ok(ret);
        }
    }

    /// Process and acknowledge a single message, waiting up to the `Consumer`'s
    /// configured `timeout` before returning a timeout error.
    ///
    /// Does not ack the processed message if the internal closure returns an `Err`.
    ///
    /// All messages are deduplicated using the `Consumer`'s built-in
    /// `dedupe_window` before being fed to the provided closure. If
    /// a message that has already been processed is received, it will
    /// be acked and skipped.
    ///
    /// Does not return an `Err` if acking the message is unsuccessful,
    /// but the message is still marked in the dedupe window. If you
    /// require stronger processing guarantees, you can manually call
    /// the `double_ack` method of the argument message. If you require
    /// both the returned `Ok` from the closure and the `Err` from a
    /// failed ack, use `process_batch` instead.
    pub fn process_timeout<R, F: Fn(&Message) -> io::Result<R>>(
        &mut self,
        f: F,
    ) -> io::Result<R> {
        loop {
            let next = if let Some(ps) = &self.push_subscriber {
                ps.next_timeout(self.timeout)?
            } else {
                if self.cfg.durable_name.is_none() {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "process and process_batch are only usable from \
                        Pull-based Consumers if there is a a durable_name set",
                    ));
                }

                let subject = format!(
                    "{}CONSUMER.MSG.NEXT.{}.{}",
                    self.api_prefix(),
                    self.stream,
                    self.cfg.durable_name.as_ref().unwrap()
                );

                self.nc.request_timeout(&subject, b"", self.timeout)?
            };

            let next_id = next.jetstream_message_info().unwrap().stream_seq;

            if self.dedupe_window.already_processed(next_id) {
                self.dedupe_window.mark_processed(next_id);
                let _dont_care = next.ack();
                continue;
            }

            let ret = f(&next)?;
            if self.cfg.ack_policy != AckPolicy::None {
                let _dont_care = next.ack();
            }
            return Ok(ret);
        }
    }

    /// For pull-based consumers (a consumer where `ConsumerConfig.deliver_subject` is `None`)
    /// this can be used to request a single message, and wait forever for a response.
    /// If you require specifying the batch size or using a timeout while consuming the
    /// responses, use the `pull_opt` method below.
    ///
    /// This is a lower-level method and does not filter messages through the `Consumer`'s
    /// built-in `dedupe_window` as the various `process*` methods do.
    pub fn pull(&mut self) -> io::Result<Message> {
        let ret_opt = self
            .pull_opt(NextRequest {
                batch: 1,
                ..Default::default()
            })?
            .next();

        if let Some(ret) = ret_opt {
            Ok(ret)
        } else {
            Err(Error::new(
                ErrorKind::BrokenPipe,
                "The nats client is shutting down.",
            ))
        }
    }

    /// For pull-based consumers (a consumer where `ConsumerConfig.deliver_subject` is `None`)
    /// this can be used to request a configurable number of messages, as well as specify
    /// how the server will keep track of this batch request over time. See the docs for
    /// `NextRequest` for more information about the options.
    ///
    /// This is a lower-level method and does not filter messages through the `Consumer`'s
    /// built-in `dedupe_window` as the various `process*` methods do.
    pub fn pull_opt(
        &mut self,
        next_request: NextRequest,
    ) -> io::Result<crate::Subscription> {
        if self.cfg.durable_name.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "this method is only usable from \
                Pull-based Consumers with a durable_name set",
            ));
        }

        if self.cfg.deliver_subject.is_some() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "this method is only usable from \
                Pull-based Consumers with a deliver_subject set to None",
            ));
        }

        let subject = format!(
            "{}CONSUMER.MSG.NEXT.{}.{}",
            self.api_prefix(),
            self.stream,
            self.cfg.durable_name.as_ref().unwrap()
        );

        let req = serde_json::ser::to_vec(&next_request).unwrap();
        self.nc.request_multi(&subject, &req)
    }

    fn api_prefix(&self) -> &str {
        &self.nc.0.client.options.jetstream_prefix
    }
}

/// Records ranges of acknowledged IDs for
/// low-memory deduplication.
#[derive(Default)]
struct IntervalTree {
    // stores interval start-end
    inner: std::collections::BTreeMap<u64, u64>,
}

impl IntervalTree {
    /// Mark this ID as being processed. Returns `true`
    /// if this ID was not already marked as processed.
    pub fn mark_processed(&mut self, id: u64) -> bool {
        if self.inner.is_empty() {
            self.inner.insert(id, id);
            return true;
        }

        let (prev_start, prev_end) = self
            .inner
            .range(..=&id)
            .next_back()
            .map(|(s, e)| (*s, *e))
            .unwrap();

        if (prev_start..=prev_end).contains(&id) {
            // range already includes id
            return false;
        }

        // we may have to merge one or two ranges.
        //
        // say we're inserting 4:
        //
        // case 1, fully disjoint:
        // [0] [4] [6]
        //
        // case 2, left merge
        // [0, 1, 2, 3, 4] [6]
        //
        // case 3, right merge
        // [0] [4, 5, 6]
        //
        // case 4, double merge
        // [3, 4, 5]

        let left_merge = prev_end + 1 == id;
        let right_merge = self.inner.contains_key(&(id + 1));

        match (left_merge, right_merge) {
            (true, true) => {
                let right_end = self.inner.remove(&(id + 1)).unwrap();
                assert_eq!(
                    self.inner.insert(prev_start, right_end),
                    Some(id - 1)
                );
            }
            (true, false) => {
                assert_eq!(self.inner.insert(prev_start, id), Some(id - 1));
            }
            (false, true) => {
                let right_end = self.inner.remove(&(id + 1)).unwrap();
                assert_eq!(self.inner.insert(id, right_end), None);
            }
            (false, false) => {
                // created disjoint range
                self.inner.insert(id, id);
            }
        }

        true
    }

    /// Returns `true` if this ID has already been processed.
    pub fn already_processed(&self, id: u64) -> bool {
        if let Some((prev_start, prev_end)) =
            self.inner.range(..=&id).next_back()
        {
            (prev_start..=prev_end).contains(&&id)
        } else {
            false
        }
    }
}
