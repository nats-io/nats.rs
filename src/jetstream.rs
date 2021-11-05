// Copyright 2020-2021 The NATS Authors
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
//! let js = nats::jetstream::new(nc);
//!
//! // add_stream converts a str into a
//! // default `StreamConfig`.
//! js.add_stream("my_stream")?;
//!
//! # Ok(()) }
//! ```
//!
//! Add a new stream with specific options set:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::{StreamConfig, StorageType};
//!
//! let nc = nats::connect("my_server::4222")?;
//! let js = nats::jetstream::new(nc);
//!
//! js.add_stream(StreamConfig {
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
//! let js = nats::jetstream::new(nc);
//!
//! js.add_stream("my_stream")?;
//!
//! let consumer: nats::jetstream::Consumer = js.add_consumer("my_stream", "my_consumer")?;
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
//! let js = nats::jetstream::new(nc);
//!
//! js.add_stream("my_stream")?;
//!
//! let consumer: nats::jetstream::Consumer = js.add_consumer("my_stream", ConsumerConfig {
//!     durable_name: Some("my_consumer".to_string()),
//!     deliver_subject: Some("my_push_consumer_subject".to_string()),
//!     ack_policy: AckPolicy::All,
//!     ..Default::default()
//! })?;
//!
//! # Ok(()) }
//! ```
//!
//! Consumers can also be created on-the-fly using `JetStream`'s `create_or_bind`, and later used with
//! `existing` if you do not wish to auto-create them.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use nats::jetstream::{AckPolicy, Consumer, ConsumerConfig};
//!
//! let nc = nats::connect("my_server::4222")?;
//! let js = nats::jetstream::new(nc);
//!
//! let consumer_res = js.existing("my_stream", "non-existent_consumer");
//!
//! // trying to use this consumer will fail because it hasn't been created yet
//! assert!(consumer_res.is_err());
//!
//! // this will create the consumer if it does not exist already
//! let consumer = js.create_or_bind("my_stream", "existing_or_created_consumer")?;
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
//! let js = nats::jetstream::new(nc);
//!
//! // this will create the consumer if it does not exist already.
//! let mut consumer = js.create_or_bind("my_stream", "existing_or_created_consumer")?;
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
    error, fmt,
    fmt::Debug,
    io::{self, ErrorKind},
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

pub use crate::jetstream_types::*;

use crate::{Connection, Message};

/// `JetStream` options
#[derive(Clone)]
pub struct JetStreamOptions {
    pub(crate) api_prefix: String,
}

impl Debug for JetStreamOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(&"api_prefix", &self.api_prefix)
            .finish()
    }
}

impl Default for JetStreamOptions {
    fn default() -> JetStreamOptions {
        JetStreamOptions {
            api_prefix: "$JS.API.".to_string(),
        }
    }
}

impl JetStreamOptions {
    /// `Options` for `JetStream` operations.
    ///
    /// # Example
    ///
    /// ```
    /// let options = nats::JetStreamOptions::new();
    /// ```
    pub fn new() -> JetStreamOptions {
        JetStreamOptions::default()
    }

    /// Set a custom `JetStream` API prefix.
    ///
    /// # Example
    ///
    /// ```
    /// let options = nats::JetStreamOptions::new()
    ///     .api_prefix("some_exported_prefix".to_string());
    /// ```
    pub fn api_prefix(mut self, mut api_prefix: String) -> Self {
        if !api_prefix.ends_with('.') {
            api_prefix.push('.');
        }

        self.api_prefix = api_prefix;
        self
    }

    /// Set a custom `JetStream` API prefix from a domain.
    ///
    /// # Example
    ///
    /// ```
    /// let options = nats::JetStreamOptions::new()
    ///   .domain("some_domain");
    /// ```
    pub fn domain(self, domain: &str) -> Self {
        if domain.is_empty() {
            self.api_prefix("".to_string())
        } else {
            self.api_prefix(format!("$JS.{}.API", domain))
        }
    }
}

/// `ApiResponse` is a standard response from the `JetStream` JSON Api
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ApiResponse<T> {
    // Note:
    // Serde will try to match the data against each variant in order and the first one that
    // deserializes successfully is the one returned.
    //
    // Therefore the error case must come first, otherwise it can be ignored.
    Err { r#type: String, error: Error },
    Ok(T),
}

/// `ErrorCode` which can be returned from a server an a response when an error occurs.
#[derive(Debug, PartialEq, Eq, Serialize_repr, Deserialize_repr, Clone, Copy)]
#[repr(u64)]
pub enum ErrorCode {
    /// Peer not a member
    ClusterPeerNotMember = 10040,
    /// Consumer expected to be ephemeral but detected a durable name set in subject
    ConsumerEphemeralWithDurableInSubject = 10019,
    /// Stream external delivery prefix {prefix} overlaps with stream subject {subject}
    StreamExternalDelPrefixOverlaps = 10022,
    /// Resource limits exceeded for account
    AccountResourcesExceeded = 10002,
    /// JetStream system temporarily unavailable
    ClusterNotAvail = 10008,
    /// Subjects overlap with an existing stream
    StreamSubjectOverlap = 10065,
    /// Wrong last sequence: {seq}
    StreamWrongLastSequence = 10071,
    /// Template name in subject does not match request
    TemplateNameNotMatchSubject = 10073,
    /// No suitable peers for placement
    ClusterNoPeers = 10005,
    /// Consumer expected to be ephemeral but a durable name was set in request
    ConsumerEphemeralWithDurableName = 10020,
    /// Insufficient resources
    InsufficientResources = 10023,
    /// Stream mirror must have max message size >= source
    MirrorMaxMessageSizeTooBig = 10030,
    /// Generic stream template deletion failed error string
    StreamTemplateDelete = 10067,
    /// Bad request
    BadRequest = 10003,
    /// Not currently supported in clustered mode
    ClusterUnSupportFeature = 10036,
    /// Consumer not found
    ConsumerNotFound = 10014,
    /// Stream source must have max message size >= target
    SourceMaxMessageSizeTooBig = 10046,
    /// Generic stream assignment error string
    StreamAssignment = 10048,
    /// Message size exceeds maximum allowed
    StreamMessageExceedsMaximum = 10054,
    /// Generic template creation failed string
    StreamTemplateCreate = 10066,
    /// Invalid JSON
    InvalidJSON = 10025,
    /// Stream external delivery prefix {prefix} must not contain wildcards
    StreamInvalidExternalDeliverySubject = 10024,
    /// Restore failed: {err}
    StreamRestore = 10062,
    /// Incomplete results
    ClusterIncomplete = 10004,
    /// Account not found
    NoAccount = 10035,
    /// General RAFT error string
    RaftGeneral = 10041,
    /// JetStream unable to subscribe to restore snapshot {subject}: {err}
    RestoreSubscribeFailed = 10042,
    /// General stream deletion error string
    StreamDelete = 10050,
    /// Stream external api prefix {prefix} must not overlap with {subject}
    StreamExternalApiOverlap = 10021,
    /// Stream mirrors can not also contain subjects
    MirrorWithSubjects = 10034,
    /// JetStream not enabled
    NotEnabled = 10076,
    /// JetStream not enabled for account
    NotEnabledForAccount = 10039,
    /// Sequence {seq} not found
    SequenceNotFound = 10043,
    /// Mirror configuration can not be updated
    StreamMirrorNotUpdatable = 10055,
    /// Expected stream sequence does not match
    StreamSequenceNotMatch = 10063,
    /// Wrong last msg Id: {id}
    StreamWrongLastMsgId = 10070,
    /// JetStream unable to open temp storage for restore
    TempStorageFailed = 10072,
    /// Insufficient storage resources available
    StorageResourcesExceeded = 10047,
    /// Stream name in subject does not match request
    StreamMismatch = 10056,
    /// Expected stream does not match
    StreamNotMatch = 10060,
    /// Generic mirror consumer setup failure string
    MirrorConsumerSetupFailed = 10029,
    /// Expected an empty request payload
    NotEmptyRequest = 10038,
    /// Stream name already in use
    StreamNameExist = 10058,
    /// Tags placement not supported for operation
    ClusterTags = 10011,
    /// Maximum consumers limit reached
    MaximumConsumersLimit = 10026,
    /// General source consumer setup failure string
    SourceConsumerSetupFailed = 10045,
    /// General consumer creation failure string
    ConsumerCreate = 10012,
    /// Consumer expected to be durable but no durable name set in subject
    ConsumerDurableNameNotInSubject = 10016,
    /// General stream limits exceeded error string
    StreamLimits = 10053,
    /// Replicas configuration can not be updated
    StreamReplicasNotUpdatable = 10061,
    /// Template not found
    StreamTemplateNotFound = 10068,
    /// JetStream cluster not assigned to this server
    ClusterNotAssigned = 10007,
    /// JetStream cluster can not handle request
    ClusterNotLeader = 10009,
    /// Consumer name already in use
    ConsumerNameExist = 10013,
    /// Stream mirrors can not also contain other sources
    MirrorWithSources = 10031,
    /// Stream not found
    StreamNotFound = 10059,
    /// JetStream clustering support required
    ClusterRequired = 10010,
    /// Consumer expected to be durable but a durable name was not set
    ConsumerDurableNameNotSet = 10018,
    /// Maximum number of streams reached
    MaximumStreamsLimit = 10027,
    /// Stream mirrors can not have both start seq and start time configured
    MirrorWithStartSeqAndTime = 10032,
    /// Snapshot failed: {err}
    StreamSnapshot = 10064,
    /// Generic stream update error string
    StreamUpdate = 10069,
    /// JetStream not in clustered mode
    ClusterNotActive = 10006,
    /// Consumer name in subject does not match durable name in request
    ConsumerDurableNameNotMatchSubject = 10017,
    /// Insufficient memory resources available
    MemoryResourcesExceeded = 10028,
    /// Stream mirrors can not contain filtered subjects
    MirrorWithSubjectFilters = 10033,
    /// Generic stream creation error string
    StreamCreate = 10049,
    /// Server is not a member of the cluster
    ClusterServerNotMember = 10044,
    /// No message found
    NoMessageFound = 10037,
    /// Deliver subject not valid
    SnapshotDeliverSubjectInvalid = 10015,
    /// General stream failure string
    StreamGeneralErrorF = 10051,
    /// Stream configuration validation error string
    StreamInvalidConfigF = 10052,
    /// Replicas > 1 not supported in non-clustered mode
    StreamReplicasNotSupported = 10074,
    /// Generic message deletion failure error string
    StreamMsgDeleteFailedF = 10057,
    /// Peer remap failed
    PeerRemap = 10075,
    /// Generic error when storing a message failed
    StreamStoreFailedF = 10077,
    /// Consumer config required
    ConsumerConfigRequired = 10078,
    /// Consumer deliver subject has wildcards
    ConsumerDeliverToWildcards = 10079,
    /// Consumer in push mode can not set max waiting
    ConsumerPushMaxWaiting = 10080,
    /// Consumer deliver subject forms a cycle
    ConsumerDeliverCycle = 10081,
    /// Consumer requires ack policy for max ack pending
    ConsumerMaxPendingAckPolicyRequired = 10082,
    /// Consumer idle heartbeat needs to be >= 100ms
    ConsumerSmallHeartbeat = 10083,
    /// Consumer in pull mode requires explicit ack policy
    ConsumerPullRequiresAck = 10084,
    /// Consumer in pull mode requires a durable name
    ConsumerPullNotDurable = 10085,
    /// Consumer in pull mode can not have rate limit set
    ConsumerPullWithRateLimit = 10086,
    /// Consumer max waiting needs to be positive
    ConsumerMaxWaitingNegative = 10087,
    /// Consumer idle heartbeat requires a push based consumer
    ConsumerHBRequiresPush = 10088,
    /// Consumer flow control requires a push based consumer
    ConsumerFCRequiresPush = 10089,
    /// Consumer direct requires a push based consumer
    ConsumerDirectRequiresPush = 10090,
    /// Consumer direct requires an ephemeral consumer
    ConsumerDirectRequiresEphemeral = 10091,
    /// Consumer direct on a mapped consumer
    ConsumerOnMapped = 10092,
    /// Consumer filter subject is not a valid subset of the interest subjects
    ConsumerFilterNotSubset = 10093,
    /// Generic delivery policy error
    ConsumerInvalidPolicy = 10094,
    /// Failed to parse consumer sampling configuration: {err}
    ConsumerInvalidSampling = 10095,
    /// Stream not valid
    StreamInvalid = 10096,
    /// Workqueue stream requires explicit ack
    ConsumerWQRequiresExplicitAck = 10098,
    /// Multiple non-filtered consumers not allowed on workqueue stream
    ConsumerWQMultipleUnfiltered = 10099,
    /// Filtered consumer not unique on workqueue stream
    ConsumerWQConsumerNotUnique = 10100,
    /// Consumer must be deliver all on workqueue stream
    ConsumerWQConsumerNotDeliverAll = 10101,
    /// Consumer name is too long, maximum allowed is {max}
    ConsumerNameTooLong = 10102,
    /// Durable name can not contain '.', '*', '>'
    ConsumerBadDurableName = 10103,
    /// Error creating store for consumer: {err}
    ConsumerStoreFailed = 10104,
    /// Consumer already exists and is still active
    ConsumerExistingActive = 10105,
    /// Consumer replacement durable config not the same
    ConsumerReplacementWithDifferentName = 10106,
    /// Consumer description is too long, maximum allowed is {max}
    ConsumerDescriptionTooLong = 10107,
    /// Header size exceeds maximum allowed of 64k
    StreamHeaderExceedsMaximum = 10097,
}

/// `Error` type returned from an API response when an error occurs.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Error {
    code: usize,
    err_code: ErrorCode,
    description: Option<String>,
}

impl Error {
    /// Returns the status code assosciated with this error
    pub fn code(&self) -> usize {
        self.code
    }

    /// Returns the server side error code associated with this error.
    pub fn error_code(&self) -> ErrorCode {
        self.err_code
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "{} (code {}, error code {})",
            self.code,
            self.description.as_ref().unwrap_or(&"unknown".to_string()),
            self.err_code as u64,
        )
    }
}

impl error::Error for Error {}

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
    manager: &'a JetStream,
    subject: String,
    offset: i64,
    items: VecDeque<T>,
    done: bool,
}

impl<'a, T> std::iter::FusedIterator for PagedIterator<'a, T> where T: DeserializeOwned + Debug {}

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

        let res: io::Result<PagedResponse<T>> = self.manager.js_request(&self.subject, &req);

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

/// A context for performing `JetStream` operations.
#[derive(Clone, Debug)]
pub struct JetStream {
    nc: Connection,
    options: JetStreamOptions,
}

impl JetStream {
    /// Create a new `JetStream` context.
    pub fn new(nc: Connection, options: JetStreamOptions) -> Self {
        Self { nc, options }
    }

    /// Adds new stream to `JetStream`.
    pub fn add_stream<S>(&self, stream_config: S) -> io::Result<StreamInfo>
    where
        StreamConfig: From<S>,
    {
        let cfg: StreamConfig = stream_config.into();
        if cfg.name.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("{}STREAM.CREATE.{}", self.api_prefix(), cfg.name);
        let req = serde_json::ser::to_vec(&cfg)?;
        self.js_request(&subject, &req)
    }

    /// Update a `JetStream` stream.
    pub fn update_stream(&self, cfg: &StreamConfig) -> io::Result<StreamInfo> {
        if cfg.name.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("{}STREAM.UPDATE.{}", self.api_prefix(), cfg.name);
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
    pub fn list_consumers<S>(&self, stream: S) -> io::Result<PagedIterator<'_, ConsumerInfo>>
    where
        S: AsRef<str>,
    {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("{}CONSUMER.LIST.{}", self.api_prefix(), stream);

        Ok(PagedIterator {
            subject,
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        })
    }

    /// Query `JetStream` stream information.
    pub fn stream_info<S: AsRef<str>>(&self, stream: S) -> io::Result<StreamInfo> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("{}STREAM.INFO.{}", self.api_prefix(), stream);
        self.js_request(&subject, b"")
    }

    /// Purge `JetStream` stream messages.
    pub fn purge_stream<S: AsRef<str>>(&self, stream: S) -> io::Result<PurgeResponse> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
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
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let req = serde_json::ser::to_vec(&DeleteRequest {
            seq: sequence_number,
        })
        .unwrap();

        let subject = format!("{}STREAM.MSG.DELETE.{}", self.api_prefix(), stream);

        self.js_request::<DeleteResponse>(&subject, &req)
            .map(|dr| dr.success)
    }

    /// Delete `JetStream` stream.
    pub fn delete_stream<S: AsRef<str>>(&self, stream: S) -> io::Result<bool> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }

        let subject = format!("{}STREAM.DELETE.{}", self.api_prefix(), stream);
        self.js_request::<DeleteResponse>(&subject, b"")
            .map(|dr| dr.success)
    }

    /// Instantiate a `JetStream` `Consumer` from an existing
    /// `ConsumerInfo` that may have been returned
    /// from the `nats::Connection::list_consumers`
    /// iterator.
    pub fn from_consumer_info(&self, ci: ConsumerInfo) -> io::Result<Consumer> {
        self.existing::<String, ConsumerConfig>(ci.stream_name, ci.config)
    }

    /// Use an existing `JetStream` `Consumer`
    pub fn existing<S, C>(&self, stream: S, cfg: C) -> io::Result<Consumer>
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let stream = stream.as_ref().to_string();
        let cfg = ConsumerConfig::from(cfg);

        let push_subscriber = if let Some(ref deliver_subject) = cfg.deliver_subject {
            Some(self.nc.subscribe(deliver_subject)?)
        } else {
            None
        };

        Ok(Consumer {
            js: self.to_owned(),
            stream,
            cfg,
            push_subscriber,
            timeout: Duration::from_millis(5),
        })
    }

    /// Create a `JetStream` consumer.
    pub fn add_consumer<S, C>(&self, stream: S, cfg: C) -> io::Result<Consumer>
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let config = ConsumerConfig::from(cfg);
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
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

        self.existing::<&str, ConsumerConfig>(stream, config)
    }

    /// Instantiate a `JetStream` `Consumer`. Performs a check to see if the consumer
    /// already exists, and creates it if not. If you want to use an existing
    /// `Consumer` without this check and creation, use the `existing`
    /// method.
    pub fn create_or_bind<S, C>(&self, stream: S, cfg: C) -> io::Result<Consumer>
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let stream = stream.as_ref().to_string();
        let cfg = ConsumerConfig::from(cfg);

        if let Some(ref durable_name) = cfg.durable_name {
            // attempt to create a durable config if it does not yet exist
            let consumer_info = self.consumer_info(&stream, durable_name);
            if let Err(e) = consumer_info {
                if e.kind() == std::io::ErrorKind::Other {
                    self.add_consumer::<&str, &ConsumerConfig>(&stream, &cfg)?;
                }
            }
        } else {
            // ephemeral consumer
            self.add_consumer::<&str, &ConsumerConfig>(&stream, &cfg)?;
        }

        self.existing::<String, ConsumerConfig>(stream, cfg)
    }

    /// Delete a `JetStream` consumer.
    pub fn delete_consumer<S, C>(&self, stream: S, consumer: C) -> io::Result<bool>
    where
        S: AsRef<str>,
        C: AsRef<str>,
    {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let consumer = consumer.as_ref();
        if consumer.is_empty() {
            return Err(io::Error::new(
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
    pub fn consumer_info<S, C>(&self, stream: S, consumer: C) -> io::Result<ConsumerInfo>
    where
        S: AsRef<str>,
        C: AsRef<str>,
    {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "the stream name must not be empty",
            ));
        }
        let consumer: &str = consumer.as_ref();
        let subject: String = format!("{}CONSUMER.INFO.{}.{}", self.api_prefix(), stream, consumer);
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
        let res_msg = self.nc.request(subject, req)?;
        let res: ApiResponse<Res> = serde_json::de::from_slice(&res_msg.data)?;
        match res {
            ApiResponse::Ok(stream_info) => Ok(stream_info),
            ApiResponse::Err { error, .. } => {
                log::error!(
                    "failed to parse API response: {:?}",
                    std::str::from_utf8(&res_msg.data)
                );

                Err(io::Error::new(io::ErrorKind::Other, error))
            }
        }
    }

    fn api_prefix(&self) -> &str {
        &self.options.api_prefix
    }
}

/// `JetStream` reliable consumption functionality.
pub struct Consumer {
    /// The underlying NATS client
    pub js: JetStream,

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
}

impl Consumer {
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
                return vec![Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "process and process_batch are only usable from \
                    Pull-based Consumers if there is a durable_name set",
                ))];
            }

            let subject = format!(
                "{}CONSUMER.MSG.NEXT.{}.{}",
                self.js.api_prefix(),
                self.stream,
                self.cfg.durable_name.as_ref().unwrap()
            );

            let sub = match self.js.nc.request_multi(&subject, batch_size.to_string()) {
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
            let ret = f(&next);
            let is_err = ret.is_err();
            rets.push(ret);

            if is_err {
                // we will still try to ack all messages before this one
                // if our ack policy is `All`, after breaking.
                break;
            } else if self.cfg.ack_policy == AckPolicy::Explicit {
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
    /// Does not return an `Err` if acking the message is unsuccessful.
    /// If you require stronger processing guarantees, you can manually call
    /// the `double_ack` method of the argument message. If you require
    /// both the returned `Ok` from the closure and the `Err` from a
    /// failed ack, use `process_batch` instead.
    pub fn process<R, F: Fn(&Message) -> io::Result<R>>(&mut self, f: F) -> io::Result<R> {
        let next = if let Some(ps) = &self.push_subscriber {
            ps.next().unwrap()
        } else {
            if self.cfg.durable_name.is_none() {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "process and process_batch are only usable from \
                        Pull-based Consumers if there is a durable_name set",
                ));
            }

            let subject = format!(
                "{}CONSUMER.MSG.NEXT.{}.{}",
                self.js.api_prefix(),
                self.stream,
                self.cfg.durable_name.as_ref().unwrap()
            );

            self.js.nc.request(&subject, AckKind::Ack)?
        };

        let ret = f(&next)?;
        if self.cfg.ack_policy != AckPolicy::None {
            let _dont_care = next.ack();
        }

        Ok(ret)
    }

    /// Process and acknowledge a single message, waiting up to the `Consumer`'s
    /// configured `timeout` before returning a timeout error.
    ///
    /// Does not ack the processed message if the internal closure returns an `Err`.
    ///
    /// Does not return an `Err` if acking the message is unsuccessful,
    /// If you require stronger processing guarantees, you can manually call
    /// the `double_ack` method of the argument message. If you require
    /// both the returned `Ok` from the closure and the `Err` from a
    /// failed ack, use `process_batch` instead.
    pub fn process_timeout<R, F: Fn(&Message) -> io::Result<R>>(&mut self, f: F) -> io::Result<R> {
        let next = if let Some(ps) = &self.push_subscriber {
            ps.next_timeout(self.timeout)?
        } else {
            if self.cfg.durable_name.is_none() {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "process and process_batch are only usable from \
                        Pull-based Consumers if there is a a durable_name set",
                ));
            }

            let subject = format!(
                "{}CONSUMER.MSG.NEXT.{}.{}",
                self.js.api_prefix(),
                self.stream,
                self.cfg.durable_name.as_ref().unwrap()
            );

            self.js.nc.request_timeout(&subject, b"", self.timeout)?
        };

        let ret = f(&next)?;
        if self.cfg.ack_policy != AckPolicy::None {
            let _dont_care = next.ack();
        }

        Ok(ret)
    }

    /// For pull-based consumers (a consumer where `ConsumerConfig.deliver_subject` is `None`)
    /// this can be used to request a single message, and wait forever for a response.
    /// If you require specifying the batch size or using a timeout while consuming the
    /// responses, use the `pull_opt` method below.
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
            Err(io::Error::new(
                ErrorKind::BrokenPipe,
                "The nats client is shutting down.",
            ))
        }
    }

    /// For pull-based consumers (a consumer where `ConsumerConfig.deliver_subject` is `None`)
    /// this can be used to request a configurable number of messages, as well as specify
    /// how the server will keep track of this batch request over time. See the docs for
    /// `NextRequest` for more information about the options.
    pub fn pull_opt(&mut self, next_request: NextRequest) -> io::Result<crate::Subscription> {
        if self.cfg.durable_name.is_none() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "this method is only usable from \
                Pull-based Consumers with a durable_name set",
            ));
        }

        if self.cfg.deliver_subject.is_some() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "this method is only usable from \
                Pull-based Consumers with a deliver_subject set to None",
            ));
        }

        let subject = format!(
            "{}CONSUMER.MSG.NEXT.{}.{}",
            self.js.api_prefix(),
            self.stream,
            self.cfg.durable_name.as_ref().unwrap()
        );

        let req = serde_json::ser::to_vec(&next_request).unwrap();
        self.js.nc.request_multi(&subject, &req)
    }
}

/// Creates a new `JetStream` context using the given `Connection` and default options.
///
pub fn new(nc: Connection) -> JetStream {
    JetStream::new(nc, JetStreamOptions::default())
}
