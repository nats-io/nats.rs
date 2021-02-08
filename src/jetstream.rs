#![allow(missing_docs)]
#![allow(unused)]
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

//! Experimental Jetstream support enabled via the `jetstream` feature.

use std::{
    io::{self, Error, ErrorKind},
    time::UNIX_EPOCH,
};

use chrono::{DateTime as ChronoDateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::Connection as NatsClient;

// Request API subjects for JetStream.
const JS_DEFAULT_API_PREFIX: &str = "$JS.API.";

// JSAPIRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
const JS_API_REQUEST_NEXT_T: &str = "CONSUMER.MSG.NEXT.%s.%s";

///
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct DateTime(ChronoDateTime<Utc>);

// ApiResponse is a standard response from the JetStream JSON Api
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ApiResponse<T> {
    Ok(T),
    Err { r#type: String, error: ApiError },
}

// ApiError is included in all Api responses if there was an error.
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize, Clone)]
struct ApiError {
    code: usize,
    description: Option<String>,
}

impl Default for DateTime {
    fn default() -> DateTime {
        DateTime(UNIX_EPOCH.into())
    }
}

///
#[derive(Debug, Default, Clone)]
pub struct Subscription {
    consumer: String,
    stream: String,
    deliver: String,
    pull: i64,
    durable: bool,
    attached: bool,
}

///
#[derive(Debug, Default, Clone, Copy)]
pub struct Msg;

///
#[derive(Debug, Default, Clone, Copy)]
pub struct MsgHandler;

///
#[derive(Debug, Default, Clone, Copy)]
pub struct Context;

///
pub struct Chan<A>(A);

///
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct JSApiCreateConsumerRequest {
    stream_name: String,
    config: ConsumerConfig,
}

// DeliverPolicy determines how the consumer should select the first message to deliver.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum DeliverPolicy {
    // DeliverAllPolicy will be the default so can be omitted from the request.
    #[serde(rename = "all")]
    DeliverAllPolicy = 0,
    // DeliverLastPolicy will start the consumer with the last sequence received.
    #[serde(rename = "last")]
    DeliverLastPolicy = 1,
    // DeliverNewPolicy will only deliver new messages that are sent
    // after the consumer is created.
    #[serde(rename = "new")]
    DeliverNewPolicy = 2,
    // DeliverByStartSequencePolicy will look for a defined starting sequence to start.
    #[serde(rename = "by_start_sequence")]
    DeliverByStartSequencePolicy = 3,
    // StartTime will select the first messsage with a timestamp >= to StartTime.
    #[serde(rename = "by_start_time")]
    DeliverByStartTimePolicy = 4,
}

impl Default for DeliverPolicy {
    fn default() -> DeliverPolicy {
        DeliverPolicy::DeliverAllPolicy
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum AckPolicy {
    #[serde(rename = "none")]
    AckNone = 0,
    #[serde(rename = "all")]
    AckAll = 1,
    #[serde(rename = "explicit")]
    AckExplicit = 2,
    // For setting
    AckPolicyNotSet = 99,
}

impl Default for AckPolicy {
    fn default() -> AckPolicy {
        AckPolicy::AckExplicit
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum ReplayPolicy {
    #[serde(rename = "instant")]
    ReplayInstant = 0,
    #[serde(rename = "original")]
    ReplayOriginal = 1,
}

impl Default for ReplayPolicy {
    fn default() -> ReplayPolicy {
        ReplayPolicy::ReplayInstant
    }
}

///
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct ConsumerConfig {
    pub durable_name: Option<String>,
    pub deliver_subject: Option<String>,
    pub deliver_policy: DeliverPolicy,
    pub opt_start_seq: Option<i64>,
    pub opt_start_time: Option<DateTime>,
    pub ack_policy: AckPolicy,
    pub ack_wait: Option<isize>,
    pub max_deliver: Option<i64>,
    pub filter_subject: Option<String>,
    pub replay_policy: ReplayPolicy,
    pub rate_limit: Option<i64>,
    pub sample_frequency: Option<String>,
    pub max_waiting: Option<i64>,
    pub max_ack_pending: Option<i64>,
}

impl From<&str> for ConsumerConfig {
    fn from(s: &str) -> ConsumerConfig {
        ConsumerConfig {
            durable_name: Some(s.to_string()),
            ..Default::default()
        }
    }
}

/// StreamConfig will determine the properties for a stream.
/// There are sensible defaults for most. If no subjects are
/// given the name will be used as the only subject.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct StreamConfig {
    pub subjects: Option<Vec<String>>,
    pub name: String,
    pub retention: RetentionPolicy,
    pub max_consumers: isize,
    pub max_msgs: i64,
    pub max_bytes: i64,
    pub discard: DiscardPolicy,
    pub max_age: isize,
    pub max_msg_size: Option<i32>,
    pub storage: StorageType,
    pub num_replicas: usize,
    pub no_ack: Option<bool>,
    pub template_owner: Option<String>,
    pub duplicate_window: Option<isize>,
}

impl From<&str> for StreamConfig {
    fn from(s: &str) -> StreamConfig {
        StreamConfig {
            name: s.to_string(),
            ..Default::default()
        }
    }
}

/// StreamInfo shows config and current state for this stream.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct StreamInfo {
    pub r#type: String,
    pub config: StreamConfig,
    pub created: DateTime,
    pub state: StreamState,
}

// StreamStats is information about the given stream.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct StreamState {
    #[serde(default)]
    pub msgs: u64,
    pub bytes: u64,
    pub first_seq: u64,
    pub first_ts: String,
    pub last_seq: u64,
    pub last_ts: DateTime,
    pub consumer_count: usize,
}

// RetentionPolicy determines how messages in a set are retained.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum RetentionPolicy {
    // LimitsPolicy (default) means that messages are retained until any given limit is reached.
    // This could be one of MaxMsgs, MaxBytes, or MaxAge.
    #[serde(rename = "limits")]
    LimitsPolicy = 0,
    // InterestPolicy specifies that when all known observables have acknowledged a message it can be removed.
    #[serde(rename = "interest")]
    InterestPolicy = 1,
    // WorkQueuePolicy specifies that when the first worker or subscriber acknowledges the message it can be removed.
    #[serde(rename = "workqueue")]
    WorkQueuePolicy = 2,
}

impl Default for RetentionPolicy {
    fn default() -> RetentionPolicy {
        RetentionPolicy::LimitsPolicy
    }
}

// Discard Policy determines how we proceed when limits of messages or bytes are hit. The default, DicscardOld will
// remove older messages. DiscardNew will fail to store the new message.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum DiscardPolicy {
    // DiscardOld will remove older messages to return to the limits.
    #[serde(rename = "old")]
    DiscardOld = 0,
    //DiscardNew will error on a StoreMsg call
    #[serde(rename = "new")]
    DiscardNew = 1,
}

impl Default for DiscardPolicy {
    fn default() -> DiscardPolicy {
        DiscardPolicy::DiscardOld
    }
}

// StorageType determines how messages are stored for retention.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum StorageType {
    // FileStorage specifies on disk storage. It's the default.
    #[serde(rename = "file")]
    FileStorage = 0,
    // MemoryStorage specifies in memory only.
    #[serde(rename = "memory")]
    MemoryStorage = 1,
}

impl Default for StorageType {
    fn default() -> StorageType {
        StorageType::FileStorage
    }
}

// AccountLimits is for the information about
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct AccountLimits {
    pub max_memory: i64,
    pub max_storage: i64,
    pub max_streams: i64,
    pub max_consumers: i64,
}

// AccountStats returns current statistics about the account's JetStream usage.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct AccountStats {
    pub memory: u64,
    pub storage: u64,
    pub streams: usize,
    pub limits: AccountLimits,
}

///
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct PubAck {
    pub stream: String,
    pub seq: u64,
    pub duplicate: Option<bool>,
}

///
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct ConsumerInfo {
    pub r#type: String,
    pub stream_name: String,
    pub name: String,
    pub created: DateTime,
    pub config: ConsumerConfig,
    pub delivered: SequencePair,
    pub ack_floor: SequencePair,
    pub num_ack_pending: usize,
    pub num_redelivered: usize,
    pub num_waiting: usize,
    pub num_pending: u64,
    pub cluster: ClusterInfo,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct ClusterInfo {
    pub leader: String,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct SequencePair {
    pub consumer_seq: u64,
    pub stream_seq: u64,
}

// NextRequest is for getting next messages for pull based consumers.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct NextRequest {
    pub expires: DateTime,
    pub batch: Option<usize>,
    pub no_wait: Option<bool>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct StreamNamesResponse {
    pub r#type: String,
    pub streams: Vec<String>,

    // related to paging
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct StreamRequest {
    pub subject: Option<String>,
}

///
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct SubOpts {
    // For attaching.
    pub stream: String,
    pub consumer: String,
    // For pull based consumers, batch size for pull
    pub pull: usize,
    // For manual ack
    pub mack: bool,
    // For creating or updating.
    pub cfg: ConsumerConfig,
}

///
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct PubOpts {
    pub ttl: isize,
    pub id: String,
    // Expected last msgId
    pub lid: String,
    // Expected stream name
    pub str: String,
    // Expected last sequence
    pub seq: u64,
}

/// AccountInfo contains info about the JetStream usage from the current account.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct AccountInfo {
    pub r#type: String,
    pub memory: i64,
    pub storage: i64,
    pub streams: i64,
    pub consumers: i64,
    pub api: ApiStats,
    pub limits: AccountLimits,
}

/// ApiStats reports on API calls to JetStream for this account.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct ApiStats {
    pub total: u64,
    pub errors: u64,
}

///
pub struct Consumer {
    nc: NatsClient,
}

impl Consumer {
    /// Publishing messages to JetStream.
    pub fn publish(
        &self,
        subject: &str,
        data: &[u8],
        opts: Option<PubOpts>,
    ) -> io::Result<PubAck> {
        todo!()
    }

    /// Publishing messages to JetStream.
    pub fn publish_msg(
        &self,
        msg: Msg,
        opts: Option<PubOpts>,
    ) -> io::Result<PubAck> {
        todo!()
    }

    /// Subscribing to messages in JetStream.
    pub fn subscribe(
        &self,
        subj: String,
        cb: MsgHandler,
        opts: Option<SubOpts>,
    ) -> io::Result<Subscription> {
        todo!()
    }

    /// Subscribing to messages in JetStream.
    pub fn subscribe_sync(
        &self,
        subj: String,
        opts: Option<SubOpts>,
    ) -> io::Result<Subscription> {
        todo!()
    }

    /// Channel versions.
    pub fn chan_subscribe(
        &self,
        subj: String,
        ch: Chan<Msg>,
        opts: Option<SubOpts>,
    ) -> io::Result<Subscription> {
        todo!()
    }

    /// QueueSubscribe.
    pub fn queue_subscribe(
        &self,
        subj: String,
        queue: String,
        cb: MsgHandler,
        opts: Option<SubOpts>,
    ) -> io::Result<Subscription> {
        todo!()
    }
}

///
pub struct Manager {
    nc: NatsClient,
}

impl Manager {
    /// Create a stream.
    pub fn add_stream<S>(&self, stream_config: S) -> io::Result<StreamInfo>
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
        let subject: String = format!("$JS.API.STREAM.CREATE.{}", cfg.name);
        let req = serde_json::ser::to_vec(&cfg)?;
        self.request(&subject, &req)
    }

    /// Query all stream names.
    pub fn stream_names(&self) -> io::Result<StreamNamesResponse> {
        self.request("$JS.API.STREAM.NAMES", b"")
    }

    /// Query stream information.
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
        let subject: String = format!("$JS.API.STREAM.INFO.{}", stream);
        self.request(&subject, b"")
    }

    /// Purge stream messages.
    pub fn purge_stream<S: AsRef<str>>(&self, stream: S) -> io::Result<()> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject = format!("$JS.API.STREAM.PURGE.{}", stream);
        self.request(&subject, b"")
    }

    /// Delete stream.
    pub fn delete_stream<S: AsRef<str>>(&self, stream: S) -> io::Result<()> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject = format!("$JS.API.STREAM.DELETE.{}", stream);
        self.request(&subject, b"")
    }

    /// Create a consumer.
    pub fn add_consumer<S, C>(
        &self,
        stream: S,
        cfg: C,
    ) -> io::Result<ConsumerInfo>
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
                format!("$JS.API.CONSUMER.CREATE.{}", stream)
            } else {
                format!(
                    "$JS.API.CONSUMER.DURABLE.CREATE.{}.{}",
                    stream, durable_name
                )
            }
        } else {
            format!("$JS.API.CONSUMER.CREATE.{}", stream)
        };

        let req = JSApiCreateConsumerRequest {
            stream_name: stream.into(),
            config,
        };

        let ser_req = serde_json::ser::to_vec(&req)?;

        self.request(&subject, &ser_req)
    }

    /// Delete a consumer.
    pub fn delete_consumer<S, C>(
        &self,
        stream: S,
        consumer: C,
    ) -> io::Result<ConsumerInfo>
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
        let consumer = stream.as_ref();
        if consumer.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the consumer name must not be empty",
            ));
        }

        let subject =
            format!("$JS.API.CONSUMER.DELETE.{}.{}", stream, consumer);

        self.request(&subject, b"")
    }

    /// Query consumer information.
    pub fn consumer_info<S1, S2>(
        &self,
        stream: S1,
        consumer: S2,
    ) -> io::Result<ConsumerInfo>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let consumer: &str = consumer.as_ref();
        let subject: String =
            format!("$JS.API.CONSUMER.INFO.{}.{}", stream, consumer);
        self.request(&subject, b"")
    }

    /// Query account information.
    pub fn account_info(&self) -> io::Result<AccountInfo> {
        self.request("$JS.API.INFO", b"")
    }

    fn request<Res>(&self, subject: &str, req: &[u8]) -> io::Result<Res>
    where
        Res: DeserializeOwned,
    {
        let res_msg = self.nc.request(subject, req)?;
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
}

/*

package nats

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "net/http"
    "strconv"
    "strings"
    "time"
)

// JetStream is the public interface for JetStream.
type JetStream interface {
}

// JetStream is the public interface for the JetStream context.
type JetStreamContext interface {
    JetStream
    JetStreamManager
}


type AccountInfoResponse struct {
    ApiResponse
    *AccountStats
}

// Internal struct for jetstream
type js struct {
    nc *Conn
    // For importing JetStream from other accounts.
    pre string
    // Amount of time to wait for Api requests.
    wait time.Duration
    // Signals only direct access and no Api access.
    direct bool
}

// Request Api subjects for JetStream.
const (
    JSDefaultApiPrefix = "$JS.Api."
    // JSApiAccountInfo is for obtaining general information about JetStream.
    JSApiAccountInfo = "INFO"
    // JSApiStreams can lookup a stream by subject.
    JSApiStreams = "STREAM.NAMES"
    // JSApiConsumerCreateT is used to create consumers.
    JSApiConsumerCreateT = "CONSUMER.CREATE.%s"
    // JSApiDurableCreateT is used to create durable consumers.
    JSApiDurableCreateT = "CONSUMER.DURABLE.CREATE.%s.%s"
    // JSApiConsumerInfoT is used to create consumers.
    JSApiConsumerInfoT = "CONSUMER.INFO.%s.%s"
    // JSApiRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
    JSApiRequestNextT = "CONSUMER.MSG.NEXT.%s.%s"
    // JSApiStreamCreateT is the endpoint to create new streams.
    JSApiStreamCreateT = "STREAM.CREATE.%s"
    // JSApiStreamInfoT is the endpoint to get information on a stream.
    JSApiStreamInfoT = "STREAM.INFO.%s"
)

// JetStream returns a JetStream context for pub/sub interactions.
func (nc *Conn) JetStream(opts ...JSOpt) (JetStreamContext, error) {
    const defaultRequestWait = 5 * time.Second

    js := &js{nc: nc, pre: JSDefaultApiPrefix, wait: defaultRequestWait}

    for _, opt := range opts {
        if err := opt.configureJSContext(js); err != nil {
            return nil, err
        }
    }

    if js.direct {
        return js, nil
    }

    resp, err := nc.Request(js.apiSubj(JSApiAccountInfo), nil, js.wait)
    if err != nil {
        if err == ErrNoResponders {
            err = ErrJetStreamNotEnabled
        }
        return nil, err
    }
    var info AccountInfoResponse
    if err := json.Unmarshal(resp.Data, &info); err != nil {
        return nil, err
    }
    if info.Error != nil && info.Error.Code == 503 {
        return nil, ErrJetStreamNotEnabled
    }
    return js, nil
}

// JSOpt configures a JetStream context.
type JSOpt interface {
    configureJSContext(opts *js) error
}

// jsOptFn configures an option for the JetStream context.
type jsOptFn func(opts *js) error

func (opt jsOptFn) configureJSContext(opts *js) error {
    return opt(opts)
}

func ApiPrefix(pre string) JSOpt {
    return jsOptFn(func(js *js) error {
        js.pre = pre
        if !strings.HasSuffix(js.pre, ".") {
            js.pre = js.pre + "."
        }
        return nil
    })
}

func DirectOnly() JSOpt {
    return jsOptFn(func(js *js) error {
        js.direct = true
        return nil
    })
}

func (js *js) apiSubj(subj string) string {
    if js.pre == _EMPTY_ {
        return subj
    }
    var b strings.Builder
    b.WriteString(js.pre)
    b.WriteString(subj)
    return b.String()
}

// PubOpt configures options for publishing JetStream messages.
type PubOpt interface {
    configurePublish(opts *pubOpts) error
}

// pubOptFn is a function option used to configure JetStream Publish.
type pubOptFn func(opts *pubOpts) error

func (opt pubOptFn) configurePublish(opts *pubOpts) error {
    return opt(opts)
}

type PubAckResponse struct {
    ApiResponse
    *PubAck
}
// Headers for published messages.
const (
    MsgIdHdr             = "Nats-Msg-Id"
    ExpectedStreamHdr    = "Nats-Expected-Stream"
    ExpectedLastSeqHdr   = "Nats-Expected-Last-Sequence"
    ExpectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id"
)

func (js *js) PublishMsg(m *Msg, opts ...PubOpt) (*PubAck, error) {
    var o pubOpts
    if len(opts) > 0 {
        if m.Header == nil {
            m.Header = http.Header{}
        }
        for _, opt := range opts {
            if err := opt.configurePublish(&o); err != nil {
                return nil, err
            }
        }
    }
    // Check for option collisions. Right now just timeout and context.
    if o.ctx != nil && o.ttl != 0 {
        return nil, ErrContextAndTimeout
    }
    if o.ttl == 0 && o.ctx == nil {
        o.ttl = js.wait
    }

    if o.id != _EMPTY_ {
        m.Header.Set(MsgIdHdr, o.id)
    }
    if o.lid != _EMPTY_ {
        m.Header.Set(ExpectedLastMsgIdHdr, o.lid)
    }
    if o.str != _EMPTY_ {
        m.Header.Set(ExpectedStreamHdr, o.str)
    }
    if o.seq > 0 {
        m.Header.Set(ExpectedLastSeqHdr, strconv.FormatUint(o.seq, 10))
    }

    var resp *Msg
    var err error

    if o.ttl > 0 {
        resp, err = js.nc.RequestMsg(m, time.Duration(o.ttl))
    } else {
        resp, err = js.nc.RequestMsgWithContext(o.ctx, m)
    }

    if err != nil {
        if err == ErrNoResponders {
            err = ErrNoStreamResponse
        }
        return nil, err
    }
    var pa PubAckResponse
    if err := json.Unmarshal(resp.Data, &pa); err != nil {
        return nil, ErrInvalidJSAck
    }
    if pa.Error != nil {
        return nil, errors.New(pa.Error.Description)
    }
    if pa.PubAck == nil || pa.PubAck.Stream == _EMPTY_ {
        return nil, ErrInvalidJSAck
    }
    return pa.PubAck, nil
}

func (js *js) Publish(subj string, data []byte, opts ...PubOpt) (*PubAck, error) {
    return js.PublishMsg(&Msg{Subject: subj, Data: data}, opts...)
}

// Options for publishing to JetStream.

// MsgId sets the message ID used for de-duplication.
func MsgId(id string) PubOpt {
    return pubOptFn(func(opts *pubOpts) error {
        opts.id = id
        return nil
    })
}

// ExpectStream sets the expected stream to respond from the publish.
func ExpectStream(stream string) PubOpt {
    return pubOptFn(func(opts *pubOpts) error {
        opts.str = stream
        return nil
    })
}

// ExpectLastSequence sets the expected sequence in the response from the publish.
func ExpectLastSequence(seq u64) PubOpt {
    return pubOptFn(func(opts *pubOpts) error {
        opts.seq = seq
        return nil
    })
}

// ExpectLastSequence sets the expected sequence in the response from the publish.
func ExpectLastMsgId(id string) PubOpt {
    return pubOptFn(func(opts *pubOpts) error {
        opts.lid = id
        return nil
    })
}

// MaxWait sets the maximum amount of time we will wait for a response.
type MaxWait time.Duration

func (ttl MaxWait) configurePublish(opts *pubOpts) error {
    opts.ttl = time.Duration(ttl)
    return nil
}

func (ttl MaxWait) configureJSContext(js *js) error {
    js.wait = time.Duration(ttl)
    return nil
}

// ContextOpt is an option used to set a context.Context.
type ContextOpt struct {
    context.Context
}

func (ctx ContextOpt) configurePublish(opts *pubOpts) error {
    opts.ctx = ctx
    return nil
}

// Context returns an option that can be used to configure a context.
func Context(ctx context.Context) ContextOpt {
    return ContextOpt{ctx}
}

// Subscribe
// We will match subjects to streams and consumers on the user's behalf.

// SubOpt configures options for subscribing to JetStream consumers.
type SubOpt interface {
    configureSubscribe(opts *subOpts) error
}

// subOptFn is a function option used to configure a JetStream Subscribe.
type subOptFn func(opts *subOpts) error

func (opt subOptFn) configureSubscribe(opts *subOpts) error {
    return opt(opts)
}

// Subscribe will create a subscription to the appropriate stream and consumer.
func (js *js) Subscribe(subj string, cb MsgHandler, opts ...SubOpt) (*Subscription, error) {
    return js.subscribe(subj, _EMPTY_, cb, nil, opts)
}

// SubscribeSync will create a sync subscription to the appropriate stream and consumer.
func (js *js) SubscribeSync(subj string, opts ...SubOpt) (*Subscription, error) {
    mch := make(chan *Msg, js.nc.Opts.SubChanLen)
    return js.subscribe(subj, _EMPTY_, nil, mch, opts)
}

// QueueSubscribe will create a subscription to the appropriate stream and consumer with queue semantics.
func (js *js) QueueSubscribe(subj, queue string, cb MsgHandler, opts ...SubOpt) (*Subscription, error) {
    return js.subscribe(subj, queue, cb, nil, opts)
}

// Subscribe will create a subscription to the appropriate stream and consumer.
func (js *js) ChanSubscribe(subj string, ch chan *Msg, opts ...SubOpt) (*Subscription, error) {
    return js.subscribe(subj, _EMPTY_, nil, ch, opts)
}
func (js *js) subscribe(subj, queue string, cb MsgHandler, ch chan *Msg, opts []SubOpt) (*Subscription, error) {
    cfg := ConsumerConfig{AckPolicy: ackPolicyNotSet}
    o := subOpts{cfg: &cfg}
    if len(opts) > 0 {
        for _, opt := range opts {
            if err := opt.configureSubscribe(&o); err != nil {
                return nil, err
            }
        }
    }

    isPullMode := o.pull > 0
    if cb != nil && isPullMode {
        return nil, ErrPullModeNotAllowed
    }

    var err error
    var stream, deliver string
    var ccfg *ConsumerConfig

    // If we are attaching to an existing consumer.
    shouldAttach := o.stream != _EMPTY_ && o.consumer != _EMPTY_ || o.cfg.DeliverSubject != _EMPTY_
    shouldCreate := !shouldAttach

    if js.direct && shouldCreate {
        return nil, ErrDirectModeRequired
    }

    if js.direct {
        if o.cfg.DeliverSubject != _EMPTY_ {
            deliver = o.cfg.DeliverSubject
        } else {
            deliver = NewInbox()
        }
    } else if shouldAttach {
        info, err := js.getConsumerInfo(o.stream, o.consumer)
        if err != nil {
            return nil, err
        }

        ccfg = &info.Config
        // Make sure this new subject matches or is a subset.
        if ccfg.FilterSubject != _EMPTY_ && subj != ccfg.FilterSubject {
            return nil, ErrSubjectMismatch
        }
        if ccfg.DeliverSubject != _EMPTY_ {
            deliver = ccfg.DeliverSubject
        } else {
            deliver = NewInbox()
        }
    } else {
        stream, err = js.lookupStreamBySubject(subj)
        if err != nil {
            return nil, err
        }
        deliver = NewInbox()
        if !isPullMode {
            cfg.DeliverSubject = deliver
        }
        // Do filtering always, server will clear as needed.
        cfg.FilterSubject = subj
    }

    var sub *Subscription

    // Check if we are manual ack.
    if cb != nil && !o.mack {
        ocb := cb
        cb = func(m *Msg) { ocb(m); m.Ack() }
    }

    sub, err = js.nc.subscribe(deliver, queue, cb, ch, cb == nil, &jsSub{js: js})
    if err != nil {
        return nil, err
    }

    // If we are creating or updating let's process that request.
    if shouldCreate {
        // If not set default to ack explicit.
        if cfg.AckPolicy == ackPolicyNotSet {
            cfg.AckPolicy = AckExplicit
        }
        // If we have acks at all and the MaxAckPending is not set go ahead
        // and set to the internal max.
        // TODO(dlc) - We should be able to update this if client updates PendingLimits.
        if cfg.MaxAckPending == 0 && cfg.AckPolicy != AckNone {
            maxMsgs, _, _ := sub.PendingLimits()
            cfg.MaxAckPending = maxMsgs
        }

        req := &JSApiCreateConsumerRequest{
            Stream: stream,
            Config: &cfg,
        }

        j, err := json.Marshal(req)
        if err != nil {
            return nil, err
        }

        var ccSubj string
        if cfg.Durable != _EMPTY_ {
            ccSubj = fmt.Sprintf(JSApiDurableCreateT, stream, cfg.Durable)
        } else {
            ccSubj = fmt.Sprintf(JSApiConsumerCreateT, stream)
        }

        resp, err := js.nc.Request(js.apiSubj(ccSubj), j, js.wait)
        if err != nil {
            if err == ErrNoResponders {
                err = ErrJetStreamNotEnabled
            }
            sub.Unsubscribe()
            return nil, err
        }

        var info JSApiConsumerResponse
        err = json.Unmarshal(resp.Data, &info)
        if err != nil {
            sub.Unsubscribe()
            return nil, err
        }
        if info.Error != nil {
            sub.Unsubscribe()
            return nil, errors.New(info.Error.Description)
        }

        // Hold onto these for later.
        sub.jsi.stream = info.Stream
        sub.jsi.consumer = info.Name
        sub.jsi.deliver = info.Config.DeliverSubject
    } else {
        sub.jsi.stream = o.stream
        sub.jsi.consumer = o.consumer
        if js.direct {
            sub.jsi.deliver = o.cfg.DeliverSubject
        } else {
            sub.jsi.deliver = ccfg.DeliverSubject
        }
    }

    // If we are pull based go ahead and fire off the first request to populate.
    if isPullMode {
        sub.jsi.pull = o.pull
        sub.Poll()
    }

    return sub, nil
}

func (js *js) lookupStreamBySubject(subj string) (string, error) {
    var slr JSApiStreamNamesResponse
    // FIXME(dlc) - prefix
    req := &streamRequest{subj}
    j, err := json.Marshal(req)
    if err != nil {
        return _EMPTY_, err
    }
    resp, err := js.nc.Request(js.apiSubj(JSApiStreams), j, js.wait)
    if err != nil {
        if err == ErrNoResponders {
            err = ErrJetStreamNotEnabled
        }
        return _EMPTY_, err
    }
    if err := json.Unmarshal(resp.Data, &slr); err != nil {
        return _EMPTY_, err
    }
    if slr.Error != nil || len(slr.Streams) != 1 {
        return _EMPTY_, ErrNoMatchingStream
    }
    return slr.Streams[0], nil
}

func Durable(name string) SubOpt {
    return subOptFn(func(opts *subOpts) error {
        opts.cfg.Durable = name
        return nil
    })
}

func Attach(stream, consumer string) SubOpt {
    return subOptFn(func(opts *subOpts) error {
        opts.stream = stream
        opts.consumer = consumer
        return nil
    })
}

func Pull(batchSize int) SubOpt {
    return subOptFn(func(opts *subOpts) error {
        if batchSize == 0 {
            return errors.New("nats: batch size of 0 not valid")
        }
        opts.pull = batchSize
        return nil
    })
}

func PullDirect(stream, consumer string, batchSize int) SubOpt {
    return subOptFn(func(opts *subOpts) error {
        if batchSize == 0 {
            return errors.New("nats: batch size of 0 not valid")
        }
        opts.stream = stream
        opts.consumer = consumer
        opts.pull = batchSize
        return nil
    })
}

func PushDirect(deliverSubject string) SubOpt {
    return subOptFn(func(opts *subOpts) error {
        opts.cfg.DeliverSubject = deliverSubject
        return nil
    })
}

func ManualAck() SubOpt {
    return subOptFn(func(opts *subOpts) error {
        opts.mack = true
        return nil
    })
}

// DeliverAll will configure a Consumer to receive all the
// messages from a Stream.
func DeliverAll() SubOpt {
    return subOptFn(func(opts *subOpts) error {
        opts.cfg.DeliverPolicy = DeliverAllPolicy
        return nil
    })
}

// DeliverLast configures a Consumer to receive messages
// starting with the latest one.
func DeliverLast() SubOpt {
    return subOptFn(func(opts *subOpts) error {
        opts.cfg.DeliverPolicy = DeliverLastPolicy
        return nil
    })
}

// DeliverNew configures a Consumer to receive messages
// published after the subscription.
func DeliverNew() SubOpt {
    return subOptFn(func(opts *subOpts) error {
        opts.cfg.DeliverPolicy = DeliverNewPolicy
        return nil
    })
}

// StartSequence configures a Consumer to receive
// messages from a start sequence.
func StartSequence(seq u64) SubOpt {
    return subOptFn(func(opts *subOpts) error {
        opts.cfg.DeliverPolicy = DeliverByStartSequencePolicy
        opts.cfg.OptStartSeq = seq
        return nil
    })
}

// DeliverFromTime configures a Consumer to receive
// messages from a start time.
func StartTime(startTime time.Time) SubOpt {
    return subOptFn(func(opts *subOpts) error {
        opts.cfg.DeliverPolicy = DeliverByStartTimePolicy
        opts.cfg.OptStartTime = &startTime
        return nil
    })
}

func (sub *Subscription) ConsumerInfo() (*ConsumerInfo, error) {
    sub.mu.Lock()
    // TODO(dlc) - Better way to mark especially if we attach.
    if sub.jsi.consumer == _EMPTY_ {
        sub.mu.Unlock()
        return nil, ErrTypeSubscription
    }

    js := sub.jsi.js
    stream, consumer := sub.jsi.stream, sub.jsi.consumer
    sub.mu.Unlock()

    return js.getConsumerInfo(stream, consumer)
}

func (sub *Subscription) Poll() error {
    sub.mu.Lock()
    if sub.jsi == nil || sub.jsi.deliver != _EMPTY_ || sub.jsi.pull == 0 {
        sub.mu.Unlock()
        return ErrTypeSubscription
    }
    batch := sub.jsi.pull
    nc, reply := sub.conn, sub.Subject
    stream, consumer := sub.jsi.stream, sub.jsi.consumer
    js := sub.jsi.js
    sub.mu.Unlock()

    req, _ := json.Marshal(&NextRequest{Batch: batch})
    reqNext := js.apiSubj(fmt.Sprintf(JSApiRequestNextT, stream, consumer))
    return nc.PublishRequest(reqNext, reply, req)
}


func (m *Msg) checkReply() (*js, bool, error) {
    if m.Reply == "" {
        return nil, false, ErrMsgNoReply
    }
    if m == nil || m.Sub == nil {
        return nil, false, ErrMsgNotBound
    }
    sub := m.Sub
    sub.mu.Lock()
    if sub.jsi == nil {
        sub.mu.Unlock()
        return nil, false, ErrNotJSMessage
    }
    js := sub.jsi.js
    isPullMode := sub.jsi.pull > 0
    sub.mu.Unlock()

    return js, isPullMode, nil
}

// ackReply handles all acks. Will do the right thing for pull and sync mode.
func (m *Msg) ackReply(ackType []byte, sync bool) error {
    js, isPullMode, err := m.checkReply()
    if err != nil {
        return err
    }
    if isPullMode {
        if bytes.Equal(ackType, AckAck) {
            err = js.nc.PublishRequest(m.Reply, m.Sub.Subject, AckNext)
        } else if bytes.Equal(ackType, AckNak) || bytes.Equal(ackType, AckTerm) {
            err = js.nc.PublishRequest(m.Reply, m.Sub.Subject, []byte("+NXT {\"batch\":1}"))
        }
        if sync && err == nil {
            _, err = js.nc.Request(m.Reply, nil, js.wait)
        }
    } else if sync {
        _, err = js.nc.Request(m.Reply, ackType, js.wait)
    } else {
        err = js.nc.Publish(m.Reply, ackType)
    }
    return err
}

// Acks for messages

// Ack a message, this will do the right thing with pull based consumers.
func (m *Msg) Ack() error {
    return m.ackReply(AckAck, false)
}

// Ack a message and wait for a response from the server.
func (m *Msg) AckSync() error {
    return m.ackReply(AckAck, true)
}

// Nak this message, indicating we can not process.
func (m *Msg) Nak() error {
    return m.ackReply(AckNak, false)
}

// Term this message from ever being delivered regardless of MaxDeliverCount.
func (m *Msg) Term() error {
    return m.ackReply(AckTerm, false)
}

// Indicate that this message is being worked on and reset redelkivery timer in the server.
func (m *Msg) InProgress() error {
    return m.ackReply(AckProgress, false)
}

// JetStream metadata associated with received messages.
type MsgMetaData struct {
    Consumer  u64
    Stream    u64
    Delivered u64
    Pending   u64
    Timestamp time.Time
}

func (m *Msg) MetaData() (*MsgMetaData, error) {
    if _, _, err := m.checkReply(); err != nil {
        return nil, err
    }

    const expectedTokens = 9
    const btsep = '.'

    tsa := [expectedTokens]string{}
    start, tokens := 0, tsa[:0]
    subject := m.Reply
    for i := 0; i < len(subject); i++ {
        if subject[i] == btsep {
            tokens = append(tokens, subject[start:i])
            start = i + 1
        }
    }
    tokens = append(tokens, subject[start:])
    if len(tokens) != expectedTokens || tokens[0] != "$JS" || tokens[1] != "ACK" {
        return nil, ErrNotJSMessage
    }

    meta := &MsgMetaData{
        Delivered: u64(parseNum(tokens[4])),
        Stream:    u64(parseNum(tokens[5])),
        Consumer:  u64(parseNum(tokens[6])),
        Timestamp: time.Unix(0, parseNum(tokens[7])),
        Pending:   u64(parseNum(tokens[8])),
    }

    return meta, nil
}

// Quick parser for positive numbers in ack reply encoding.
func parseNum(d string) (n int64) {
    if len(d) == 0 {
        return -1
    }

    // Ascii numbers 0-9
    const (
        asciiZero = 48
        asciiNine = 57
    )

    for _, dec := range d {
        if dec < asciiZero || dec > asciiNine {
            return -1
        }
        n = n*10 + (int64(dec) - asciiZero)
    }
    return n
}

var (
    AckAck      = []byte("+ACK")
    AckNak      = []byte("-NAK")
    AckProgress = []byte("+WPI")
    AckNext     = []byte("+NXT")
    AckTerm     = []byte("+TERM")
)


*/

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn round_trip() {
        let nc = crate::connect("localhost:4222").unwrap();
        let manager = Manager { nc };

        dbg!(manager.add_stream("test2"));
        dbg!(manager.stream_info("test2"));
        dbg!(manager.add_consumer("test2", "consumer1"));
        dbg!(manager.consumer_info("test2", "consumer1"));
        dbg!(manager.stream_names());
        dbg!(manager.account_info());
    }
}
