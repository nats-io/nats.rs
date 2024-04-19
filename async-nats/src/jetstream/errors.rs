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

use std::{error, fmt};

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize)]
pub struct ErrorCode(u64);

impl ErrorCode {
    /// Peer not a member
    pub const CLUSTER_PEER_NOT_MEMBER: ErrorCode = ErrorCode(10040);

    /// Consumer expected to be ephemeral but detected a durable name set in subject
    pub const CONSUMER_EPHEMERAL_WITH_DURABLE: ErrorCode = ErrorCode(10019);

    /// Stream external delivery prefix overlaps with stream subject
    pub const STREAM_EXTERNAL_DELETE_PREFIX_OVERLAPS: ErrorCode = ErrorCode(10022);

    /// Resource limits exceeded for account
    pub const ACCOUNT_RESOURCES_EXCEEDED: ErrorCode = ErrorCode(10002);

    /// Jetstream system temporarily unavailable
    pub const CLUSTER_NOT_AVAILABLE: ErrorCode = ErrorCode(10008);

    /// Subjects overlap with an existing stream
    pub const STREAM_SUBJECT_OVERLAP: ErrorCode = ErrorCode(10065);

    /// Wrong last sequence
    pub const STREAM_WRONG_LAST_SEQUENCE: ErrorCode = ErrorCode(10071);

    /// Template name in subject does not match request
    pub const NAME_NOT_MATCH_SUBJECT: ErrorCode = ErrorCode(10073);

    /// No suitable peers for placement
    pub const CLUSTER_NO_PEERS: ErrorCode = ErrorCode(10005);

    /// Consumer expected to be ephemeral but a durable name was set in request
    pub const CONSUMER_EPHEMERAL_WITH_DURABLE_NAME: ErrorCode = ErrorCode(10020);

    /// Insufficient resources
    pub const INSUFFICIENT_RESOURCES: ErrorCode = ErrorCode(10023);

    /// Stream mirror must have max message size >= source
    pub const MIRROR_MAX_MESSAGE_SIZE_TOO_BIG: ErrorCode = ErrorCode(10030);

    /// Generic error from stream deletion operation
    pub const STREAM_DELETE_FAILED: ErrorCode = ErrorCode(10067);

    /// Bad request
    pub const BAD_REQUEST: ErrorCode = ErrorCode(10003);

    /// Not currently supported in clustered mode
    pub const NOT_SUPPORTED_IN_CLUSTER_MODE: ErrorCode = ErrorCode(10036);

    /// Consumer not found
    pub const CONSUMER_NOT_FOUND: ErrorCode = ErrorCode(10014);

    /// Stream source must have max message size >= target
    pub const SOURCE_MAX_MESSAGE_SIZE_TOO_BIG: ErrorCode = ErrorCode(10046);

    /// Generic error when stream operation fails.
    pub const STREAM_ASSIGNMENT: ErrorCode = ErrorCode(10048);

    /// Message size exceeds maximum allowed
    pub const STREAM_MESSAGE_EXCEEDS_MAXIMUM: ErrorCode = ErrorCode(10054);

    /// Generic error for stream creation error with a string
    pub const STREAM_CREATE_TEMPLATE: ErrorCode = ErrorCode(10066);

    /// Invalid JSON
    pub const INVALID_JSON: ErrorCode = ErrorCode(10025);

    /// Stream external delivery prefix must not contain wildcards
    pub const STREAM_INVALID_EXTERNAL_DELIVERY_SUBJECT: ErrorCode = ErrorCode(10024);

    /// Restore failed
    pub const STREAM_RESTORE: ErrorCode = ErrorCode(10062);

    /// Incomplete results
    pub const CLUSTER_INCOMPLETE: ErrorCode = ErrorCode(10004);

    /// Account not found
    pub const NO_ACCOUNT: ErrorCode = ErrorCode(10035);

    /// General RAFT error
    pub const RAFT_GENERAL: ErrorCode = ErrorCode(10041);

    /// Jetstream unable to subscribe to restore snapshot
    pub const RESTORE_SUBSCRIBE_FAILED: ErrorCode = ErrorCode(10042);

    /// Stream deletion failed
    pub const STREAM_DELETE: ErrorCode = ErrorCode(10050);

    /// Stream external api prefix must not overlap
    pub const STREAM_EXTERNAL_API_OVERLAP: ErrorCode = ErrorCode(10021);

    /// Stream mirrors can not contain subjects
    pub const MIRROR_WITH_SUBJECTS: ErrorCode = ErrorCode(10034);

    /// Jetstream not enabled
    pub const JETSTREAM_NOT_ENABLED: ErrorCode = ErrorCode(10076);

    /// Jetstream not enabled for account
    pub const JETSTREAM_NOT_ENABLED_FOR_ACCOUNT: ErrorCode = ErrorCode(10039);

    /// Sequence not found
    pub const SEQUENCE_NOT_FOUND: ErrorCode = ErrorCode(10043);

    /// Stream mirror configuration can not be updated
    pub const STREAM_MIRROR_NOT_UPDATABLE: ErrorCode = ErrorCode(10055);

    /// Expected stream sequence does not match
    pub const STREAM_SEQUENCE_NOT_MATCH: ErrorCode = ErrorCode(10063);

    /// Wrong last msg id
    pub const STREAM_WRONG_LAST_MESSAGE_ID: ErrorCode = ErrorCode(10070);

    /// Jetstream unable to open temp storage for restore
    pub const TEMP_STORAGE_FAILED: ErrorCode = ErrorCode(10072);

    /// Insufficient storage resources available
    pub const STORAGE_RESOURCES_EXCEEDED: ErrorCode = ErrorCode(10047);

    /// Stream name in subject does not match request
    pub const STREAM_MISMATCH: ErrorCode = ErrorCode(10056);

    /// Expected stream does not match
    pub const STREAM_NOT_MATCH: ErrorCode = ErrorCode(10060);

    /// Setting up consumer mirror failed
    pub const MIRROR_CONSUMER_SETUP_FAILED: ErrorCode = ErrorCode(10029);

    /// Expected an empty request payload
    pub const NOT_EMPTY_REQUEST: ErrorCode = ErrorCode(10038);

    /// Stream name already in use with a different configuration
    pub const STREAM_NAME_EXIST: ErrorCode = ErrorCode(10058);

    /// Tags placement not supported for operation
    pub const CLUSTER_TAGS: ErrorCode = ErrorCode(10011);

    /// Maximum consumers limit reached
    pub const MAXIMUM_CONSUMERS_LIMIT: ErrorCode = ErrorCode(10026);

    /// General source consumer setup failure
    pub const SOURCE_CONSUMER_SETUP_FAILED: ErrorCode = ErrorCode(10045);

    /// Consumer creation failed
    pub const CONSUMER_CREATE: ErrorCode = ErrorCode(10012);

    /// Consumer expected to be durable but no durable name set in subject
    pub const CONSUMER_DURABLE_NAME_NOT_IN_SUBJECT: ErrorCode = ErrorCode(10016);

    /// Stream limits error
    pub const STREAM_LIMITS: ErrorCode = ErrorCode(10053);

    /// Replicas configuration can not be updated
    pub const STREAM_REPLICAS_NOT_UPDATABLE: ErrorCode = ErrorCode(10061);

    /// Template not found
    pub const STREAM_TEMPLATE_NOT_FOUND: ErrorCode = ErrorCode(10068);

    /// Jetstream cluster not assigned to this server
    pub const CLUSTER_NOT_ASSIGNED: ErrorCode = ErrorCode(10007);

    /// Jetstream cluster can't handle request
    pub const CLUSTER_NOT_LEADER: ErrorCode = ErrorCode(10009);

    /// Consumer name already in use
    pub const CONSUMER_NAME_EXIST: ErrorCode = ErrorCode(10013);

    /// Stream mirrors can't also contain other sources
    pub const MIRROR_WITH_SOURCES: ErrorCode = ErrorCode(10031);

    /// Stream not found
    pub const STREAM_NOT_FOUND: ErrorCode = ErrorCode(10059);

    /// Jetstream clustering support required
    pub const CLUSTER_REQUIRED: ErrorCode = ErrorCode(10010);

    /// Consumer expected to be durable but a durable name was not set
    pub const CONSUMER_DURABLE_NAME_NOT_SET: ErrorCode = ErrorCode(10018);

    /// Maximum number of streams reached
    pub const MAXIMUM_STREAMS_LIMIT: ErrorCode = ErrorCode(10027);

    /// Stream mirrors can not have both start seq and start time configured
    pub const MIRROR_WITH_START_SEQUENCE_AND_TIME: ErrorCode = ErrorCode(10032);

    /// Stream snapshot failed
    pub const STREAM_SNAPSHOT: ErrorCode = ErrorCode(10064);

    /// Stream update failed
    pub const STREAM_UPDATE: ErrorCode = ErrorCode(10069);

    /// Jetstream not in clustered mode
    pub const CLUSTER_NOT_ACTIVE: ErrorCode = ErrorCode(10006);

    /// Consumer name in subject does not match durable name in request
    pub const CONSUMER_DURABLE_NAME_NOT_MATCH_SUBJECT: ErrorCode = ErrorCode(10017);

    /// Insufficient memory resources available
    pub const MEMORY_RESOURCES_EXCEEDED: ErrorCode = ErrorCode(10028);

    /// Stream mirrors can not contain filtered subjects
    pub const MIRROR_WITH_SUBJECT_FILTERS: ErrorCode = ErrorCode(10033);

    /// Stream create failed with a string
    pub const STREAM_CREATE: ErrorCode = ErrorCode(10049);

    /// Server is not a member of the cluster
    pub const CLUSTER_SERVER_NOT_MEMBER: ErrorCode = ErrorCode(10044);

    /// No message found
    pub const NO_MESSAGE_FOUND: ErrorCode = ErrorCode(10037);

    /// Deliver subject not valid
    pub const SNAPSHOT_DELIVER_SUBJECT_INVALID: ErrorCode = ErrorCode(10015);

    /// General stream failure
    pub const STREAM_GENERALOR: ErrorCode = ErrorCode(10051);

    /// Invalid stream config
    pub const STREAM_INVALID_CONFIG: ErrorCode = ErrorCode(10052);

    /// Replicas > 1 not supported in non-clustered mode
    pub const STREAM_REPLICAS_NOT_SUPPORTED: ErrorCode = ErrorCode(10074);

    /// Stream message delete failed
    pub const STREAM_MESSAGE_DELETE_FAILED: ErrorCode = ErrorCode(10057);

    /// Peer remap failed
    pub const PEER_REMAP: ErrorCode = ErrorCode(10075);

    /// Stream store failed
    pub const STREAM_STORE_FAILED: ErrorCode = ErrorCode(10077);

    /// Consumer config required
    pub const CONSUMER_CONFIG_REQUIRED: ErrorCode = ErrorCode(10078);

    /// Consumer deliver subject has wildcards
    pub const CONSUMER_DELIVER_TO_WILDCARDS: ErrorCode = ErrorCode(10079);

    /// Consumer in push mode can not set max waiting
    pub const CONSUMER_PUSH_MAX_WAITING: ErrorCode = ErrorCode(10080);

    /// Consumer deliver subject forms a cycle
    pub const CONSUMER_DELIVER_CYCLE: ErrorCode = ErrorCode(10081);

    /// Consumer requires ack policy for max ack pending
    pub const CONSUMER_MAX_PENDING_ACK_POLICY_REQUIRED: ErrorCode = ErrorCode(10082);

    /// Consumer idle heartbeat needs to be >= 100ms
    pub const CONSUMER_SMALL_HEARTBEAT: ErrorCode = ErrorCode(10083);

    /// Consumer in pull mode requires ack policy
    pub const CONSUMER_PULL_REQUIRES_ACK: ErrorCode = ErrorCode(10084);

    /// Consumer in pull mode requires a durable name
    pub const CONSUMER_PULL_NOT_DURABLE: ErrorCode = ErrorCode(10085);

    /// Consumer in pull mode can not have rate limit set
    pub const CONSUMER_PULL_WITH_RATE_LIMIT: ErrorCode = ErrorCode(10086);

    /// Consumer max waiting needs to be positive
    pub const CONSUMER_MAX_WAITING_NEGATIVE: ErrorCode = ErrorCode(10087);

    /// Consumer idle heartbeat requires a push based consumer
    pub const CONSUMER_HEARTBEAT_REQUIRES_PUSH: ErrorCode = ErrorCode(10088);

    /// Consumer flow control requires a push based consumer
    pub const CONSUMER_FLOW_CONTROL_REQUIRES_PUSH: ErrorCode = ErrorCode(10089);

    /// Consumer direct requires a push based consumer
    pub const CONSUMER_DIRECT_REQUIRES_PUSH: ErrorCode = ErrorCode(10090);

    /// Consumer direct requires an ephemeral consumer
    pub const CONSUMER_DIRECT_REQUIRES_EPHEMERAL: ErrorCode = ErrorCode(10091);

    /// Consumer direct on a mapped consumer
    pub const CONSUMER_ON_MAPPED: ErrorCode = ErrorCode(10092);

    /// Consumer filter subject is not a valid subset of the interest subjects
    pub const CONSUMER_FILTER_NOT_SUBSET: ErrorCode = ErrorCode(10093);

    /// Invalid consumer policy
    pub const CONSUMER_INVALID_POLICY: ErrorCode = ErrorCode(10094);

    /// Failed to parse consumer sampling configuration
    pub const CONSUMER_INVALID_SAMPLING: ErrorCode = ErrorCode(10095);

    /// Stream not valid
    pub const STREAM_INVALID: ErrorCode = ErrorCode(10096);

    /// Workqueue stream requires explicit ack
    pub const CONSUMER_WQ_REQUIRES_EXPLICIT_ACK: ErrorCode = ErrorCode(10098);

    /// Multiple non-filtered consumers not allowed on workqueue stream
    pub const CONSUMER_WQ_MULTIPLE_UNFILTERED: ErrorCode = ErrorCode(10099);

    /// Filtered consumer not unique on workqueue stream
    pub const CONSUMER_WQ_CONSUMER_NOT_UNIQUE: ErrorCode = ErrorCode(10100);

    /// Consumer must be deliver all on workqueue stream
    pub const CONSUMER_WQ_CONSUMER_NOT_DELIVER_ALL: ErrorCode = ErrorCode(10101);

    /// Consumer name is too long
    pub const CONSUMER_NAME_TOO_LONG: ErrorCode = ErrorCode(10102);

    /// Durable name can not contain token separators and wildcards
    pub const CONSUMER_BAD_DURABLE_NAME: ErrorCode = ErrorCode(10103);

    /// Error creating store for consumer
    pub const CONSUMER_STORE_FAILED: ErrorCode = ErrorCode(10104);

    /// Consumer already exists and is still active
    pub const CONSUMER_EXISTING_ACTIVE: ErrorCode = ErrorCode(10105);

    /// Consumer replacement durable config not the same
    pub const CONSUMER_REPLACEMENT_WITH_DIFFERENT_NAME: ErrorCode = ErrorCode(10106);

    /// Consumer description is too long
    pub const CONSUMER_DESCRIPTION_TOO_LONG: ErrorCode = ErrorCode(10107);

    /// Header size exceeds maximum allowed of 64k
    pub const STREAM_HEADER_EXCEEDS_MAXIMUM: ErrorCode = ErrorCode(10097);

    /// Consumer with flow control also needs heartbeats
    pub const CONSUMER_WITH_FLOW_CONTROL_NEEDS_HEARTBEATS: ErrorCode = ErrorCode(10108);

    /// Invalid operation on sealed stream
    pub const STREAM_SEALED: ErrorCode = ErrorCode(10109);

    /// Stream purge failed
    pub const STREAM_PURGE_FAILED: ErrorCode = ErrorCode(10110);

    /// Stream rollup failed
    pub const STREAM_ROLLUP_FAILED: ErrorCode = ErrorCode(10111);

    /// Invalid push consumer deliver subject
    pub const CONSUMER_INVALID_DELIVER_SUBJECT: ErrorCode = ErrorCode(10112);

    /// Account requires a stream config to have max bytes set
    pub const STREAM_MAX_BYTES_REQUIRED: ErrorCode = ErrorCode(10113);

    /// Consumer max request batch needs to be > 0
    pub const CONSUMER_MAX_REQUEST_BATCH_NEGATIVE: ErrorCode = ErrorCode(10114);

    /// Consumer max request expires needs to be >= 1ms
    pub const CONSUMER_MAX_REQUEST_EXPIRES_TO_SMALL: ErrorCode = ErrorCode(10115);

    /// Max deliver is required to be > length of backoff values
    pub const CONSUMER_MAX_DELIVER_BACKOFF: ErrorCode = ErrorCode(10116);

    /// Subject details would exceed maximum allowed
    pub const STREAM_INFO_MAX_SUBJECTS: ErrorCode = ErrorCode(10117);

    /// Stream is offline
    pub const STREAM_OFFLINE: ErrorCode = ErrorCode(10118);

    /// Consumer is offline
    pub const CONSUMER_OFFLINE: ErrorCode = ErrorCode(10119);

    /// No jetstream default or applicable tiered limit present
    pub const NO_LIMITS: ErrorCode = ErrorCode(10120);

    /// Consumer max ack pending exceeds system limit
    pub const CONSUMER_MAX_PENDING_ACK_EXCESS: ErrorCode = ErrorCode(10121);

    /// Stream max bytes exceeds account limit max stream bytes
    pub const STREAM_MAX_STREAM_BYTES_EXCEEDED: ErrorCode = ErrorCode(10122);

    /// Can not move and scale a stream in a single update
    pub const STREAM_MOVE_AND_SCALE: ErrorCode = ErrorCode(10123);

    /// Stream move already in progress
    pub const STREAM_MOVE_IN_PROGRESS: ErrorCode = ErrorCode(10124);

    /// Consumer max request batch exceeds server limit
    pub const CONSUMER_MAX_REQUEST_BATCH_EXCEEDED: ErrorCode = ErrorCode(10125);

    /// Consumer config replica count exceeds parent stream
    pub const CONSUMER_REPLICAS_EXCEEDS_STREAM: ErrorCode = ErrorCode(10126);

    /// Consumer name can not contain path separators
    pub const CONSUMER_NAME_CONTAINS_PATH_SEPARATORS: ErrorCode = ErrorCode(10127);

    /// Stream name can not contain path separators
    pub const STREAM_NAME_CONTAINS_PATH_SEPARATORS: ErrorCode = ErrorCode(10128);

    /// Stream move not in progress
    pub const STREAM_MOVE_NOT_IN_PROGRESS: ErrorCode = ErrorCode(10129);

    /// Stream name already in use, cannot restore
    pub const STREAM_NAME_EXIST_RESTORE_FAILED: ErrorCode = ErrorCode(10130);

    /// Consumer create request did not match filtered subject from create subject
    pub const CONSUMER_CREATE_FILTER_SUBJECT_MISMATCH: ErrorCode = ErrorCode(10131);

    /// Consumer durable and name have to be equal if both are provided
    pub const CONSUMER_CREATE_DURABLE_AND_NAME_MISMATCH: ErrorCode = ErrorCode(10132);

    /// Replicas count cannot be negative
    pub const REPLICAS_COUNT_CANNOT_BE_NEGATIVE: ErrorCode = ErrorCode(10133);

    /// Consumer config replicas must match interest retention stream's replicas
    pub const CONSUMER_REPLICAS_SHOULD_MATCH_STREAM: ErrorCode = ErrorCode(10134);

    /// Consumer metadata exceeds maximum size
    pub const CONSUMER_METADATA_LENGTH: ErrorCode = ErrorCode(10135);

    /// Consumer cannot have both filter_subject and filter_subjects specified
    pub const CONSUMER_DUPLICATE_FILTER_SUBJECTS: ErrorCode = ErrorCode(10136);

    /// Consumer with multiple subject filters cannot use subject based api
    pub const CONSUMER_MULTIPLE_FILTERS_NOT_ALLOWED: ErrorCode = ErrorCode(10137);

    /// Consumer subject filters cannot overlap
    pub const CONSUMER_OVERLAPPING_SUBJECT_FILTERS: ErrorCode = ErrorCode(10138);

    /// Consumer filter in filter_subjects cannot be empty
    pub const CONSUMER_EMPTY_FILTER: ErrorCode = ErrorCode(10139);

    /// Duplicate source configuration detected
    pub const SOURCE_DUPLICATE_DETECTED: ErrorCode = ErrorCode(10140);

    /// Sourced stream name is invalid
    pub const SOURCE_INVALID_STREAM_NAME: ErrorCode = ErrorCode(10141);

    /// Mirrored stream name is invalid
    pub const MIRROR_INVALID_STREAM_NAME: ErrorCode = ErrorCode(10142);

    /// Source with multiple subject transforms cannot also have a single subject filter
    pub const SOURCE_MULTIPLE_FILTERS_NOT_ALLOWED: ErrorCode = ErrorCode(10144);

    /// Source subject filter is invalid
    pub const SOURCE_INVALID_SUBJECT_FILTER: ErrorCode = ErrorCode(10145);

    /// Source transform destination is invalid
    pub const SOURCE_INVALID_TRANSFORM_DESTINATION: ErrorCode = ErrorCode(10146);

    /// Source filters cannot overlap
    pub const SOURCE_OVERLAPPING_SUBJECT_FILTERS: ErrorCode = ErrorCode(10147);

    /// Consumer already exists
    pub const CONSUMER_ALREADY_EXISTS: ErrorCode = ErrorCode(10148);

    /// Consumer does not exist
    pub const CONSUMER_DOES_NOT_EXIST: ErrorCode = ErrorCode(10149);

    /// Mirror with multiple subject transforms cannot also have a single subject filter
    pub const MIRROR_MULTIPLE_FILTERS_NOT_ALLOWED: ErrorCode = ErrorCode(10150);

    /// Mirror subject filter is invalid
    pub const MIRROR_INVALID_SUBJECT_FILTER: ErrorCode = ErrorCode(10151);

    /// Mirror subject filters cannot overlap
    pub const MIRROR_OVERLAPPING_SUBJECT_FILTERS: ErrorCode = ErrorCode(10152);

    /// Consumer inactive threshold exceeds system limit
    pub const CONSUMER_INACTIVE_THRESHOLD_EXCESS: ErrorCode = ErrorCode(10153);
}

/// `Error` type returned from an API response when an error occurs.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Error {
    code: usize,
    err_code: ErrorCode,
    description: Option<String>,
}

impl Error {
    /// Returns the status code associated with this error
    pub fn code(&self) -> usize {
        self.code
    }

    /// Returns the server side error code associated with this error.
    pub fn error_code(&self) -> ErrorCode {
        self.err_code
    }

    pub fn kind(&self) -> ErrorCode {
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
            self.err_code.0,
        )
    }
}

impl error::Error for Error {}
