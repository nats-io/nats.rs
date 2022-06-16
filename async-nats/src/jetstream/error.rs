use crate::status::StatusCode;
use std::error;
use std::fmt;

/// The error type for jetstream operations and associated types.
///
/// Errors mostly originate from the server, but custom instances of
/// `Error` can be created with crafted error messages and a particular value of [`ErrorCode`].
///
#[derive(Debug)]
pub struct Error {
    code: Option<ErrorCode>,
    status: Option<StatusCode>,
    description: Option<String>,
    source: Option<Box<dyn error::Error + 'static>>,
}

/// Specialized `ErrorCode` which can be returned from a server an a response when an error occurs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
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
    ClusterNotAvailable = 10008,
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
    /// Invalid Json
    InvalidJson = 10025,
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
    StreamWrongLastMessageId = 10070,
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
    StreamGeneralError = 10051,
    /// Stream configuration validation error string
    StreamInvalidConfig = 10052,
    /// Replicas > 1 not supported in non-clustered mode
    StreamReplicasNotSupported = 10074,
    /// Generic message deletion failure error string
    StreamMessageDeleteFailed = 10057,
    /// Peer remap failed
    PeerRemap = 10075,
    /// Generic error when storing a message failed
    StreamStoreFailed = 10077,
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
    /// ConsumerMaxRequestBatchNegative consumer max request batch needs to be > 0
    ConsumerMaxRequestBatchNegative = 10114,
    /// ConsumerMaxRequestExpiresToSmall consumer max request expires needs to be >= 1ms
    ConsumerMaxRequestExpiresToSmall = 10115,
    /// Consumer idle heartbeat needs to be >= 100ms
    ConsumerSmallHeartbeat = 10083,
    /// Consumer in pull mode requires ack policy
    ConsumerPullRequiresAck = 10084,
    /// Consumer in pull mode requires a durable name
    ConsumerPullNotDurable = 10085,
    /// Consumer in pull mode can not have rate limit set
    ConsumerPullWithRateLimit = 10086,
    /// Consumer max waiting needs to be positive
    ConsumerMaxWaitingNegative = 10087,
    /// Consumer idle heartbeat requires a push based consumer
    ConsumerHeartbeatRequiresPush = 10088,
    /// Consumer flow control requires a push based consumer
    ConsumerFlowControlRequiresPush = 10089,
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

impl From<ErrorCode> for Error {
    /// Converts an [`ErrorCode`] into an [`Error`].
    ///
    /// This conversion creates a new error with the given error code.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::jetstream::{Error, ErrorCode};
    ///
    /// let stream_not_found = ErrorCode::StreamNotFound;
    /// let error = Error::from(stream_not_found);
    /// ```
    #[inline]
    fn from(code: ErrorCode) -> Error {
        Error {
            code: Some(code),
            status: None,
            description: None,
            source: None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{:?} {:?} {:?} {:?}",
            self.code, self.status, self.description, self.source
        )
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.source.as_ref().map(Box::as_ref)
    }
}

impl Error {
    pub fn new(code: ErrorCode, status: StatusCode, description: String) -> Error {
        Error {
            code: Some(code),
            status: Some(status),
            description: Some(description),
            source: None,
        }
    }

    pub fn other<E>(source: E) -> Error
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        Error {
            code: None,
            status: None,
            description: None,
            source: Some(source.into()),
        }
    }

    pub fn code(&self) -> Option<ErrorCode> {
        self.code
    }

    pub fn status(&self) -> Option<StatusCode> {
        self.status
    }

    pub fn description(&self) -> Option<String> {
        self.description
    }
}
