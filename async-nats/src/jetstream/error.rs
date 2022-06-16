use crate::status::StatusCode;
use std::error;
use std::fmt;
use std::num::NonZeroU64;

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

/// A possible error value when converting a `ErrorCode` from a `u16`
///
/// This error indicates that the supplied input was not a valid number or was zero.
pub struct InvalidErrorCode {}

impl fmt::Debug for InvalidErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("InvalidErrorCode").finish()
    }
}

impl fmt::Display for InvalidErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid error code")
    }
}

impl error::Error for InvalidErrorCode {}

impl InvalidErrorCode {
    fn new() -> InvalidErrorCode {
        InvalidErrorCode {}
    }
}

/// Specialized `ErrorCode` which can be returned from a server an a response when an error occurs.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorCode(NonZeroU64);

impl ErrorCode {
    /// Converts a u64 to a error code.
    ///
    /// # Example
    ///
    /// ```
    /// use async_nats::status::ErrorCode;
    ///
    /// let ok = ErrorCode::from_u64(200).unwrap();
    /// assert_eq!(ok, ErrorCode::OK);
    /// ```
    #[inline]
    pub fn from_u64(src: u64) -> Result<ErrorCode, InvalidErrorCode> {
        NonZeroU64::new(src)
            .map(ErrorCode)
            .ok_or_else(InvalidErrorCode::new)
    }

    /// Returns the `u64` corresponding to this `ErrorCode`.
    ///
    /// # Example
    ///
    /// ```
    /// let code = async_nats::ErrorCode::CLUSTER_PEER_NOT_MEMBER;;
    /// assert_eq!(status.as_u64(), 10040);
    /// ```
    #[inline]
    pub fn as_u64(&self) -> u64 {
        (*self).into()
    }
}

impl fmt::Debug for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

/// Formats the error code.
///
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO(caspervonb) display a canonical statically known reason / human readable description of the error code
        write!(f, "{}", u64::from(*self))
    }
}

impl PartialEq<u64> for ErrorCode {
    #[inline]
    fn eq(&self, other: &u64) -> bool {
        self.as_u64() == *other
    }
}

impl PartialEq<ErrorCode> for u64 {
    #[inline]
    fn eq(&self, other: &ErrorCode) -> bool {
        *self == other.as_u64()
    }
}

impl From<ErrorCode> for u64 {
    #[inline]
    fn from(status: ErrorCode) -> u64 {
        status.0.get()
    }
}

impl ErrorCode {
    /// Peer not a member
    const ClusterPeerNotMember: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10040) });
    /// Consumer expected to be ephemeral but detected a durable name set in subject
    const ConsumerEphemeralWithDurableInSubject: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10019) });
    /// Stream external delivery prefix {prefix} overlaps with stream subject {subject}
    const StreamExternalDeliveryPrefixOverlaps: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10022) });
    /// Resource limits exceeded for account
    const AccountResourcesExceeded: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10002) });
    /// JetStream system temporarily unavailable
    const ClusterNotAvailable: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10008) });
    /// Subjects overlap with an existing stream
    const StreamSubjectOverlap: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10065) });
    /// Wrong last sequence: {seq}
    const StreamWrongLastSequence: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10071) });
    /// Template name in subject does not match request
    const TemplateNameNotMatchSubject: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10073) });
    /// No suitable peers for placement
    const ClusterNoPeers: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10005) });
    /// Consumer expected to be ephemeral but a durable name was set in request
    const ConsumerEphemeralWithDurableName: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10020) });
    /// Insufficient resources
    const InsufficientResources: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10023) });
    /// Stream mirror must have max message size >= source
    const MirrorMaxMessageSizeTooBig: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10030) });
    /// Generic stream template deletion failed error string
    const StreamTemplateDelete: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10067) });
    /// Bad request
    const BadRequest: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10003) });
    /// Not currently supported in clustered mode
    const ClusterUnSupportFeature: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10036) });
    /// Consumer not found
    const ConsumerNotFound: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10014) });
    /// Stream source must have max message size >= target
    const SourceMaxMessageSizeTooBig: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10046) });
    /// Generic stream assignment error string
    const StreamAssignment: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10048) });
    /// Message size exceeds maximum allowed
    const StreamMessageExceedsMaximum: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10054) });
    /// Generic template creation failed string
    const StreamTemplateCreate: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10066) });
    /// Invalid Json
    const InvalidJson: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10025) });
    /// Stream external delivery prefix {prefix} must not contain wildcards
    const StreamInvalidExternalDeliverySubject: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10024) });
    /// Restore failed: {err}
    const StreamRestore: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10062) });
    /// Incomplete results
    const ClusterIncomplete: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10004) });
    /// Account not found
    const NoAccount: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10035) });
    /// General RAFT error string
    const RaftGeneral: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10041) });
    /// JetStream unable to subscribe to restore snapshot {subject}: {err}
    const RestoreSubscribeFailed: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10042) });
    /// General stream deletion error string
    const StreamDelete: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10050) });
    /// Stream external api prefix {prefix} must not overlap with {subject}
    const StreamExternalApiOverlap: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10021) });
    /// Stream mirrors can not also contain subjects
    const MirrorWithSubjects: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10034) });
    /// JetStream not enabled
    const NotEnabled: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10076) });
    /// JetStream not enabled for account
    const NotEnabledForAccount: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10039) });
    /// Sequence {seq} not found
    const SequenceNotFound: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10043) });
    /// Mirror configuration can not be updated
    const StreamMirrorNotUpdatable: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10055) });
    /// Expected stream sequence does not match
    const StreamSequenceNotMatch: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10063) });
    /// Wrong last msg Id: {id}
    const StreamWrongLastMessageId: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10070) });
    /// JetStream unable to open temp storage for restore
    const TempStorageFailed: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10072) });
    /// Insufficient storage resources available
    const StorageResourcesExceeded: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10047) });
    /// Stream name in subject does not match request
    const StreamMismatch: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10056) });
    /// Expected stream does not match
    const StreamNotMatch: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10060) });
    /// Generic mirror consumer setup failure string
    const MirrorConsumerSetupFailed: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10029) });
    /// Expected an empty request payload
    const NotEmptyRequest: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10038) });
    /// Stream name already in use
    const StreamNameExist: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10058) });
    /// Tags placement not supported for operation
    const ClusterTags: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10011) });
    /// Maximum consumers limit reached
    const MaximumConsumersLimit: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10026) });
    /// General source consumer setup failure string
    const SourceConsumerSetupFailed: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10045) });
    /// General consumer creation failure string
    const ConsumerCreate: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10012) });
    /// Consumer expected to be durable but no durable name set in subject
    const ConsumerDurableNameNotInSubject: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10016) });
    /// General stream limits exceeded error string
    const StreamLimits: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10053) });
    /// Replicas configuration can not be updated
    const StreamReplicasNotUpdatable: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10061) });
    /// Template not found
    const StreamTemplateNotFound: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10068) });
    /// JetStream cluster not assigned to this server
    const ClusterNotAssigned: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10007) });
    /// JetStream cluster can not handle request
    const ClusterNotLeader: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10009) });
    /// Consumer name already in use
    const ConsumerNameExist: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10013) });
    /// Stream mirrors can not also contain other sources
    const MirrorWithSources: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10031) });
    /// Stream not found
    const StreamNotFound: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10059) });
    /// JetStream clustering support required
    const ClusterRequired: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10010) });
    /// Consumer expected to be durable but a durable name was not set
    const ConsumerDurableNameNotSet: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10018) });
    /// Maximum number of streams reached
    const MaximumStreamsLimit: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10027) });
    /// Stream mirrors can not have both start seq and start time configured
    const MirrorWithStartSeqAndTime: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10032) });
    /// Snapshot failed: {err}
    const StreamSnapshot: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10064) });
    /// Generic stream update error string
    const StreamUpdate: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10069) });
    /// JetStream not in clustered mode
    const ClusterNotActive: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10006) });
    /// Consumer name in subject does not match durable name in request
    const ConsumerDurableNameNotMatchSubject: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10017) });
    /// Insufficient memory resources available
    const MemoryResourcesExceeded: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10028) });
    /// Stream mirrors can not contain filtered subjects
    const MirrorWithSubjectFilters: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10033) });
    /// Generic stream creation error string
    const StreamCreate: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10049) });
    /// Server is not a member of the cluster
    const ClusterServerNotMember: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10044) });
    /// No message found
    const NoMessageFound: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10037) });
    /// Deliver subject not valid
    const SnapshotDeliverSubjectInvalid: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10015) });
    /// General stream failure string
    const StreamGeneralError: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10051) });
    /// Stream configuration validation error string
    const StreamInvalidConfig: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10052) });
    /// Replicas > 1 not supported in non-clustered mode
    const StreamReplicasNotSupported: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10074) });
    /// Generic message deletion failure error string
    const StreamMessageDeleteFailed: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10057) });
    /// Peer remap failed
    const PeerRemap: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10075) });
    /// Generic error when storing a message failed
    const StreamStoreFailed: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10077) });
    /// Consumer config required
    const ConsumerConfigRequired: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10078) });
    /// Consumer deliver subject has wildcards
    const ConsumerDeliverToWildcards: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10079) });
    /// Consumer in push mode can not set max waiting
    const ConsumerPushMaxWaiting: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10080) });
    /// Consumer deliver subject forms a cycle
    const ConsumerDeliverCycle: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10081) });
    /// Consumer requires ack policy for max ack pending
    const ConsumerMaxPendingAckPolicyRequired: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10082) });
    /// ConsumerMaxRequestBatchNegative consumer max request batch needs to be > 0
    const ConsumerMaxRequestBatchNegative: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10114) });
    /// ConsumerMaxRequestExpiresToSmall consumer max request expires needs to be >= 1ms
    const ConsumerMaxRequestExpiresToSmall: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10115) });
    /// Consumer idle heartbeat needs to be >= 100ms
    const ConsumerSmallHeartbeat: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10083) });
    /// Consumer in pull mode requires ack policy
    const ConsumerPullRequiresAck: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10084) });
    /// Consumer in pull mode requires a durable name
    const ConsumerPullNotDurable: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10085) });
    /// Consumer in pull mode can not have rate limit set
    const ConsumerPullWithRateLimit: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10086) });
    /// Consumer max waiting needs to be positive
    const ConsumerMaxWaitingNegative: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10087) });
    /// Consumer idle heartbeat requires a push based consumer
    const ConsumerHeartbeatRequiresPush: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10088) });
    /// Consumer flow control requires a push based consumer
    const ConsumerFlowControlRequiresPush: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10089) });
    /// Consumer direct requires a push based consumer
    const ConsumerDirectRequiresPush: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10090) });
    /// Consumer direct requires an ephemeral consumer
    const ConsumerDirectRequiresEphemeral: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10091) });
    /// Consumer direct on a mapped consumer
    const ConsumerOnMapped: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10092) });
    /// Consumer filter subject is not a valid subset of the interest subjects
    const ConsumerFilterNotSubset: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10093) });
    /// Generic delivery policy error
    const ConsumerInvalidPolicy: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10094) });
    /// Failed to parse consumer sampling configuration: {err}
    const ConsumerInvalidSampling: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10095) });
    /// Stream not valid
    const StreamInvalid: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10096) });
    /// Workqueue stream requires explicit ack
    const ConsumerWQRequiresExplicitAck: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10098) });
    /// Multiple non-filtered consumers not allowed on workqueue stream
    const ConsumerWQMultipleUnfiltered: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10099) });
    /// Filtered consumer not unique on workqueue stream
    const ConsumerWQConsumerNotUnique: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10100) });
    /// Consumer must be deliver all on workqueue stream
    const ConsumerWQConsumerNotDeliverAll: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10101) });
    /// Consumer name is too long, maximum allowed is {max}
    const ConsumerNameTooLong: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10102) });
    /// Durable name can not contain '.', '*', '>'
    const ConsumerBadDurableName: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10103) });
    /// Error creating store for consumer: {err}
    const ConsumerStoreFailed: ErrorCode = ErrorCode(unsafe { NonZeroU64::new_unchecked(10104) });
    /// Consumer already exists and is still active
    const ConsumerExistingActive: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10105) });
    /// Consumer replacement durable config not the same
    const ConsumerReplacementWithDifferentName: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10106) });
    /// Consumer description is too long, maximum allowed is {max}
    const ConsumerDescriptionTooLong: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10107) });
    /// Header size exceeds maximum allowed of 64k
    const StreamHeaderExceedsMaximum: ErrorCode =
        ErrorCode(unsafe { NonZeroU64::new_unchecked(10097) });
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

impl From<StatusCode> for Error {
    /// Converts a [`StatusCode`] into an [`Error`].
    ///
    /// This conversion creates a new error with the given status code.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::jetstream::{Error, ErrorCode};
    ///
    /// let no_responders = StatusCode::NO_RESPONDERS;
    /// let error = Error::from(no_responders);
    /// ```
    #[inline]
    fn from(status: StatusCode) -> Error {
        Error {
            code: None,
            status: Some(status),
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

    pub fn custom<E>(code: ErrorCode, status: StatusCode, description: String, source: E) -> Error
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        Error {
            code: Some(code),
            status: Some(status),
            description: Some(description),
            source: Some(source.into()),
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
