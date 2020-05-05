use serde::Serialize;

/// Info to construct a CONNECT message.
#[derive(Clone, Serialize, Debug)]
pub(crate) struct ConnectInfo {
    /// Turns on +OK protocol acknowledgements.
    pub verbose: bool,

    /// Turns on additional strict format checking, e.g. for properly formed subjects.
    pub pedantic: bool,

    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub user_jwt: Option<String>,

    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub nkey: Option<String>,

    /// Optional client name.
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub name: Option<String>,

    /// If set to `true`, the server (version 1.2.0+) will not send originating messages from this
    /// connection to its own subscriptions. Clients should set this to `true` only for server
    /// supporting this feature, which is when proto in the INFO protocol is set to at least 1.
    #[serde(skip_serializing_if = "is_true")]
    pub echo: bool,

    /// The implementation language of the client.
    pub lang: String,

    /// The version of the client.
    pub version: String,

    /// Indicates whether the client requires an SSL connection.
    #[serde(default)]
    pub tls_required: bool,

    /// Connection username (if `auth_required` is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub user: Option<String>,

    /// Connection password (if auth_required is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub pass: Option<String>,

    /// Client authorization token (if auth_required is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub auth_token: Option<String>,

    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub sig: Option<String>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_true(field: &bool) -> bool {
    *field
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[inline]
fn is_empty_or_none(field: &Option<String>) -> bool {
    match field {
        Some(inner) => inner.is_empty(),
        None => true,
    }
}
