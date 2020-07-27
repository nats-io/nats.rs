use serde::Serialize;

use crate::SecureString;

/// Info to construct a CONNECT message.
#[derive(Clone, Serialize, Debug)]
#[doc(hidden)]
#[allow(clippy::module_name_repetitions)]
pub struct ConnectInfo {
    /// Turns on +OK protocol acknowledgements.
    pub verbose: bool,

    /// Turns on additional strict format checking, e.g. for properly formed subjects.
    pub pedantic: bool,

    /// User's JWT.
    #[serde(rename = "jwt", skip_serializing_if = "is_empty_or_none")]
    pub user_jwt: Option<SecureString>,

    /// Signed nonce, encoded to Base64URL.
    #[serde(rename = "sig", skip_serializing_if = "is_empty_or_none")]
    pub signature: Option<SecureString>,

    /// Optional client name.
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub name: Option<SecureString>,

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
    pub user: Option<SecureString>,

    /// Connection password (if auth_required is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub pass: Option<SecureString>,

    /// Client authorization token (if auth_required is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub auth_token: Option<SecureString>,

    /// Whether the client supports the usage of headers.
    pub headers: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_true(field: &bool) -> bool {
    *field
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[inline]
fn is_empty_or_none(field: &Option<SecureString>) -> bool {
    match field {
        Some(inner) => inner.is_empty(),
        None => true,
    }
}
