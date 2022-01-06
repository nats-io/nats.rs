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

use crate::SecureString;

/// Info to construct a CONNECT message.
#[derive(Clone, Debug)]
#[doc(hidden)]
#[allow(clippy::module_name_repetitions)]
pub struct ConnectInfo {
    /// Turns on +OK protocol acknowledgements.
    pub verbose: bool,

    /// Turns on additional strict format checking, e.g. for properly formed
    /// subjects.
    pub pedantic: bool,

    /// User's JWT.
    pub user_jwt: Option<SecureString>,

    /// Public nkey.
    pub nkey: Option<SecureString>,

    /// Signed nonce, encoded to Base64URL.
    pub signature: Option<SecureString>,

    /// Optional client name.
    pub name: Option<SecureString>,

    /// If set to `true`, the server (version 1.2.0+) will not send originating
    /// messages from this connection to its own subscriptions. Clients should
    /// set this to `true` only for server supporting this feature, which is
    /// when proto in the INFO protocol is set to at least 1.
    pub echo: bool,

    /// The implementation language of the client.
    pub lang: String,

    /// The version of the client.
    pub version: String,

    /// Sending 0 (or absent) indicates client supports original protocol.
    /// Sending 1 indicates that the client supports dynamic reconfiguration
    /// of cluster topology changes by asynchronously receiving INFO messages
    /// with known servers it can reconnect to.
    pub protocol: Protocol,

    /// Indicates whether the client requires an SSL connection.
    pub tls_required: bool,

    /// Connection username (if `auth_required` is set)
    pub user: Option<SecureString>,

    /// Connection password (if auth_required is set)
    pub pass: Option<SecureString>,

    /// Client authorization token (if auth_required is set)
    pub auth_token: Option<SecureString>,

    /// Whether the client supports the usage of headers.
    pub headers: bool,

    /// Whether the client supports no_responders.
    pub no_responders: bool,
}

/// Protocol version used by the client.
#[derive(Clone, Copy, Debug)]
pub enum Protocol {
    /// Original protocol.
    Original = 0,
    /// Protocol with dynamic reconfiguration of cluster and lame duck mode functionality.
    Dynamic = 1,
}

impl ConnectInfo {
    pub(crate) fn dump(&self) -> Option<String> {
        let mut obj = json::object! {
            verbose: self.verbose,
            pedantic: self.pedantic,
            echo: self.echo,
            lang: self.lang.clone(),
            version: self.version.clone(),
            protocol: self.protocol as u8,
            tls_required: self.tls_required,
            headers: self.headers,
            no_responders: self.no_responders,
        };
        if let Some(s) = &self.user_jwt {
            obj.insert("jwt", s.to_string()).ok()?;
        }
        if let Some(s) = &self.nkey {
            obj.insert("nkey", s.to_string()).ok()?;
        }
        if let Some(s) = &self.signature {
            obj.insert("sig", s.to_string()).ok()?;
        }
        if let Some(s) = &self.name {
            obj.insert("name", s.to_string()).ok()?;
        }
        if let Some(s) = &self.user {
            obj.insert("user", s.to_string()).ok()?;
        }
        if let Some(s) = &self.pass {
            obj.insert("pass", s.to_string()).ok()?;
        }
        if let Some(s) = &self.auth_token {
            obj.insert("auth_token", s.to_string()).ok()?;
        }
        Some(obj.dump())
    }
}
