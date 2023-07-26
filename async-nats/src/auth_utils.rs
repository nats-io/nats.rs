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

use nkeys::KeyPair;
use once_cell::sync::Lazy;
use regex::Regex;
use std::{io, path::Path};

/// Loads user credentials file with jwt and key. Return file contents.
/// Uses tokio non-blocking io
pub(crate) async fn load_creds(path: &Path) -> io::Result<String> {
    tokio::fs::read_to_string(path).await.map_err(|err| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("loading creds file '{}': {}", path.display(), err),
        )
    })
}

/// Parses the string, expected to be formatted as credentials file
pub(crate) fn parse_jwt_and_key_from_creds(contents: &str) -> io::Result<(&str, KeyPair)> {
    let jwt = parse_decorated_jwt(contents).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "cannot parse user JWT from the credentials file",
        )
    })?;

    let nkey = parse_decorated_nkey(contents).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "cannot parse nkey from the credentials file",
        )
    })?;

    let kp =
        KeyPair::from_seed(nkey).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

    Ok((jwt, kp))
}

// This regex parses a credentials file.
//
// The credentials file is typically
// `~/.nkeys/creds/synadia/<account/<account>.creds` and looks like this:
//
// ```
// -----BEGIN NATS USER JWT-----
// eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5...
// ------END NATS USER JWT------
//
// ************************* IMPORTANT *************************
// NKEY Seed printed below can be used sign and prove identity.
// NKEYs are sensitive and should be treated as secrets.
//
// -----BEGIN USER NKEY SEED-----
// SUAIO3FHUX5PNV2LQIIP7TZ3N4L7TX3W53MQGEIVYFIGA635OZCKEYHFLM
// ------END USER NKEY SEED------
// ```
static USER_CONFIG_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\s*(?:(?:[-]{3,}.*[-]{3,}\r?\n)([\w\-.=]+)(?:\r?\n[-]{3,}.*[-]{3,}\r?\n))")
        .unwrap()
});

/// Parses a credentials file and returns its user JWT.
fn parse_decorated_jwt(contents: &str) -> Option<&str> {
    let capture = USER_CONFIG_RE.captures_iter(contents).next()?;
    Some(capture.get(1)?.as_str())
}

/// Parses a credentials file and returns its nkey.
fn parse_decorated_nkey(contents: &str) -> Option<&str> {
    let capture = USER_CONFIG_RE.captures_iter(contents).nth(1)?;
    Some(capture.get(1)?.as_str())
}
