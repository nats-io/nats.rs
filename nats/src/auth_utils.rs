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

use std::fs;
use std::io::{self, BufReader, ErrorKind};
use std::path::Path;

use nkeys::KeyPair;
use once_cell::sync::Lazy;
use regex::Regex;
use webpki::types::{CertificateDer, PrivateKeyDer};

use crate::SecureString;

/// Loads the user JWT and nkey from a `.creds` file.
pub(crate) fn load_creds(path: &Path) -> io::Result<(SecureString, KeyPair)> {
    // Load the private nkey.
    let contents = SecureString::from(fs::read_to_string(path)?);
    jwt_kp(&contents)
}

pub(crate) fn jwt_kp(contents: &str) -> io::Result<(SecureString, KeyPair)> {
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
        KeyPair::from_seed(&nkey).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

    Ok((jwt, kp))
}

/// Signs nonce using a credentials file.
pub(crate) fn sign_nonce(nonce: &[u8], key_pair: &KeyPair) -> io::Result<SecureString> {
    // Use the nkey to sign the nonce.
    let sig = key_pair
        .sign(nonce)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

    // Encode the signature to Base64URL.
    Ok(SecureString::from(base64_url::encode(&sig)))
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
fn parse_decorated_jwt(contents: &str) -> Option<SecureString> {
    let capture = USER_CONFIG_RE.captures_iter(contents).next()?;
    Some(SecureString::from(capture[1].to_string()))
}

/// Parses a credentials file and returns its nkey.
fn parse_decorated_nkey(contents: &str) -> Option<SecureString> {
    let capture = USER_CONFIG_RE.captures_iter(contents).nth(1)?;
    Some(SecureString::from(capture[1].to_string()))
}

/// Loads client certificates from a `.pem` file.
/// If the pem file is found, but does not contain any certificates, it will return
/// empty set of Certificates, not error.
/// Can be used to parse only client certificates from .pem file containing both client key and certs.
pub(crate) fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader).collect::<io::Result<Vec<_>>>()
}

/// Loads client key from a `.pem` file.
/// Can be used to parse only client key from .pem file containing both client key and certs.
pub(crate) fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);

    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "could not find client key in the path"))
}
