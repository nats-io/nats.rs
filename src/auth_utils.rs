use std::fs;
use std::io::{self, prelude::*};
use std::path::Path;

use nkeys::KeyPair;
use once_cell::sync::Lazy;
use regex::Regex;
use rustls::{Certificate, PrivateKey};

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

    let kp = KeyPair::from_seed(&nkey)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

    Ok((jwt, kp))
}

/// Signs nonce using a credentials file.
pub(crate) fn sign_nonce(
    nonce: &[u8],
    key_pair: &KeyPair,
) -> io::Result<SecureString> {
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

/// Extract and decode all PEM sections from `rd`, which begin with `start_mark`
/// and end with `end_mark`.  Apply the functor `f` to each decoded buffer,
/// and return a Vec of `f`'s return values.
fn extract<A>(
    path: &Path,
    start_mark: &str,
    end_mark: &str,
    f: &dyn Fn(Vec<u8>) -> A,
) -> io::Result<Vec<A>> {
    let contents = fs::read(path)?;
    let mut rd = io::Cursor::new(contents);

    let mut ders = Vec::new();
    let mut b64buf = String::new();
    let mut take_base64 = false;

    let mut raw_line = Vec::<u8>::new();
    loop {
        raw_line.clear();
        let len = rd
            .read_until(b'\n', &mut raw_line)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

        if len == 0 {
            return Ok(ders);
        }
        let line = String::from_utf8_lossy(&raw_line);

        if line.starts_with(start_mark) {
            take_base64 = true;
            continue;
        }

        if line.starts_with(end_mark) {
            take_base64 = false;
            let der = base64::decode(&b64buf).map_err(|err| {
                io::Error::new(io::ErrorKind::InvalidData, err)
            })?;
            ders.push(f(der));
            b64buf = String::new();
            continue;
        }

        if take_base64 {
            b64buf.push_str(line.trim());
        }
    }
}

/// Loads client certificates from a `.pem` file.
pub(crate) fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
    extract(
        path,
        "-----BEGIN CERTIFICATE-----",
        "-----END CERTIFICATE-----",
        &|v| Certificate(v),
    )
}

/// Loads client key from a `.pem` file.
pub(crate) fn load_key(path: &Path) -> io::Result<PrivateKey> {
    let mut keys = extract(
        path,
        "-----BEGIN PRIVATE KEY-----",
        "-----END PRIVATE KEY-----",
        &|v| PrivateKey(v),
    )?;
    if keys.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no keys found in the client key file",
        ));
    }
    Ok(keys.remove(0))
}
