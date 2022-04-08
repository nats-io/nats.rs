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

use std::io::BufReader;
use std::io::{self, ErrorKind};
use std::path::Path;
use tokio_rustls::rustls::{Certificate, PrivateKey};

/// Loads client certificates from a `.pem` file.
/// If the pem file is found, but does not contain any certificates, it will return
/// empty set of Certificates, not error.
/// Can be used to parse only client certificates from .pem file containing both client key and certs.
pub(crate) fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)?
        .iter()
        .map(|v| Certificate(v.clone()))
        .collect();
    Ok(certs)
}

/// Loads client key from a `.pem` file.
/// Can be used to parse only client key from .pem file containing both client key and certs.
pub(crate) fn load_key(path: &Path) -> io::Result<PrivateKey> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);

    loop {
        match rustls_pemfile::read_one(&mut reader)? {
            Some(rustls_pemfile::Item::RSAKey(key))
            | Some(rustls_pemfile::Item::PKCS8Key(key))
            | Some(rustls_pemfile::Item::ECKey(key)) => return Ok(PrivateKey(key)),
            // if public key is found, don't error, just skip it and hope to find client key next.
            Some(rustls_pemfile::Item::X509Certificate(_)) | Some(_) => {}
            None => break,
        }
    }

    Err(io::Error::new(
        ErrorKind::NotFound,
        "could not find client key in the path",
    ))
}
