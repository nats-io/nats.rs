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

use crate::connector::ConnectorOptions;
use crate::tls;
use std::io::{self, BufReader, ErrorKind};
use std::path::PathBuf;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use webpki::types::{CertificateDer, PrivateKeyDer};

/// Loads client certificates from a `.pem` file.
/// If the pem file is found, but does not contain any certificates, it will return
/// empty set of Certificates, not error.
/// Can be used to parse only client certificates from .pem file containing both client key and certs.
pub(crate) async fn load_certs(path: PathBuf) -> io::Result<Vec<CertificateDer<'static>>> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);
        rustls_pemfile::certs(&mut reader).collect::<io::Result<Vec<_>>>()
    })
    .await?
}

/// Loads client key from a `.pem` file.
/// Can be used to parse only client key from .pem file containing both client key and certs.
pub(crate) async fn load_key(path: PathBuf) -> io::Result<PrivateKeyDer<'static>> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);
        rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
            io::Error::new(ErrorKind::NotFound, "could not find client key in the path")
        })
    })
    .await?
}

pub(crate) async fn config_tls(options: &ConnectorOptions) -> io::Result<ClientConfig> {
    let mut root_store = RootCertStore::empty();
    // load native system certs only if user did not specify them.
    if options.tls_client_config.is_some() || options.certificates.is_empty() {
        let certs_iter = rustls_native_certs::load_native_certs().map_err(|err| {
            io::Error::new(
                ErrorKind::Other,
                format!("could not load platform certs: {err}"),
            )
        })?;
        root_store.add_parsable_certificates(certs_iter);
    }

    // use provided ClientConfig or built it from options.
    let tls_config = {
        if let Some(config) = &options.tls_client_config {
            Ok(config.to_owned())
        } else {
            // Include user-provided certificates.
            for cafile in &options.certificates {
                let trust_anchors = load_certs(cafile.to_owned())
                    .await?
                    .into_iter()
                    .map(|cert| webpki::anchor_from_trusted_cert(&cert).map(|ta| ta.to_owned()))
                    .collect::<Result<Vec<_>, webpki::Error>>()
                    .map_err(|err| {
                        io::Error::new(
                            ErrorKind::InvalidInput,
                            format!("could not load certs: {err}"),
                        )
                    })?;
                root_store.extend(trust_anchors);
            }
            let builder = ClientConfig::builder().with_root_certificates(root_store);
            if let Some(cert) = options.client_cert.clone() {
                if let Some(key) = options.client_key.clone() {
                    let key = tls::load_key(key).await?;
                    let cert = tls::load_certs(cert).await?;
                    builder.with_client_auth_cert(cert, key).map_err(|_| {
                        io::Error::new(ErrorKind::Other, "could not add certificate or key")
                    })
                } else {
                    Err(io::Error::new(
                        ErrorKind::Other,
                        "found certificate, but no key",
                    ))
                }
            } else {
                // if there are no client certs provided, connect with just TLS.
                Ok(builder.with_no_client_auth())
            }
        }
    }?;
    Ok(tls_config)
}
