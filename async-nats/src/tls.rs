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
use crate::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use crate::tls;
use rustls_pki_types::pem::PemObject;
use std::io::{self, BufReader, ErrorKind};
use std::path::PathBuf;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

/// Loads client certificates from a `.pem` file.
/// If the pem file is found, but does not contain any certificates, it will return
/// empty set of Certificates, not error.
/// Can be used to parse only client certificates from .pem file containing both client key and certs.
pub(crate) async fn load_certs(path: PathBuf) -> io::Result<Vec<CertificateDer<'static>>> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(path)?;
        let reader = BufReader::new(file);
        CertificateDer::pem_reader_iter(reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!("could not load certificates: {}", e),
                )
            })
    })
    .await?
}

/// Loads client key from a `.pem` file.
/// Can be used to parse only client key from .pem file containing both client key and certs.
pub(crate) async fn load_key(path: PathBuf) -> io::Result<PrivateKeyDer<'static>> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);
        PrivateKeyDer::from_pem_reader(&mut reader).map_err(|e| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("could not load private key: {}", e),
            )
        })
    })
    .await?
}

/// Whether TLS falls back on the platform's root certificates: the user gave neither root
/// certificates nor a rustls client config.
pub(crate) fn needs_platform_roots(options: &ConnectorOptions) -> bool {
    options.tls_client_config.is_none() && options.certificates.is_empty()
}

/// How to get roots in a build without the `native-certs` feature, for the errors that say
/// there are none.
#[cfg(not(feature = "native-certs"))]
pub(crate) const NO_ROOTS_WAY_OUT: &str = "enable the `native-certs` feature of async-nats for \
    the platform's roots, or pass roots with add_root_certificates() or tls_client_config()";

/// Adds the platform's root certificates to `root_store`.
#[cfg(feature = "native-certs")]
fn add_native_roots(root_store: &mut RootCertStore) -> io::Result<()> {
    let certs_result = rustls_native_certs::load_native_certs();
    if !certs_result.errors.is_empty() {
        let errors = certs_result
            .errors
            .into_iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join("\n");
        return Err(io::Error::other(format!(
            "could not load platform certs: {errors}"
        )));
    }
    root_store.add_parsable_certificates(certs_result.certs);
    Ok(())
}

/// Without the `native-certs` feature there are no platform roots to fall back on. The
/// connector refuses TLS without roots before it gets here (see `refused_tls` and
/// `tls_handshake` in connector.rs); this stays as the last line.
#[cfg(not(feature = "native-certs"))]
fn add_native_roots(_root_store: &mut RootCertStore) -> io::Result<()> {
    Err(io::Error::new(
        ErrorKind::NotFound,
        format!("no root certificates to verify the server with; {NO_ROOTS_WAY_OUT}"),
    ))
}

pub(crate) async fn config_tls(options: &ConnectorOptions) -> io::Result<ClientConfig> {
    // use provided ClientConfig or built it from options.
    let tls_config = {
        if let Some(config) = &options.tls_client_config {
            // The user's config brings its own roots, so the platform's are not loaded.
            Ok(config.to_owned())
        } else {
            let mut root_store = RootCertStore::empty();
            // Fall back on the platform's roots only when the user gave none.
            if needs_platform_roots(options) {
                add_native_roots(&mut root_store)?;
            }
            // Include user-provided certificates.
            for cafile in &options.certificates {
                let trust_anchors = load_certs(cafile.to_owned())
                    .await?
                    .into_iter()
                    .map(|cert| {
                        rustls_webpki::anchor_from_trusted_cert(&cert).map(|ta| ta.to_owned())
                    })
                    .collect::<Result<Vec<_>, rustls_webpki::Error>>()
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
                    builder
                        .with_client_auth_cert(cert, key)
                        .map_err(|_| io::Error::other("could not add certificate or key"))
                } else {
                    Err(io::Error::other("found certificate, but no key"))
                }
            } else {
                // if there are no client certs provided, connect with just TLS.
                Ok(builder.with_no_client_auth())
            }
        }
    }?;
    Ok(tls_config)
}
