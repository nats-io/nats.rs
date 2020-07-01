pub use native_tls::{Certificate, Identity, Protocol, TlsConnector, TlsConnectorBuilder};

/// Returns a new TLS configuration object for use
/// with `ConnectionOptions::set_tls_connector`.
///
/// # Examples
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let tls_connector = nats::tls::builder()
///     .identity(nats::tls::Identity::from_pkcs12(b"der_bytes", "my_password")?)
///     .add_root_certificate(nats::tls::Certificate::from_pem(b"my_pem_bytes")?)
///     .build()?;
///
/// let nc = nats::ConnectionOptions::new()
///     .tls_connector(tls_connector)
///     .connect("tls://demo.nats.io:4443")?;
/// # Ok(())
/// # }
/// ```
pub fn builder() -> TlsConnectorBuilder {
    TlsConnector::builder()
}
