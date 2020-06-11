use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
};

use native_tls::TlsStream;

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

pub(crate) fn split_tls(tls: TlsStream<TcpStream>) -> (TlsReader, TlsWriter) {
    let tls_reader = TlsReader {
        raw_socket: tls.get_ref().try_clone().unwrap(),
        tls: Arc::new(Mutex::new(tls)),
    };

    let tls_writer = TlsWriter {
        tls: tls_reader.tls.clone(),
    };

    (tls_reader, tls_writer)
}

#[derive(Debug)]
pub(crate) struct TlsReader {
    tls: Arc<Mutex<TlsStream<TcpStream>>>,
    raw_socket: TcpStream,
}

impl TlsReader {
    fn wait_for_readable(&self) -> io::Result<()> {
        let mut peek_buf = [0];
        self.raw_socket.peek(&mut peek_buf)?;
        Ok(())
    }
}

impl Read for TlsReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.wait_for_readable()?;

        let mut tls = self.tls.lock().unwrap();

        tls.read(buf)
    }
}

#[derive(Debug)]
pub(crate) struct TlsWriter {
    tls: Arc<Mutex<TlsStream<TcpStream>>>,
}

impl Write for TlsWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut tls = self.tls.lock().unwrap();
        tls.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut tls = self.tls.lock().unwrap();
        tls.flush()
    }
}

impl TlsWriter {
    pub(crate) fn shutdown(&mut self) -> io::Result<()> {
        let mut tls = self.tls.lock().unwrap();
        tls.shutdown()
    }
}
