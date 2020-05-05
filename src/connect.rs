use std::{
    io::{self, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
};

use nkeys::KeyPair;
use serde::Serialize;

use crate::{
    inject_io_failure, parser::parse_control_op, parser::ControlOp, split_tls, FinalizedOptions,
    Reader, ServerInfo, Writer,
};

/// Info to construct a CONNECT message.
#[derive(Clone, Serialize, Debug)]
pub(crate) struct ConnectInfo {
    /// Turns on +OK protocol acknowledgements.
    pub verbose: bool,

    /// Turns on additional strict format checking, e.g. for properly formed subjects.
    pub pedantic: bool,

    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub user_jwt: Option<String>,

    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub nkey: Option<String>,

    /// Optional client name.
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub name: Option<String>,

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
    pub user: Option<String>,

    /// Connection password (if auth_required is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub pass: Option<String>,

    /// Client authorization token (if auth_required is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub auth_token: Option<String>,

    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub sig: Option<String>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_true(field: &bool) -> bool {
    *field
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[inline]
fn is_empty_or_none(field: &Option<String>) -> bool {
    match field {
        Some(inner) => inner.is_empty(),
        None => true,
    }
}

/// Attempts to connect to a server using a single address.
pub(crate) fn connect_to_addr(
    host: String,
    tls_required: bool,
    options: &FinalizedOptions,
    addr: SocketAddr,
    mut connect_op: ConnectInfo,
    nkey: Option<&KeyPair>,
) -> io::Result<(Reader, Writer, ServerInfo)> {
    inject_io_failure()?;
    let mut stream = TcpStream::connect(&addr)?;
    let info = crate::parser::expect_info(&mut stream)?;

    if !info.nonce.is_empty() {
        if let Some(nkey) = nkey {
            let sig = nkey
                .sign(info.nonce.as_bytes())
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
            let sig = base64_url::encode(&sig);
            connect_op.sig = Some(sig);
        }
    }

    let op = format!(
        "CONNECT {}\r\nPING\r\n",
        serde_json::to_string(&connect_op)?
    );

    // potentially upgrade to TLS
    let (mut reader, mut writer) = if options.tls_required || info.tls_required || tls_required {
        let attempt = if let Some(ref tls_connector) = options.tls_connector {
            tls_connector.connect(&host, stream)
        } else {
            match native_tls::TlsConnector::new() {
                Ok(connector) => connector.connect(&host, stream),
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            }
        };
        match attempt {
            Ok(tls) => {
                let (tls_reader, tls_writer) = split_tls(tls);
                let reader = Reader::Tls(BufReader::with_capacity(64 * 1024, tls_reader));
                let writer = Writer::Tls(BufWriter::with_capacity(64 * 1024, tls_writer));
                (reader, writer)
            }
            Err(e) => {
                log::error!("failed to upgrade TLS: {:?}", e);
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, e));
            }
        }
    } else {
        let reader = Reader::Tcp(BufReader::with_capacity(64 * 1024, stream.try_clone()?));
        let writer = Writer::Tcp(BufWriter::with_capacity(64 * 1024, stream));
        (reader, writer)
    };

    writer.write_all(op.as_bytes())?;
    writer.flush()?;
    let parsed_op = parse_control_op(&mut reader)?;

    match parsed_op {
        ControlOp::Pong => Ok((reader, writer, info)),
        ControlOp::Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
        ControlOp::Ping | ControlOp::Msg(_) | ControlOp::Info(_) | ControlOp::Unknown(_) => {
            log::error!(
                "encountered unexpected control op during connection: {:?}",
                parsed_op
            );
            Err(io::Error::new(io::ErrorKind::ConnectionRefused, "Protocol Error"))
        }
    }
}
