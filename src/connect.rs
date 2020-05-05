use std::{
    io::{self, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
};

use serde::Serialize;

use crate::{
    inject_io_failure,
    parser::ControlOp,
    parser::{expect_info, parse_control_op},
    split_tls, AuthStyle, FinalizedOptions, Reader, SecureString, ServerInfo, Writer,
};

/// Info to construct a CONNECT message.
#[derive(Clone, Serialize, Debug)]
pub(crate) struct ConnectInfo {
    /// Turns on +OK protocol acknowledgements.
    pub verbose: bool,

    /// Turns on additional strict format checking, e.g. for properly formed subjects.
    pub pedantic: bool,

    /// User's JWT.
    #[serde(rename = "jwt", skip_serializing_if = "is_empty_or_none")]
    pub user_jwt: Option<String>,

    /// Signed nonce, encoded to Base64URL.
    #[serde(rename = "sig", skip_serializing_if = "secure_is_empty_or_none")]
    pub signature: Option<SecureString>,

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
    #[serde(skip_serializing_if = "secure_is_empty_or_none")]
    pub pass: Option<SecureString>,

    /// Client authorization token (if auth_required is set)
    #[serde(skip_serializing_if = "secure_is_empty_or_none")]
    pub auth_token: Option<SecureString>,
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

#[allow(clippy::trivially_copy_pass_by_ref)]
#[inline]
fn secure_is_empty_or_none(field: &Option<SecureString>) -> bool {
    match field {
        Some(inner) => inner.is_empty(),
        None => true,
    }
}

/// Attempts to connect to a server on a single `SocketAddr`.
pub(crate) fn connect_to_socket_addr(
    addr: SocketAddr,
    host: String,
    tls_required: bool,
    options: FinalizedOptions,
) -> io::Result<(Reader, Writer, ServerInfo)> {
    inject_io_failure()?;

    // Connect to the socket and read the initial INFO message.
    let mut stream = TcpStream::connect(&addr)?;
    let server_info = expect_info(&mut stream)?;

    // Send back a CONNECT message to authenticate the client.
    let (mut reader, writer) = authenticate(
        stream,
        server_info.clone(),
        options.clone(),
        tls_required,
        host.clone(),
    )?;

    let parsed_op = parse_control_op(&mut reader)?;

    match parsed_op {
        ControlOp::Pong => Ok((reader, writer, server_info)),
        ControlOp::Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
        ControlOp::Ping | ControlOp::Msg(_) | ControlOp::Info(_) | ControlOp::Unknown(_) => {
            log::error!(
                "encountered unexpected control op during connection: {:?}",
                parsed_op
            );
            Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "Protocol Error",
            ))
        }
    }
}

fn authenticate(
    stream: TcpStream,
    server_info: ServerInfo,
    options: FinalizedOptions,
    tls_required: bool,
    host: String,
) -> io::Result<(Reader, Writer)> {
    // Data that will be formatted as a CONNECT message.
    let mut connect_info = ConnectInfo {
        tls_required: tls_required,
        name: options.name.to_owned(),
        pedantic: false,
        verbose: false,
        lang: crate::LANG.to_string(),
        version: crate::VERSION.to_string(),
        user: None,
        pass: None,
        auth_token: None,
        user_jwt: None,
        signature: None,
        echo: !options.no_echo,
    };

    // Fill in the info that authenticates the client.
    match &options.auth {
        AuthStyle::UserPass(user, pass) => {
            connect_info.user = Some(user.to_string());
            connect_info.pass = Some(pass.to_string().into());
        }
        AuthStyle::Token(token) => {
            connect_info.auth_token = Some(token.to_string().into());
        }
        AuthStyle::Credentials { jwt_cb, sig_cb } => {
            let jwt = jwt_cb()?;
            let sig = sig_cb(server_info.nonce.as_bytes())?;
            connect_info.user_jwt = Some(jwt);
            connect_info.signature = Some(sig);
        }
        AuthStyle::None => {}
    }

    // Format the data as a CONNECT message followed by a PING.
    let op = format!(
        "CONNECT {}\r\nPING\r\n",
        serde_json::to_string(&connect_info)?
    );

    // potentially upgrade to TLS
    let (reader, mut writer) = if options.tls_required || server_info.tls_required || tls_required {
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

    Ok((reader, writer))
}
