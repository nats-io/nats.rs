use std::{
    fs,
    io::{self, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
};

use nkeys::KeyPair;
use regex::Regex;
use serde::Serialize;

use crate::{
    inject_io_failure, parser::parse_control_op, parser::ControlOp, split_tls, AuthStyle,
    FinalizedOptions, Reader, ServerInfo, Writer,
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

    /// User's Nkey.
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub nkey: Option<String>,

    /// Signed nonce, formatted using base64 URL encoding.
    #[serde(rename = "sig", skip_serializing_if = "is_empty_or_none")]
    pub signature: Option<String>,

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

/// Attempts to connect to a server on a single `SocketAddr`.
pub(crate) fn connect_to_socket_addr(
    addr: SocketAddr,
    host: String,
    tls_required: bool,
    options: FinalizedOptions,
) -> io::Result<(Reader, Writer, ServerInfo)> {
    inject_io_failure()?;

    let mut stream = TcpStream::connect(&addr)?;
    let server_info = crate::parser::expect_info(&mut stream)?;

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
    // This regex parses a credentials file.
    //
    // The credentials file is typically `~/.nkeys/creds/synadia/<account/<account>.creds` and
    // looks as follows:
    //
    // ```
    // -----BEGIN NATS USER JWT-----
    // <public jwt>
    // ------END NATS USER JWT------
    //
    // ************************* IMPORTANT *************************
    // NKEY Seed printed below can be used to sign and prove identity.
    // NKEYs are sensitive and should be treated as secrets.
    //
    // -----BEGIN USER NKEY SEED-----
    // <private nkey>
    // ------END USER NKEY SEED------
    //
    // *************************************************************
    // ```
    const CREDS_FILE_REGEX: &str =
        r"\s*(?:(?:[-]{3,}.*[-]{3,}\r?\n)([\w\-.=]+)(?:\r?\n[-]{3,}.*[-]{3,}\r?\n))";

    let mut connect_info = ConnectInfo {
        tls_required: tls_required,
        name: options.name.to_owned(),
        nkey: None,
        pedantic: false,
        verbose: false,
        lang: crate::LANG.into(),
        version: crate::VERSION.into(),
        user: None,
        pass: None,
        auth_token: None,
        user_jwt: None,
        signature: None,
        echo: !options.no_echo,
    };

    let mut nkey = None;

    match &options.auth {
        AuthStyle::UserPass(user, pass) => {
            connect_info.user = Some(user.into());
            connect_info.pass = Some(pass.into());
        }
        AuthStyle::Token(token) => connect_info.auth_token = Some(token.into()),
        AuthStyle::Credentials(path) => {
            let contents = fs::read_to_string(&path)?;
            let re = Regex::new(CREDS_FILE_REGEX).unwrap();
            let captures = re.captures_iter(&contents).collect::<Vec<_>>();

            let (user_jwt, seed) = match &captures[..] {
                [jwt, seed, ..] => (jwt[1].to_string(), seed[1].to_string()),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("cannot parse credentials in {}", path.display()),
                    ))
                }
            };

            let key_pair = KeyPair::from_seed(&seed)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            nkey = Some(key_pair);

            let jwt = Some(user_jwt);
            connect_info.user_jwt = jwt.into();
        }
        AuthStyle::None => {}
    }

    if !server_info.nonce.is_empty() {
        if let Some(nkey) = nkey {
            let sig = nkey
                .sign(server_info.nonce.as_bytes())
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
            let sig = base64_url::encode(&sig);
            connect_info.signature = Some(sig);
        }
    }

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
