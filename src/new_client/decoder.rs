//! Decode bytes received from the server to protocol operations.

use std::io::{self, Error, ErrorKind};
use std::str::FromStr;

use futures::prelude::*;

use crate::{inject_io_failure, ServerInfo};

/// A protocol operation sent by the server.
#[derive(Debug)]
pub(crate) enum ServerOp {
    /// `INFO {["option_name":option_value],...}`
    Info(ServerInfo),

    /// `MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n`
    Msg {
        subject: String,
        sid: usize,
        reply_to: Option<String>,
        payload: Vec<u8>,
    },

    /// `PING`
    Ping,

    /// `PONG`
    Pong,

    /// `-ERR <error message>`
    Err(String),

    /// Unknown protocol message.
    Unknown(String),
}

/// Decodes a single operation from the server.
pub(crate) async fn decode(mut stream: impl AsyncBufRead + Unpin) -> io::Result<ServerOp> {
    // Inject random I/O failures when testing.
    inject_io_failure()?;

    // Read a line, which should be human readable.
    let mut line = Vec::new();
    stream.read_until(b'\n', &mut line).await?;

    // Convert into a UTF8 string for simpler parsing.
    let line = String::from_utf8(line).map_err(|err| Error::new(ErrorKind::InvalidInput, err))?;
    let line_uppercase = line.trim().to_uppercase();

    if line_uppercase.starts_with("PING") {
        return Ok(ServerOp::Ping);
    }

    if line_uppercase.starts_with("PONG") {
        return Ok(ServerOp::Pong);
    }

    if line_uppercase.starts_with("INFO") {
        // Parse the JSON-formatted server information.
        let server_info = serde_json::from_slice(&line["INFO".len()..].as_bytes())
            .map_err(|err| Error::new(ErrorKind::InvalidInput, err))?;

        return Ok(ServerOp::Info(server_info));
    }

    if line_uppercase.starts_with("MSG") {
        // Extract whitespace-delimited arguments that come after "MSG".
        let args = line["MSG".len()..]
            .split_whitespace()
            .filter(|s| !s.is_empty());
        let args = args.collect::<Vec<_>>();

        // Parse the operation syntax: MSG <subject> <sid> [reply-to] <#bytes>
        let (subject, sid, reply_to, num_bytes) = match args[..] {
            [subject, sid, num_bytes] => (subject, sid, None, num_bytes),
            [subject, sid, reply_to, num_bytes] => (subject, sid, Some(reply_to), num_bytes),
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid number of arguments after MSG",
                ))
            }
        };

        // Convert the slice into an owned string.
        let subject = subject.to_string();

        // Parse the subject ID.
        let sid = usize::from_str(sid).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse sid argument after MSG",
            )
        })?;

        // Convert the slice into an owned string.
        let reply_to = reply_to.map(|s| s.to_string());

        // Parse the number of payload bytes.
        let num_bytes = u32::from_str(num_bytes).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse the number of bytes argument after MSG",
            )
        })?;

        // Read the payload.
        let mut payload = Vec::new();
        payload.resize(num_bytes as usize, 0u8);
        stream.read_exact(&mut payload[..]).await?;

        return Ok(ServerOp::Msg {
            subject,
            sid,
            reply_to,
            payload,
        });
    }

    if line_uppercase.starts_with("-ERR") {
        // Extract the message argument.
        let msg = line["-ERR".len()..].trim().trim_matches('\'').to_string();

        return Ok(ServerOp::Err(msg));
    }

    Ok(ServerOp::Unknown(line))
}
