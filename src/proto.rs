use std::convert::TryFrom;
use std::io::prelude::*;
use std::io::{self, Error, ErrorKind};
use std::str::{self, FromStr};

use crate::connect::ConnectInfo;
use crate::{inject_io_failure, Headers, ServerInfo};

/// A protocol operation sent by the server.
#[derive(Debug)]
pub(crate) enum ServerOp {
    /// `INFO {["option_name":option_value],...}`
    Info(ServerInfo),

    /// `MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n`
    Msg {
        subject: String,
        sid: u64,
        reply_to: Option<String>,
        payload: Vec<u8>,
    },

    /// `HMSG <subject> <sid> [reply-to] <# header bytes> <# total
    /// bytes>\r\n<version line>\r\n[headers]\r\n\r\n[payload]\r\n`
    Hmsg {
        subject: String,
        headers: Headers,
        sid: u64,
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

// adapted from `std::io::BufRead::read_until`, made
// to use a fixed buffer instead of a growable vector.
fn read_line<R: BufRead + ?Sized>(
    r: &mut R,
    buf: &mut [u8],
) -> io::Result<usize> {
    let mut read = 0;
    loop {
        let available = match r.fill_buf() {
            Ok(n) => n,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        let (done, len) = {
            if let Some(i) = memchr::memchr(b'\n', available) {
                (true, i + 1)
            } else {
                buf[read..read + available.len()].copy_from_slice(available);
                (false, available.len())
            }
        };
        if len + read > buf.len() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "received command exceeded fixed command buffer",
            ));
        }
        buf[read..read + len].copy_from_slice(&available[..len]);
        r.consume(len);
        read += len;
        if done || len == 0 {
            return Ok(read);
        }
    }
}

/// Decodes a single operation from the server.
///
/// If the connection is closed, `None` will be returned.
pub(crate) fn decode(mut stream: impl BufRead) -> io::Result<Option<ServerOp>> {
    // Inject random I/O failures when testing.
    inject_io_failure()?;

    // Read a line, which should be human readable.
    #[allow(unsafe_code)]
    #[allow(clippy::uninit_assumed_init)]
    let mut command_buf: [u8; 4096] =
        unsafe { std::mem::MaybeUninit::uninit().assume_init() };

    let command_len = read_line(&mut stream, &mut command_buf)?;
    if command_len == 0 {
        // If zero bytes were read, the connection is closed.
        return Ok(None);
    }

    // Convert into a UTF8 string for simpler parsing.
    let line = str::from_utf8(&command_buf[..command_len])
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err))?;

    let op = line
        .split_ascii_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();

    if op == "PING" {
        return Ok(Some(ServerOp::Ping));
    }

    if op == "PONG" {
        return Ok(Some(ServerOp::Pong));
    }

    if op == "INFO" {
        // Parse the JSON-formatted server information.
        let server_info =
            ServerInfo::parse(&line["INFO".len()..]).ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "cannot parse server info")
            })?;

        return Ok(Some(ServerOp::Info(server_info)));
    }

    if op == "MSG" {
        // Extract whitespace-delimited arguments that come after "MSG".
        let args = line["MSG".len()..]
            .split_whitespace()
            .filter(|s| !s.is_empty());
        let args = args.collect::<Vec<_>>();

        // Parse the operation syntax: MSG <subject> <sid> [reply-to] <#bytes>
        let (subject, sid, reply_to, num_bytes) = match args[..] {
            [subject, sid, num_bytes] => (subject, sid, None, num_bytes),
            [subject, sid, reply_to, num_bytes] => {
                (subject, sid, Some(reply_to), num_bytes)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid number of arguments after MSG",
                ));
            }
        };

        // Convert the slice into an owned string.
        let subject = subject.to_string();

        // Parse the subject ID.
        let sid = u64::from_str(sid).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse sid argument after MSG",
            )
        })?;

        // Convert the slice into an owned string.
        let reply_to = reply_to.map(ToString::to_string);

        // Parse the number of payload bytes.
        let num_bytes = u32::from_str(num_bytes).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse the number of bytes argument after MSG",
            )
        })?;

        // Read the payload.
        let mut payload = Vec::new();
        payload.resize(num_bytes as usize, 0_u8);
        stream.read_exact(&mut payload[..])?;
        // Read "\r\n".
        stream.read_exact(&mut [0_u8; 2])?;

        return Ok(Some(ServerOp::Msg {
            subject,
            sid,
            reply_to,
            payload,
        }));
    }

    if op == "HMSG" {
        // Extract whitespace-delimited arguments that come after "HMSG".
        let args = line["HMSG".len()..]
            .split_whitespace()
            .filter(|s| !s.is_empty());
        let args = args.collect::<Vec<_>>();

        // Parse the operation syntax:
        // `HMSG <subject> <sid> [reply-to] <# header bytes>
        // <# total bytes>\r\n<version line>\r\n[headers]\r\n\r\n[payload]\r\n`
        let (subject, sid, reply_to, num_header_bytes, num_bytes) = match args[..]
        {
            [subject, sid, num_header_bytes, num_bytes] => {
                (subject, sid, None, num_header_bytes, num_bytes)
            }
            [subject, sid, reply_to, num_header_bytes, num_bytes] => {
                (subject, sid, Some(reply_to), num_header_bytes, num_bytes)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid number of arguments after HMSG",
                ));
            }
        };

        // Convert the slice into an owned string.
        let subject = subject.to_string();

        // Parse the subject ID.
        let sid = u64::from_str(sid).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse sid argument after HMSG",
            )
        })?;

        // Convert the slice into an owned string.
        let reply_to = reply_to.map(ToString::to_string);

        // Parse the number of payload bytes.
        let num_header_bytes =
            u32::from_str(num_header_bytes).map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "cannot parse the number of header bytes argument after \
                     HMSG",
                )
            })?;

        // Parse the number of payload bytes.
        let num_bytes = u32::from_str(num_bytes).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "cannot parse the number of bytes argument after HMSG",
            )
        })?;

        if num_bytes < num_header_bytes {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "number of header bytes was greater than or equal to the \
                 total number of bytes after HMSG",
            ));
        }

        let num_payload_bytes = num_bytes - num_header_bytes;

        // `HMSG <subject> <sid> [reply-to]
        // <# header bytes> <# total bytes>\r\n
        // <version line>\r\n[headers]\r\n\r\n[payload]\r\n`

        // Read the header payload.
        let mut header_payload = Vec::new();
        header_payload.resize(num_header_bytes as usize, 0_u8);
        stream.read_exact(&mut header_payload[..])?;

        let headers = Headers::try_from(&*header_payload)?;

        // Read the payload.
        let mut payload = Vec::new();
        payload.resize(num_payload_bytes as usize, 0_u8);
        stream.read_exact(&mut payload[..])?;
        // Read "\r\n".
        stream.read_exact(&mut [0_u8; 2])?;

        return Ok(Some(ServerOp::Hmsg {
            subject,
            headers,
            sid,
            reply_to,
            payload,
        }));
    }

    if op == "-ERR" {
        // Extract the message argument.
        let msg = line["-ERR".len()..].trim().trim_matches('\'').to_string();

        return Ok(Some(ServerOp::Err(msg)));
    }

    Ok(Some(ServerOp::Unknown(line.to_owned())))
}

/// A protocol operation sent by the client.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ClientOp<'a> {
    /// `CONNECT {["option_name":option_value],...}`
    Connect(&'a ConnectInfo),

    /// `PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n`
    Pub {
        subject: &'a str,
        reply_to: Option<&'a str>,
        payload: &'a [u8],
    },

    /// `HPUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n`
    Hpub {
        subject: &'a str,
        reply_to: Option<&'a str>,
        headers: &'a Headers,
        payload: &'a [u8],
    },

    /// `SUB <subject> [queue group] <sid>\r\n`
    Sub {
        subject: &'a str,
        queue_group: Option<&'a str>,
        sid: u64,
    },

    /// `UNSUB <sid> [max_msgs]`
    Unsub { sid: u64, max_msgs: Option<u64> },

    /// `PING`
    Ping,

    /// `PONG`
    Pong,
}

/// Encodes a single operation from the client.
pub(crate) fn encode(
    mut stream: impl Write,
    op: ClientOp<'_>,
) -> io::Result<()> {
    match &op {
        ClientOp::Connect(connect_info) => {
            let op = format!(
                "CONNECT {}\r\n",
                connect_info.dump().ok_or_else(|| Error::new(
                    ErrorKind::InvalidData,
                    "cannot serialize connect info"
                ))?
            );
            stream.write_all(op.as_bytes())?;
        }

        ClientOp::Pub {
            subject,
            reply_to,
            payload,
        } => {
            stream.write_all(b"PUB ")?;
            stream.write_all(subject.as_bytes())?;
            stream.write_all(b" ")?;

            if let Some(reply_to) = reply_to {
                stream.write_all(reply_to.as_bytes())?;
                stream.write_all(b" ")?;
            }

            let mut buf = itoa::Buffer::new();
            stream.write_all(buf.format(payload.len()).as_bytes())?;
            stream.write_all(b"\r\n")?;

            stream.write_all(payload)?;
            stream.write_all(b"\r\n")?;
        }

        ClientOp::Hpub {
            subject,
            reply_to,
            headers,
            payload,
        } => {
            stream.write_all(b"HPUB ")?;
            stream.write_all(subject.as_bytes())?;
            stream.write_all(b" ")?;

            if let Some(reply_to) = reply_to {
                stream.write_all(reply_to.as_bytes())?;
                stream.write_all(b" ")?;
            }

            let header_bytes = headers.to_bytes();

            let header_len = header_bytes.len();
            let total_len = header_len + payload.len();

            let mut hlen_buf = itoa::Buffer::new();
            stream.write_all(hlen_buf.format(header_len).as_bytes())?;

            stream.write_all(b" ")?;

            let mut tlen_buf = itoa::Buffer::new();
            stream.write_all(tlen_buf.format(total_len).as_bytes())?;

            stream.write_all(b"\r\n")?;

            stream.write_all(&header_bytes)?;
            stream.write_all(payload)?;
            stream.write_all(b"\r\n")?;
        }

        ClientOp::Sub {
            subject,
            queue_group,
            sid,
        } => {
            let op = if let Some(queue_group) = queue_group {
                format!("SUB {} {} {}\r\n", subject, queue_group, sid)
            } else {
                format!("SUB {} {}\r\n", subject, sid)
            };
            stream.write_all(op.as_bytes())?;
        }

        ClientOp::Unsub { sid, max_msgs } => {
            let op = if let Some(max_msgs) = max_msgs {
                format!("UNSUB {} {}\r\n", sid, max_msgs)
            } else {
                format!("UNSUB {}\r\n", sid)
            };
            stream.write_all(op.as_bytes())?;
        }

        ClientOp::Ping => {
            stream.write_all(b"PING\r\n")?;
        }

        ClientOp::Pong => {
            stream.write_all(b"PONG\r\n")?;
        }
    }

    Ok(())
}
