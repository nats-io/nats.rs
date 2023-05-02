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

//! This module provides a connection implementation for communicating with a NATS server.

use std::fmt::Display;
use std::str::{self, FromStr};

use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::io::{AsyncReadExt, AsyncWrite};

use bytes::{Buf, BytesMut};
use tokio::io;

use crate::header::{HeaderMap, HeaderName};
use crate::status::StatusCode;
use crate::subject::Subject;
use crate::{ClientOp, ServerError, ServerOp};

/// Supertrait enabling trait object for containing both TLS and non TLS `TcpStream` connection.
pub(crate) trait AsyncReadWrite: AsyncWrite + AsyncRead + Send + Unpin {}

/// Blanked implementation that applies to both TLS and non-TLS `TcpStream`.
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// An enum representing the state of the connection.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum State {
    Pending,
    Connected,
    Disconnected,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Pending => write!(f, "pending"),
            State::Connected => write!(f, "connected"),
            State::Disconnected => write!(f, "disconnected"),
        }
    }
}

/// A framed connection
pub(crate) struct Connection {
    pub(crate) stream: Box<dyn AsyncReadWrite>,
    pub(crate) buffer: BytesMut,
}

/// Internal representation of the connection.
/// Holds connection with NATS Server and communicates with `Client` via channels.
impl Connection {
    /// Attempts to read a server operation from the read buffer.
    /// Returns `None` if there is not enough data to parse an entire operation.
    pub(crate) fn try_read_op(&mut self) -> Result<Option<ServerOp>, io::Error> {
        let maybe_len = memchr::memmem::find(&self.buffer, b"\r\n");
        if maybe_len.is_none() {
            return Ok(None);
        }

        let len = maybe_len.unwrap();

        if self.buffer.starts_with(b"+OK") {
            self.buffer.advance(len + 2);
            return Ok(Some(ServerOp::Ok));
        }

        if self.buffer.starts_with(b"PING") {
            self.buffer.advance(len + 2);
            return Ok(Some(ServerOp::Ping));
        }

        if self.buffer.starts_with(b"PONG") {
            self.buffer.advance(len + 2);
            return Ok(Some(ServerOp::Pong));
        }

        if self.buffer.starts_with(b"-ERR") {
            let description = str::from_utf8(&self.buffer[5..len])
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?
                .trim_matches('\'')
                .to_string();

            self.buffer.advance(len + 2);

            return Ok(Some(ServerOp::Error(ServerError::new(description))));
        }

        if self.buffer.starts_with(b"INFO ") {
            let info = serde_json::from_slice(&self.buffer[4..len])
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

            self.buffer.advance(len + 2);

            return Ok(Some(ServerOp::Info(Box::new(info))));
        }

        if self.buffer.starts_with(b"MSG ") {
            let line = str::from_utf8(&self.buffer[4..len]).unwrap();
            let mut args = line.split(' ').filter(|s| !s.is_empty());

            // Parse the operation syntax: MSG <subject> <sid> [reply] <#bytes>
            let subject = args.next();
            let sid = args.next();
            let mut reply = args.next();
            let mut payload_len = args.next();
            if payload_len.is_none() {
                std::mem::swap(&mut reply, &mut payload_len);
            }

            if subject.is_none() || sid.is_none() || payload_len.is_none() || args.next().is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid number of arguments after MSG",
                ));
            }

            let sid = u64::from_str(sid.unwrap())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

            // Parse the number of payload bytes.
            let payload_len = usize::from_str(payload_len.unwrap())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

            // Return early without advancing if there is not enough data read the entire
            // message
            if len + payload_len + 4 > self.buffer.remaining() {
                return Ok(None);
            }

            let subject = Subject::from(subject.unwrap());
            let reply = reply.map(Subject::from);

            self.buffer.advance(len + 2);
            let payload = self.buffer.split_to(payload_len).freeze();
            self.buffer.advance(2);

            return Ok(Some(ServerOp::Message {
                sid,
                length: payload_len
                    + reply.as_ref().map(|reply| reply.len()).unwrap_or(0)
                    + subject.len(),
                reply,
                headers: None,
                subject,
                payload,
                status: None,
                description: None,
            }));
        }

        if self.buffer.starts_with(b"HMSG ") {
            // Extract whitespace-delimited arguments that come after "HMSG".
            let line = std::str::from_utf8(&self.buffer[5..len]).unwrap();
            let mut args = line.split_whitespace().filter(|s| !s.is_empty());

            // <subject> <sid> [reply] <# header bytes><# total bytes>
            let subject = args.next();
            let sid = args.next();
            let mut reply = args.next();
            let mut num_header_bytes = args.next();
            let mut num_bytes = args.next();
            if num_bytes.is_none() {
                std::mem::swap(&mut num_header_bytes, &mut num_bytes);
                std::mem::swap(&mut reply, &mut num_header_bytes);
            }

            if subject.is_none()
                || sid.is_none()
                || num_header_bytes.is_none()
                || num_bytes.is_none()
                || args.next().is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid number of arguments after HMSG",
                ));
            }

            // Convert the slice into an owned string.
            let subject = Subject::from(subject.unwrap());

            // Parse the subject ID.
            let sid = u64::from_str(sid.unwrap()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot parse sid argument after HMSG",
                )
            })?;

            // Convert the slice into a subject.
            let reply = reply.map(Subject::from);

            // Parse the number of payload bytes.
            let num_header_bytes = usize::from_str(num_header_bytes.unwrap()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot parse the number of header bytes argument after \
                     HMSG",
                )
            })?;

            // Parse the number of payload bytes.
            let num_bytes = usize::from_str(num_bytes.unwrap()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot parse the number of bytes argument after HMSG",
                )
            })?;

            if num_bytes < num_header_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "number of header bytes was greater than or equal to the \
                 total number of bytes after HMSG",
                ));
            }

            if len + num_bytes + 4 > self.buffer.remaining() {
                return Ok(None);
            }

            self.buffer.advance(len + 2);
            let buffer = self.buffer.split_to(num_header_bytes).freeze();
            let payload = self.buffer.split_to(num_bytes - num_header_bytes).freeze();
            self.buffer.advance(2);

            let mut lines = std::str::from_utf8(&buffer).unwrap().lines().peekable();
            let version_line = lines.next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "no header version line found")
            })?;

            if !version_line.starts_with("NATS/1.0") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "header version line does not begin with nats/1.0",
                ));
            }

            let mut maybe_status: Option<StatusCode> = None;
            let mut maybe_description: Option<String> = None;
            if let Some(slice) = version_line.get("NATS/1.0".len()..).map(|s| s.trim()) {
                match slice.split_once(' ') {
                    Some((status, description)) => {
                        if !status.is_empty() {
                            maybe_status = Some(status.trim().parse().map_err(|_| {
                                std::io::Error::new(
                                    io::ErrorKind::Other,
                                    "could not covert Description header into header value",
                                )
                            })?);
                        }
                        if !description.is_empty() {
                            maybe_description = Some(description.trim().to_string());
                        }
                    }
                    None => {
                        if !slice.is_empty() {
                            maybe_status = Some(slice.trim().parse().map_err(|_| {
                                std::io::Error::new(
                                    io::ErrorKind::Other,
                                    "could not covert Description header into header value",
                                )
                            })?);
                        }
                    }
                }
            }

            let mut headers = HeaderMap::new();
            while let Some(line) = lines.next() {
                if line.is_empty() {
                    continue;
                }

                let (key, value) = line.split_once(':').ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "no header version line found")
                })?;

                let mut value = value.to_owned();
                while let Some(v) = lines.next_if(|s| s.starts_with(char::is_whitespace)) {
                    value.push_str(v);
                }

                let name = HeaderName::from_str(key)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

                headers.append(name, value.trim().to_string());
            }

            return Ok(Some(ServerOp::Message {
                length: reply.as_ref().map(|reply| reply.len()).unwrap_or(0)
                    + subject.len()
                    + num_bytes,
                sid,
                reply,
                subject,
                headers: Some(headers),
                payload,
                status: maybe_status,
                description: maybe_description,
            }));
        }

        let buffer = self.buffer.split_to(len + 2);
        let line = str::from_utf8(&buffer).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "unable to parse unknown input")
        })?;

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid server operation: '{line}'"),
        ))
    }

    // TODO: do we want an custom error here?
    /// Read a server operation from read buffer.
    /// Blocks until an operation ca be parsed.
    pub(crate) async fn read_op(&mut self) -> Result<Option<ServerOp>, io::Error> {
        loop {
            if let Some(op) = self.try_read_op()? {
                return Ok(Some(op));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(io::ErrorKind::ConnectionReset, ""));
                }
            }
        }
    }

    /// Writes a client operation to the write buffer.
    pub(crate) async fn write_op<'a>(&mut self, item: &'a ClientOp) -> Result<(), io::Error> {
        match item {
            ClientOp::Connect(connect_info) => {
                let op = format!(
                    "CONNECT {}\r\n",
                    serde_json::to_string(&connect_info)
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
                );
                self.stream.write_all(op.as_bytes()).await?;
            }
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
            } => {
                match headers.as_ref() {
                    Some(headers) if !headers.is_empty() => {
                        self.stream.write_all(b"HPUB ").await?;
                    }
                    _ => {
                        self.stream.write_all(b"PUB ").await?;
                    }
                }

                self.stream.write_all(subject.as_bytes()).await?;
                self.stream.write_all(b" ").await?;

                if let Some(respond) = respond {
                    self.stream.write_all(respond.as_bytes()).await?;
                    self.stream.write_all(b" ").await?;
                }

                match headers {
                    Some(headers) if !headers.is_empty() => {
                        let headers = headers.to_bytes();

                        let mut header_len_buf = itoa::Buffer::new();
                        self.stream
                            .write_all(header_len_buf.format(headers.len()).as_bytes())
                            .await?;

                        self.stream.write_all(b" ").await?;

                        let mut total_len_buf = itoa::Buffer::new();
                        self.stream
                            .write_all(
                                total_len_buf
                                    .format(headers.len() + payload.len())
                                    .as_bytes(),
                            )
                            .await?;

                        self.stream.write_all(b"\r\n").await?;
                        self.stream.write_all(&headers).await?;
                    }
                    _ => {
                        let mut len_buf = itoa::Buffer::new();
                        self.stream
                            .write_all(len_buf.format(payload.len()).as_bytes())
                            .await?;
                        self.stream.write_all(b"\r\n").await?;
                    }
                }

                self.stream.write_all(payload).await?;
                self.stream.write_all(b"\r\n").await?;
            }

            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                self.stream.write_all(b"SUB ").await?;
                self.stream.write_all(subject.as_bytes()).await?;
                if let Some(queue_group) = queue_group {
                    self.stream
                        .write_all(format!(" {queue_group}").as_bytes())
                        .await?;
                }
                self.stream
                    .write_all(format!(" {sid}\r\n").as_bytes())
                    .await?;
            }

            ClientOp::Unsubscribe { sid, max } => {
                self.stream.write_all(b"UNSUB ").await?;
                self.stream.write_all(format!("{sid}").as_bytes()).await?;
                if let Some(max) = max {
                    self.stream.write_all(format!(" {max}").as_bytes()).await?;
                }
                self.stream.write_all(b"\r\n").await?;
            }
            ClientOp::Ping => {
                self.stream.write_all(b"PING\r\n").await?;
                self.stream.flush().await?;
            }
            ClientOp::Pong => {
                self.stream.write_all(b"PONG\r\n").await?;
            }
        }

        Ok(())
    }

    /// Flush the write buffer, sending all pending data down the current write stream.
    pub(crate) async fn flush(&mut self) -> Result<(), io::Error> {
        self.stream.flush().await
    }
}

#[cfg(test)]
mod read_op {
    use super::Connection;
    use crate::{HeaderMap, ServerError, ServerInfo, ServerOp, StatusCode};
    use bytes::BytesMut;
    use tokio::io::{self, AsyncWriteExt};

    #[tokio::test]
    async fn ok() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        server.write_all(b"+OK\r\n").await.unwrap();
        let result = connection.read_op().await.unwrap();
        assert_eq!(result, Some(ServerOp::Ok));
    }

    #[tokio::test]
    async fn ping() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        server.write_all(b"PING\r\n").await.unwrap();
        let result = connection.read_op().await.unwrap();
        assert_eq!(result, Some(ServerOp::Ping));
    }

    #[tokio::test]
    async fn pong() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        server.write_all(b"PONG\r\n").await.unwrap();
        let result = connection.read_op().await.unwrap();
        assert_eq!(result, Some(ServerOp::Pong));
    }

    #[tokio::test]
    async fn info() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        server.write_all(b"INFO {}\r\n").await.unwrap();
        server.flush().await.unwrap();

        let result = connection.read_op().await.unwrap();
        assert_eq!(result, Some(ServerOp::Info(Box::default())));

        server
            .write_all(b"INFO { \"version\": \"1.0.0\" }\r\n")
            .await
            .unwrap();
        server.flush().await.unwrap();

        let result = connection.read_op().await.unwrap();
        assert_eq!(
            result,
            Some(ServerOp::Info(Box::new(ServerInfo {
                version: "1.0.0".into(),
                ..Default::default()
            })))
        );
    }

    #[tokio::test]
    async fn error() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        server.write_all(b"INFO {}\r\n").await.unwrap();
        let result = connection.read_op().await.unwrap();
        assert_eq!(result, Some(ServerOp::Info(Box::default())));

        server
            .write_all(b"-ERR something went wrong\r\n")
            .await
            .unwrap();
        let result = connection.read_op().await.unwrap();
        assert_eq!(
            result,
            Some(ServerOp::Error(ServerError::Other(
                "something went wrong".into()
            )))
        );
    }

    #[tokio::test]
    async fn message() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        server
            .write_all(b"MSG FOO.BAR 9 11\r\nHello World\r\n")
            .await
            .unwrap();

        let result = connection.read_op().await.unwrap();
        assert_eq!(
            result,
            Some(ServerOp::Message {
                sid: 9,
                subject: "FOO.BAR".into(),
                reply: None,
                headers: None,
                payload: "Hello World".into(),
                status: None,
                description: None,
                length: 7 + 11,
            })
        );

        server
            .write_all(b"MSG FOO.BAR 9 INBOX.34 11\r\nHello World\r\n")
            .await
            .unwrap();

        let result = connection.read_op().await.unwrap();
        assert_eq!(
            result,
            Some(ServerOp::Message {
                sid: 9,
                subject: "FOO.BAR".into(),
                reply: Some("INBOX.34".into()),
                headers: None,
                payload: "Hello World".into(),
                status: None,
                description: None,
                length: 7 + 8 + 11,
            })
        );

        server
            .write_all(b"HMSG FOO.BAR 10 INBOX.35 23 34\r\n")
            .await
            .unwrap();
        server.write_all(b"NATS/1.0\r\n").await.unwrap();
        server.write_all(b"Header: X\r\n").await.unwrap();
        server.write_all(b"\r\n").await.unwrap();
        server.write_all(b"Hello World\r\n").await.unwrap();

        let result = connection.read_op().await.unwrap();

        assert_eq!(
            result,
            Some(ServerOp::Message {
                sid: 10,
                subject: "FOO.BAR".into(),
                reply: Some("INBOX.35".into()),
                headers: Some(HeaderMap::from_iter([(
                    "Header".parse().unwrap(),
                    "X".parse().unwrap()
                )])),
                payload: "Hello World".into(),
                status: None,
                description: None,
                length: 7 + 8 + 34
            })
        );

        server
            .write_all(b"HMSG FOO.BAR 10 INBOX.35 23 34\r\n")
            .await
            .unwrap();
        server.write_all(b"NATS/1.0\r\n").await.unwrap();
        server.write_all(b"Header: Y\r\n").await.unwrap();
        server.write_all(b"\r\n").await.unwrap();
        server.write_all(b"Hello World\r\n").await.unwrap();

        let result = connection.read_op().await.unwrap();
        assert_eq!(
            result,
            Some(ServerOp::Message {
                sid: 10,
                subject: "FOO.BAR".into(),
                reply: Some("INBOX.35".into()),
                headers: Some(HeaderMap::from_iter([(
                    "Header".parse().unwrap(),
                    "Y".parse().unwrap()
                )])),
                payload: "Hello World".into(),
                status: None,
                description: None,
                length: 7 + 8 + 34,
            })
        );

        server
            .write_all(b"HMSG FOO.BAR 10 INBOX.35 28 28\r\n")
            .await
            .unwrap();
        server
            .write_all(b"NATS/1.0 404 No Messages\r\n")
            .await
            .unwrap();
        server.write_all(b"\r\n").await.unwrap();
        server.write_all(b"\r\n").await.unwrap();

        let result = connection.read_op().await.unwrap();
        assert_eq!(
            result,
            Some(ServerOp::Message {
                sid: 10,
                subject: "FOO.BAR".into(),
                reply: Some("INBOX.35".into()),
                headers: Some(HeaderMap::default()),
                payload: "".into(),
                status: Some(StatusCode::NOT_FOUND),
                description: Some("No Messages".to_string()),
                length: 7 + 8 + 28,
            })
        );

        server
            .write_all(b"MSG FOO.BAR 9 11\r\nHello Again\r\n")
            .await
            .unwrap();

        let result = connection.read_op().await.unwrap();
        assert_eq!(
            result,
            Some(ServerOp::Message {
                sid: 9,
                subject: "FOO.BAR".into(),
                reply: None,
                headers: None,
                payload: "Hello Again".into(),
                status: None,
                description: None,
                length: 7 + 11,
            })
        );
    }

    #[tokio::test]
    async fn unknown() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        server.write_all(b"ONE\r\n").await.unwrap();
        connection.read_op().await.unwrap_err();

        server.write_all(b"TWO\r\n").await.unwrap();
        connection.read_op().await.unwrap_err();

        server.write_all(b"PING\r\n").await.unwrap();
        connection.read_op().await.unwrap();

        server.write_all(b"THREE\r\n").await.unwrap();
        connection.read_op().await.unwrap_err();

        server
            .write_all(b"HMSG FOO.BAR 10 INBOX.35 28 28\r\n")
            .await
            .unwrap();
        server
            .write_all(b"NATS/1.0 404 No Messages\r\n")
            .await
            .unwrap();
        server.write_all(b"\r\n").await.unwrap();
        server.write_all(b"\r\n").await.unwrap();

        let result = connection.read_op().await.unwrap();
        assert_eq!(
            result,
            Some(ServerOp::Message {
                sid: 10,
                subject: "FOO.BAR".into(),
                reply: Some("INBOX.35".into()),
                headers: Some(HeaderMap::default()),
                payload: "".into(),
                status: Some(StatusCode::NOT_FOUND),
                description: Some("No Messages".to_string()),
                length: 7 + 8 + 28,
            })
        );

        server.write_all(b"FOUR\r\n").await.unwrap();
        connection.read_op().await.unwrap_err();

        server.write_all(b"PONG\r\n").await.unwrap();
        connection.read_op().await.unwrap();
    }
}

#[cfg(test)]
mod write_op {
    use super::Connection;
    use crate::{ClientOp, ConnectInfo, HeaderMap, Protocol};
    use bytes::BytesMut;
    use tokio::io::{self, AsyncBufReadExt, BufReader};

    #[tokio::test]
    async fn publish() {
        let (stream, server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        connection
            .write_op(&ClientOp::Publish {
                subject: "FOO.BAR".into(),
                payload: "Hello World".into(),
                respond: None,
                headers: None,
            })
            .await
            .unwrap();
        connection.flush().await.unwrap();

        let mut buffer = String::new();
        let mut reader = BufReader::new(server);
        reader.read_line(&mut buffer).await.unwrap();
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "PUB FOO.BAR 11\r\nHello World\r\n");

        connection
            .write_op(&ClientOp::Publish {
                subject: "FOO.BAR".into(),
                payload: "Hello World".into(),
                respond: Some("INBOX.67".into()),
                headers: None,
            })
            .await
            .unwrap();
        connection.flush().await.unwrap();

        buffer.clear();
        reader.read_line(&mut buffer).await.unwrap();
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "PUB FOO.BAR INBOX.67 11\r\nHello World\r\n");

        connection
            .write_op(&ClientOp::Publish {
                subject: "FOO.BAR".into(),
                payload: "Hello World".into(),
                respond: Some("INBOX.67".into()),
                headers: Some(HeaderMap::from_iter([(
                    "Header".parse().unwrap(),
                    "X".parse().unwrap(),
                )])),
            })
            .await
            .unwrap();
        connection.flush().await.unwrap();

        buffer.clear();
        reader.read_line(&mut buffer).await.unwrap();
        reader.read_line(&mut buffer).await.unwrap();
        reader.read_line(&mut buffer).await.unwrap();
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(
            buffer,
            "HPUB FOO.BAR INBOX.67 23 34\r\nNATS/1.0\r\nHeader: X\r\n\r\n"
        );
    }

    #[tokio::test]
    async fn subscribe() {
        let (stream, server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        connection
            .write_op(&ClientOp::Subscribe {
                sid: 11,
                subject: "FOO.BAR".into(),
                queue_group: None,
            })
            .await
            .unwrap();
        connection.flush().await.unwrap();

        let mut buffer = String::new();
        let mut reader = BufReader::new(server);
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "SUB FOO.BAR 11\r\n");

        connection
            .write_op(&ClientOp::Subscribe {
                sid: 11,
                subject: "FOO.BAR".into(),
                queue_group: Some("QUEUE.GROUP".into()),
            })
            .await
            .unwrap();
        connection.flush().await.unwrap();

        buffer.clear();
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "SUB FOO.BAR QUEUE.GROUP 11\r\n");
    }

    #[tokio::test]
    async fn unsubscribe() {
        let (stream, server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        connection
            .write_op(&ClientOp::Unsubscribe { sid: 11, max: None })
            .await
            .unwrap();
        connection.flush().await.unwrap();

        let mut buffer = String::new();
        let mut reader = BufReader::new(server);
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "UNSUB 11\r\n");

        connection
            .write_op(&ClientOp::Unsubscribe {
                sid: 11,
                max: Some(2),
            })
            .await
            .unwrap();
        connection.flush().await.unwrap();

        buffer.clear();
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "UNSUB 11 2\r\n");
    }

    #[tokio::test]
    async fn ping() {
        let (stream, server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        let mut reader = BufReader::new(server);
        let mut buffer = String::new();

        connection.write_op(&ClientOp::Ping).await.unwrap();
        connection.flush().await.unwrap();

        reader.read_line(&mut buffer).await.unwrap();

        assert_eq!(buffer, "PING\r\n");
    }

    #[tokio::test]
    async fn pong() {
        let (stream, server) = io::duplex(128);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        let mut reader = BufReader::new(server);
        let mut buffer = String::new();

        connection.write_op(&ClientOp::Pong).await.unwrap();
        connection.flush().await.unwrap();

        reader.read_line(&mut buffer).await.unwrap();

        assert_eq!(buffer, "PONG\r\n");
    }

    #[tokio::test]
    async fn connect() {
        let (stream, server) = io::duplex(1024);
        let mut connection = Connection {
            stream: Box::new(stream),
            buffer: BytesMut::new(),
        };

        let mut reader = BufReader::new(server);
        let mut buffer = String::new();

        connection
            .write_op(&ClientOp::Connect(ConnectInfo {
                verbose: false,
                pedantic: false,
                user_jwt: None,
                nkey: None,
                signature: None,
                name: None,
                echo: false,
                lang: "Rust".into(),
                version: "1.0.0".into(),
                protocol: Protocol::Dynamic,
                tls_required: false,
                user: None,
                pass: None,
                auth_token: None,
                headers: false,
                no_responders: false,
            }))
            .await
            .unwrap();
        connection.flush().await.unwrap();

        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(
            buffer,
            "CONNECT {\"verbose\":false,\"pedantic\":false,\"jwt\":null,\"nkey\":null,\"sig\":null,\"name\":null,\"echo\":false,\"lang\":\"Rust\",\"version\":\"1.0.0\",\"protocol\":1,\"tls_required\":false,\"user\":null,\"pass\":null,\"auth_token\":null,\"headers\":false,\"no_responders\":false}\r\n"
        );
    }
}
