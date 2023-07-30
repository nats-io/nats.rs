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
use std::future::{self, Future};
use std::str::{self, FromStr};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::io::{AsyncReadExt, AsyncWrite};

use bytes::{Buf, BytesMut};
use tokio::io;

use crate::header::{HeaderMap, HeaderName, IntoHeaderValue};
use crate::status::StatusCode;
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
    read_buf: BytesMut,
}

/// Internal representation of the connection.
/// Holds connection with NATS Server and communicates with `Client` via channels.
impl Connection {
    pub(crate) fn new(stream: Box<dyn AsyncReadWrite>, read_buffer_capacity: usize) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(read_buffer_capacity),
        }
    }

    /// Attempts to read a server operation from the read buffer.
    /// Returns `None` if there is not enough data to parse an entire operation.
    pub(crate) fn try_read_op(&mut self) -> Result<Option<ServerOp>, io::Error> {
        let len = match memchr::memmem::find(&self.read_buf, b"\r\n") {
            Some(len) => len,
            None => return Ok(None),
        };

        if self.read_buf.starts_with(b"+OK") {
            self.read_buf.advance(len + 2);
            return Ok(Some(ServerOp::Ok));
        }

        if self.read_buf.starts_with(b"PING") {
            self.read_buf.advance(len + 2);
            return Ok(Some(ServerOp::Ping));
        }

        if self.read_buf.starts_with(b"PONG") {
            self.read_buf.advance(len + 2);
            return Ok(Some(ServerOp::Pong));
        }

        if self.read_buf.starts_with(b"-ERR") {
            let description = str::from_utf8(&self.read_buf[5..len])
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?
                .trim_matches('\'')
                .to_owned();

            self.read_buf.advance(len + 2);

            return Ok(Some(ServerOp::Error(ServerError::new(description))));
        }

        if self.read_buf.starts_with(b"INFO ") {
            let info = serde_json::from_slice(&self.read_buf[4..len])
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

            self.read_buf.advance(len + 2);

            return Ok(Some(ServerOp::Info(Box::new(info))));
        }

        if self.read_buf.starts_with(b"MSG ") {
            let line = str::from_utf8(&self.read_buf[4..len]).unwrap();
            let mut args = line.split(' ').filter(|s| !s.is_empty());

            // Parse the operation syntax: MSG <subject> <sid> [reply-to] <#bytes>
            let (subject, sid, reply_to, payload_len) = match (
                args.next(),
                args.next(),
                args.next(),
                args.next(),
                args.next(),
            ) {
                (Some(subject), Some(sid), Some(reply_to), Some(payload_len), None) => {
                    (subject, sid, Some(reply_to), payload_len)
                }
                (Some(subject), Some(sid), Some(payload_len), None, None) => {
                    (subject, sid, None, payload_len)
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid number of arguments after MSG",
                    ))
                }
            };

            let sid = sid
                .parse::<u64>()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

            // Parse the number of payload bytes.
            let payload_len = payload_len
                .parse::<usize>()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

            // Return early without advancing if there is not enough data read the entire
            // message
            if len + payload_len + 4 > self.read_buf.remaining() {
                return Ok(None);
            }

            let subject = subject.to_owned();
            let reply_to = reply_to.map(ToOwned::to_owned);

            self.read_buf.advance(len + 2);
            let payload = self.read_buf.split_to(payload_len).freeze();
            self.read_buf.advance(2);

            let length = payload_len
                + reply_to.as_ref().map(|reply| reply.len()).unwrap_or(0)
                + subject.len();
            return Ok(Some(ServerOp::Message {
                sid,
                length,
                reply: reply_to,
                headers: None,
                subject,
                payload,
                status: None,
                description: None,
            }));
        }

        if self.read_buf.starts_with(b"HMSG ") {
            // Extract whitespace-delimited arguments that come after "HMSG".
            let line = std::str::from_utf8(&self.read_buf[5..len]).unwrap();
            let mut args = line.split_whitespace().filter(|s| !s.is_empty());

            // <subject> <sid> [reply-to] <# header bytes><# total bytes>
            let (subject, sid, reply_to, header_len, total_len) = match (
                args.next(),
                args.next(),
                args.next(),
                args.next(),
                args.next(),
                args.next(),
            ) {
                (
                    Some(subject),
                    Some(sid),
                    Some(reply_to),
                    Some(header_len),
                    Some(total_len),
                    None,
                ) => (subject, sid, Some(reply_to), header_len, total_len),
                (Some(subject), Some(sid), Some(header_len), Some(total_len), None, None) => {
                    (subject, sid, None, header_len, total_len)
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid number of arguments after HMSG",
                    ))
                }
            };

            // Convert the slice into an owned string.
            let subject = subject.to_owned();

            // Parse the subject ID.
            let sid = sid.parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot parse sid argument after HMSG",
                )
            })?;

            // Convert the slice into an owned string.
            let reply_to = reply_to.map(ToOwned::to_owned);

            // Parse the number of payload bytes.
            let header_len = header_len.parse::<usize>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot parse the number of header bytes argument after \
                     HMSG",
                )
            })?;

            // Parse the number of payload bytes.
            let total_len = total_len.parse::<usize>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot parse the number of bytes argument after HMSG",
                )
            })?;

            if total_len < header_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "number of header bytes was greater than or equal to the \
                 total number of bytes after HMSG",
                ));
            }

            if len + total_len + 4 > self.read_buf.remaining() {
                return Ok(None);
            }

            self.read_buf.advance(len + 2);
            let header = self.read_buf.split_to(header_len);
            let payload = self.read_buf.split_to(total_len - header_len).freeze();
            self.read_buf.advance(2);

            let mut lines = std::str::from_utf8(&header)
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "header isn't valid utf-8")
                })?
                .lines()
                .peekable();
            let version_line = lines.next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "no header version line found")
            })?;

            let version_line_suffix = version_line
                .strip_prefix("NATS/1.0")
                .map(str::trim)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "header version line does not begin with `NATS/1.0`",
                    )
                })?;

            let (status, description) = version_line_suffix
                .split_once(' ')
                .map(|(status, description)| (status.trim(), description.trim()))
                .unwrap_or((version_line_suffix, ""));
            let status = if !status.is_empty() {
                Some(status.parse::<StatusCode>().map_err(|_| {
                    std::io::Error::new(io::ErrorKind::Other, "could not parse status parameter")
                })?)
            } else {
                None
            };
            let description = if !description.is_empty() {
                Some(description.to_owned())
            } else {
                None
            };

            let mut headers = HeaderMap::new();
            while let Some(line) = lines.next() {
                if line.is_empty() {
                    continue;
                }

                let (name, value) = line.split_once(':').ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "no header version line found")
                })?;

                let name = HeaderName::from_str(name)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

                // Read the header value, which might have been split into multiple lines
                // `trim_start` and `trim_end` do the same job as doing `value.trim().to_owned()` at the end, but without a reallocation
                let mut value = value.trim_start().to_owned();
                while let Some(v) = lines.next_if(|s| s.starts_with(char::is_whitespace)) {
                    value.push_str(v);
                }
                value.truncate(value.trim_end().len());

                headers.append(name, value.into_header_value());
            }

            return Ok(Some(ServerOp::Message {
                length: reply_to.as_ref().map_or(0, |reply| reply.len())
                    + subject.len()
                    + total_len,
                sid,
                reply: reply_to,
                subject,
                headers: Some(headers),
                payload,
                status,
                description,
            }));
        }

        let buffer = self.read_buf.split_to(len + 2);
        let line = str::from_utf8(&buffer).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "unable to parse unknown input")
        })?;

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid server operation: '{line}'"),
        ))
    }

    pub(crate) fn read_op(&mut self) -> impl Future<Output = io::Result<Option<ServerOp>>> + '_ {
        future::poll_fn(|cx| self.poll_read_op(cx))
    }

    // TODO: do we want an custom error here?
    /// Read a server operation from read buffer.
    /// Blocks until an operation ca be parsed.
    pub(crate) fn poll_read_op(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Option<ServerOp>>> {
        loop {
            if let Some(op) = self.try_read_op()? {
                return Poll::Ready(Ok(Some(op)));
            }

            let read_buf = self.stream.read_buf(&mut self.read_buf);
            tokio::pin!(read_buf);
            return match read_buf.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(0)) if self.read_buf.is_empty() => Poll::Ready(Ok(None)),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(io::ErrorKind::ConnectionReset.into())),
                Poll::Ready(Ok(_n)) => continue,
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            };
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
    use tokio::io::{self, AsyncWriteExt};

    #[tokio::test]
    async fn ok() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection::new(Box::new(stream), 0);

        server.write_all(b"+OK\r\n").await.unwrap();
        let result = connection.read_op().await.unwrap();
        assert_eq!(result, Some(ServerOp::Ok));
    }

    #[tokio::test]
    async fn ping() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection::new(Box::new(stream), 0);

        server.write_all(b"PING\r\n").await.unwrap();
        let result = connection.read_op().await.unwrap();
        assert_eq!(result, Some(ServerOp::Ping));
    }

    #[tokio::test]
    async fn pong() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection::new(Box::new(stream), 0);

        server.write_all(b"PONG\r\n").await.unwrap();
        let result = connection.read_op().await.unwrap();
        assert_eq!(result, Some(ServerOp::Pong));
    }

    #[tokio::test]
    async fn info() {
        let (stream, mut server) = io::duplex(128);
        let mut connection = Connection::new(Box::new(stream), 0);

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
        let mut connection = Connection::new(Box::new(stream), 0);

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
        let mut connection = Connection::new(Box::new(stream), 0);

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
        let mut connection = Connection::new(Box::new(stream), 0);

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
    use tokio::io::{self, AsyncBufReadExt, BufReader};

    #[tokio::test]
    async fn publish() {
        let (stream, server) = io::duplex(128);
        let mut connection = Connection::new(Box::new(stream), 0);

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
        let mut connection = Connection::new(Box::new(stream), 0);

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
        let mut connection = Connection::new(Box::new(stream), 0);

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
        let mut connection = Connection::new(Box::new(stream), 0);

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
        let mut connection = Connection::new(Box::new(stream), 0);

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
        let mut connection = Connection::new(Box::new(stream), 0);

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
