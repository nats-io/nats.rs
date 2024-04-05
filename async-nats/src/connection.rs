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

use std::collections::VecDeque;
use std::fmt::{self, Display, Write as _};
use std::future::{self, Future};
use std::io::IoSlice;
use std::pin::Pin;
use std::str::{self, FromStr};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite};

use crate::header::{HeaderMap, HeaderName, IntoHeaderValue};
use crate::status::StatusCode;
use crate::subject::Subject;
use crate::{ClientOp, ServerError, ServerOp};

/// Soft limit for the amount of bytes in [`Connection::write_buf`]
/// and [`Connection::flattened_writes`].
const SOFT_WRITE_BUF_LIMIT: usize = 65535;
/// How big a single buffer must be before it's written separately
/// instead of being flattened.
const WRITE_FLATTEN_THRESHOLD: usize = 4096;
/// How many buffers to write in a single vectored write call.
const WRITE_VECTORED_CHUNKS: usize = 64;

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

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ShouldFlush {
    /// Write buffers are empty, but the connection hasn't been flushed yet
    Yes,
    /// The connection hasn't been flushed yet, but write buffers aren't empty
    May,
    /// Flushing would just be a no-op
    No,
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
    write_buf: VecDeque<Bytes>,
    write_buf_len: usize,
    flattened_writes: BytesMut,
    can_flush: bool,
}

/// Internal representation of the connection.
/// Holds connection with NATS Server and communicates with `Client` via channels.
impl Connection {
    pub(crate) fn new(stream: Box<dyn AsyncReadWrite>, read_buffer_capacity: usize) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(read_buffer_capacity),
            write_buf: VecDeque::new(),
            write_buf_len: 0,
            flattened_writes: BytesMut::new(),
            can_flush: false,
        }
    }

    /// Returns `true` if no more calls to [`Self::enqueue_write_op`] _should_ be made.
    pub(crate) fn is_write_buf_full(&self) -> bool {
        self.write_buf_len >= SOFT_WRITE_BUF_LIMIT
    }

    /// Returns `true` if [`Self::poll_flush`] should be polled.
    pub(crate) fn should_flush(&self) -> ShouldFlush {
        match (
            self.can_flush,
            self.write_buf.is_empty() && self.flattened_writes.is_empty(),
        ) {
            (true, true) => ShouldFlush::Yes,
            (true, false) => ShouldFlush::May,
            (false, _) => ShouldFlush::No,
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

            let length = payload_len
                + reply_to.as_ref().map(|reply| reply.len()).unwrap_or(0)
                + subject.len();

            let subject = Subject::from(subject);
            let reply = reply_to.map(Subject::from);

            self.read_buf.advance(len + 2);
            let payload = self.read_buf.split_to(payload_len).freeze();
            self.read_buf.advance(2);

            return Ok(Some(ServerOp::Message {
                sid,
                length,
                reply,
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

            // Convert the slice into a subject
            let subject = Subject::from(subject);

            // Parse the subject ID.
            let sid = sid.parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot parse sid argument after HMSG",
                )
            })?;

            // Convert the slice into a subject.
            let reply = reply_to.map(Subject::from);

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
                length: reply.as_ref().map_or(0, |reply| reply.len()) + subject.len() + total_len,
                sid,
                reply,
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

    pub(crate) async fn easy_write_and_flush<'a>(
        &mut self,
        items: impl Iterator<Item = &'a ClientOp>,
    ) -> io::Result<()> {
        for item in items {
            self.enqueue_write_op(item);
        }

        future::poll_fn(|cx| self.poll_write(cx)).await?;
        future::poll_fn(|cx| self.poll_flush(cx)).await?;
        Ok(())
    }

    /// Writes a client operation to the write buffer.
    pub(crate) fn enqueue_write_op(&mut self, item: &ClientOp) {
        macro_rules! small_write {
            ($dst:expr) => {
                write!(self.small_write(), $dst).expect("do small write to Connection");
            };
        }

        match item {
            ClientOp::Connect(connect_info) => {
                let json = serde_json::to_vec(&connect_info).expect("serialize `ConnectInfo`");

                self.write("CONNECT ");
                self.write(json);
                self.write("\r\n");
            }
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
            } => {
                let verb = match headers.as_ref() {
                    Some(headers) if !headers.is_empty() => "HPUB",
                    _ => "PUB",
                };

                small_write!("{verb} {subject} ");

                if let Some(respond) = respond {
                    small_write!("{respond} ");
                }

                match headers {
                    Some(headers) if !headers.is_empty() => {
                        let headers = headers.to_bytes();

                        let headers_len = headers.len();
                        let total_len = headers_len + payload.len();
                        small_write!("{headers_len} {total_len}\r\n");
                        self.write(headers);
                    }
                    _ => {
                        let payload_len = payload.len();
                        small_write!("{payload_len}\r\n");
                    }
                }

                self.write(Bytes::clone(payload));
                self.write("\r\n");
            }

            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => match queue_group {
                Some(queue_group) => {
                    small_write!("SUB {subject} {queue_group} {sid}\r\n");
                }
                None => {
                    small_write!("SUB {subject} {sid}\r\n");
                }
            },

            ClientOp::Unsubscribe { sid, max } => match max {
                Some(max) => {
                    small_write!("UNSUB {sid} {max}\r\n");
                }
                None => {
                    small_write!("UNSUB {sid}\r\n");
                }
            },
            ClientOp::Ping => {
                self.write("PING\r\n");
            }
            ClientOp::Pong => {
                self.write("PONG\r\n");
            }
        }
    }

    /// Write the internal buffers into the write stream
    ///
    /// Returns one of the following:
    ///
    /// * `Poll::Pending` means that we weren't able to fully empty
    ///   the internal buffers. Compared to [`AsyncWrite::poll_write`],
    ///   this implementation may do a partial write before yielding.
    /// * `Poll::Ready(Ok())` means that the internal write buffers have
    ///   been emptied or were already empty.
    /// * `Poll::Ready(Err(err))` means that writing to the stream failed.
    ///   Compared to [`AsyncWrite::poll_write`], this implementation
    ///   may do a partial write before failing.
    pub(crate) fn poll_write(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.stream.is_write_vectored() {
            self.poll_write_sequential(cx)
        } else {
            self.poll_write_vectored(cx)
        }
    }

    /// Write the internal buffers into the write stream using sequential write operations
    ///
    /// Writes one chunk at a time. Less efficient.
    fn poll_write_sequential(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let buf = match self.write_buf.front() {
                Some(buf) => &**buf,
                None if !self.flattened_writes.is_empty() => &self.flattened_writes,
                None => return Poll::Ready(Ok(())),
            };

            debug_assert!(!buf.is_empty());

            match Pin::new(&mut self.stream).poll_write(cx, buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(n)) => {
                    self.write_buf_len -= n;
                    self.can_flush = true;

                    match self.write_buf.front_mut() {
                        Some(buf) if n < buf.len() => {
                            buf.advance(n);
                        }
                        Some(_buf) => {
                            self.write_buf.pop_front();
                        }
                        None => {
                            self.flattened_writes.advance(n);
                        }
                    }
                    continue;
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            }
        }
    }

    /// Write the internal buffers into the write stream using vectored write operations
    ///
    /// Writes [`WRITE_VECTORED_CHUNKS`] at a time. More efficient _if_
    /// the underlying writer supports it.
    fn poll_write_vectored(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        'outer: loop {
            let mut writes = [IoSlice::new(b""); WRITE_VECTORED_CHUNKS];
            let mut writes_len = 0;

            self.write_buf
                .iter()
                .take(WRITE_VECTORED_CHUNKS)
                .enumerate()
                .for_each(|(i, buf)| {
                    writes[i] = IoSlice::new(buf);
                    writes_len += 1;
                });

            if writes_len < WRITE_VECTORED_CHUNKS && !self.flattened_writes.is_empty() {
                writes[writes_len] = IoSlice::new(&self.flattened_writes);
                writes_len += 1;
            }

            if writes_len == 0 {
                return Poll::Ready(Ok(()));
            }

            match Pin::new(&mut self.stream).poll_write_vectored(cx, &writes[..writes_len]) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(mut n)) => {
                    self.write_buf_len -= n;
                    self.can_flush = true;

                    while let Some(buf) = self.write_buf.front_mut() {
                        if n < buf.len() {
                            buf.advance(n);
                            continue 'outer;
                        }

                        n -= buf.len();
                        self.write_buf.pop_front();
                    }

                    self.flattened_writes.advance(n);
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            }
        }
    }

    /// Write `buf` into the writes buffer
    ///
    /// If `buf` is smaller than [`WRITE_FLATTEN_THRESHOLD`]
    /// flattens it, otherwise appends it to the chunks queue.
    ///
    /// Empty `buf`s are a no-op.
    fn write(&mut self, buf: impl Into<Bytes>) {
        let buf = buf.into();
        if buf.is_empty() {
            return;
        }

        self.write_buf_len += buf.len();
        if buf.len() < WRITE_FLATTEN_THRESHOLD {
            self.flattened_writes.extend_from_slice(&buf);
        } else {
            if !self.flattened_writes.is_empty() {
                let buf = self.flattened_writes.split().freeze();
                self.write_buf.push_back(buf);
            }

            self.write_buf.push_back(buf);
        }
    }

    /// Obtain an [`fmt::Write`]r for the small writes buffer.
    fn small_write(&mut self) -> impl fmt::Write + '_ {
        struct Writer<'a> {
            this: &'a mut Connection,
        }

        impl<'a> fmt::Write for Writer<'a> {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                self.this.write_buf_len += s.len();
                self.this.flattened_writes.write_str(s)
            }
        }

        Writer { this: self }
    }

    /// Flush the write buffer, sending all pending data down the current write stream.
    ///
    /// no-op if the write stream didn't need to be flushed.
    pub(crate) fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.stream).poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.can_flush = false;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
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
            .easy_write_and_flush(
                [ClientOp::Publish {
                    subject: "FOO.BAR".into(),
                    payload: "Hello World".into(),
                    respond: None,
                    headers: None,
                }]
                .iter(),
            )
            .await
            .unwrap();

        let mut buffer = String::new();
        let mut reader = BufReader::new(server);
        reader.read_line(&mut buffer).await.unwrap();
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "PUB FOO.BAR 11\r\nHello World\r\n");

        connection
            .easy_write_and_flush(
                [ClientOp::Publish {
                    subject: "FOO.BAR".into(),
                    payload: "Hello World".into(),
                    respond: Some("INBOX.67".into()),
                    headers: None,
                }]
                .iter(),
            )
            .await
            .unwrap();

        buffer.clear();
        reader.read_line(&mut buffer).await.unwrap();
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "PUB FOO.BAR INBOX.67 11\r\nHello World\r\n");

        connection
            .easy_write_and_flush(
                [ClientOp::Publish {
                    subject: "FOO.BAR".into(),
                    payload: "Hello World".into(),
                    respond: Some("INBOX.67".into()),
                    headers: Some(HeaderMap::from_iter([(
                        "Header".parse().unwrap(),
                        "X".parse().unwrap(),
                    )])),
                }]
                .iter(),
            )
            .await
            .unwrap();

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
            .easy_write_and_flush(
                [ClientOp::Subscribe {
                    sid: 11,
                    subject: "FOO.BAR".into(),
                    queue_group: None,
                }]
                .iter(),
            )
            .await
            .unwrap();

        let mut buffer = String::new();
        let mut reader = BufReader::new(server);
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "SUB FOO.BAR 11\r\n");

        connection
            .easy_write_and_flush(
                [ClientOp::Subscribe {
                    sid: 11,
                    subject: "FOO.BAR".into(),
                    queue_group: Some("QUEUE.GROUP".into()),
                }]
                .iter(),
            )
            .await
            .unwrap();

        buffer.clear();
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "SUB FOO.BAR QUEUE.GROUP 11\r\n");
    }

    #[tokio::test]
    async fn unsubscribe() {
        let (stream, server) = io::duplex(128);
        let mut connection = Connection::new(Box::new(stream), 0);

        connection
            .easy_write_and_flush([ClientOp::Unsubscribe { sid: 11, max: None }].iter())
            .await
            .unwrap();

        let mut buffer = String::new();
        let mut reader = BufReader::new(server);
        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(buffer, "UNSUB 11\r\n");

        connection
            .easy_write_and_flush(
                [ClientOp::Unsubscribe {
                    sid: 11,
                    max: Some(2),
                }]
                .iter(),
            )
            .await
            .unwrap();

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

        connection
            .easy_write_and_flush([ClientOp::Ping].iter())
            .await
            .unwrap();

        reader.read_line(&mut buffer).await.unwrap();

        assert_eq!(buffer, "PING\r\n");
    }

    #[tokio::test]
    async fn pong() {
        let (stream, server) = io::duplex(128);
        let mut connection = Connection::new(Box::new(stream), 0);

        let mut reader = BufReader::new(server);
        let mut buffer = String::new();

        connection
            .easy_write_and_flush([ClientOp::Pong].iter())
            .await
            .unwrap();

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
            .easy_write_and_flush(
                [ClientOp::Connect(ConnectInfo {
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
                })]
                .iter(),
            )
            .await
            .unwrap();

        reader.read_line(&mut buffer).await.unwrap();
        assert_eq!(
            buffer,
            "CONNECT {\"verbose\":false,\"pedantic\":false,\"jwt\":null,\"nkey\":null,\"sig\":null,\"name\":null,\"echo\":false,\"lang\":\"Rust\",\"version\":\"1.0.0\",\"protocol\":1,\"tls_required\":false,\"user\":null,\"pass\":null,\"auth_token\":null,\"headers\":false,\"no_responders\":false}\r\n"
        );
    }
}
