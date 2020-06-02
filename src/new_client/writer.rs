use std::io::{self, Error, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::prelude::*;

use crate::inject_io_failure;

pub(crate) struct Writer {
    stream: Option<Pin<Box<dyn AsyncWrite + Send>>>,
    bytes: Box<[u8]>,
    written: usize,
    committed: usize,
}

impl Writer {
    pub(crate) fn new(buf_size: usize) -> Writer {
        Writer {
            stream: None,
            bytes: vec![0u8; buf_size].into_boxed_slice(),
            written: 0,
            committed: 0,
        }
    }

    pub(crate) async fn reconnect(
        &mut self,
        stream: impl AsyncWrite + Send + 'static,
    ) -> io::Result<()> {
        // Drop the current stream, if there is any.
        self.stream = None;

        // Take out buffered operations.
        let range = ..self.committed;
        self.written = 0;
        self.committed = 0;

        // Inject random I/O failures when testing.
        inject_io_failure()?;

        // Write buffered operations into the new stream.
        let mut stream = Box::pin(stream);
        stream.write_all(&self.bytes[range]).await?;

        // Use the new stream from now on.
        self.stream = Some(stream);
        Ok(())
    }

    pub(crate) fn commit(&mut self) {
        self.committed = self.written;
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let res = match self.stream.as_mut() {
            Some(stream) => {
                // Inject random I/O failures when testing.
                match inject_io_failure() {
                    Ok(()) => futures::ready!(stream.as_mut().poll_write(cx, buf)),
                    Err(err) => Err(err),
                }
            }
            None => {
                let n = buf.len();
                if self.bytes.len() - self.written < n {
                    Err(Error::new(
                        ErrorKind::Other,
                        "the disconnection buffer is full",
                    ))
                } else {
                    let range = self.written..self.written + n;
                    self.bytes[range].copy_from_slice(&buf[..n]);
                    self.written += n;
                    Ok(n)
                }
            }
        };

        // In case of I/O errors, drop the connection.
        if res.is_err() {
            self.stream = None;
        }
        Poll::Ready(res)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let res = match self.stream.as_mut() {
            Some(stream) => {
                // Inject random I/O failures when testing.
                match inject_io_failure() {
                    Ok(()) => futures::ready!(stream.as_mut().poll_flush(cx)),
                    Err(err) => Err(err),
                }
            }
            None => Ok(()),
        };

        // In case of I/O errors, drop the connection.
        if res.is_err() {
            self.stream = None;
        }
        Poll::Ready(res)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
