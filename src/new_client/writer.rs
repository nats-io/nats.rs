use std::collections::VecDeque;
use std::io::{self, Error, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::oneshot;
use futures::prelude::*;

use crate::inject_io_failure;
use crate::new_client::encoder::{encode, ClientOp};

pub(crate) struct Writer {
    stream: Option<Pin<Box<dyn AsyncWrite + Send>>>,
    bytes: Box<[u8]>,
    written: usize,
    committed: usize,
    pongs: VecDeque<oneshot::Sender<()>>,
}

impl Writer {
    pub(crate) fn new(buf_size: usize) -> Writer {
        Writer {
            stream: None,
            bytes: vec![0u8; buf_size].into_boxed_slice(),
            written: 0,
            committed: 0,
            pongs: VecDeque::new(),
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

    /// Sends a PING to the server.
    pub(crate) async fn ping(&mut self) -> io::Result<oneshot::Receiver<()>> {
        encode(&mut *self, ClientOp::Ping).await?;
        self.commit();

        // Flush to make sure the PING goes through.
        self.flush().await?;

        let (sender, receiver) = oneshot::channel();
        self.pongs.push_back(sender);

        Ok(receiver)
    }

    /// Processes a PONG received from the server.
    pub(crate) fn pong(&mut self) {
        // Take the next expected pong from the queue.
        // TODO(stjepang): pongs should be marked with connection #
        let pong = self.pongs.pop_front().expect("unexpected pong");

        // Complete the pong by sending a message into the channel.
        let _ = pong.send(());
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

        // In case of I/O errors, drop the connection and clear the pong list.
        if res.is_err() {
            self.stream = None;
            self.pongs.clear();
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

        // In case of I/O errors, drop the connection and clear the pong list.
        if res.is_err() {
            self.stream = None;
            self.pongs.clear();
        }
        Poll::Ready(res)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
