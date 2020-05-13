use std::io::{self, Error, ErrorKind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use smol::block_on;

use crate::new_client::client::{self, UserOp};
use crate::new_client::subscription::Subscription;

/// A NATS client connection.
pub struct Connection {
    /// Enqueues user operations.
    user_ops: mpsc::UnboundedSender<UserOp>,

    /// Subscription ID generator.
    sid_gen: AtomicUsize,

    /// Thread running the main future.
    thread: Option<thread::JoinHandle<io::Result<()>>>,
}

impl Connection {
    /// Connects a NATS client.
    pub fn connect(url: &str) -> io::Result<Connection> {
        let (sender, receiver) = mpsc::unbounded();
        let thread = client::spawn(url, receiver);

        Ok(Connection {
            user_ops: sender,
            sid_gen: AtomicUsize::new(1),
            thread: Some(thread),
        })
    }

    /// Publishes a message.
    pub fn publish(&mut self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        let subject = subject.to_string();
        let payload = msg.as_ref().to_vec();
        let reply_to = None;

        // Enqueue a PUB operation.
        self.user_ops
            .unbounded_send(UserOp::Pub {
                subject,
                reply_to,
                payload,
            })
            .map_err(|err| Error::new(ErrorKind::ConnectionReset, err))?;

        Ok(())
    }

    /// Creates a new subscriber.
    pub fn subscribe(&mut self, subject: &str) -> Subscription {
        let sid = self.sid_gen.fetch_add(1, Ordering::SeqCst);
        Subscription::new(subject, sid, self.user_ops.clone())
    }

    /// Flushes by performing a round trip to the server.
    pub fn flush(&mut self) -> io::Result<()> {
        let (sender, receiver) = oneshot::channel();

        // Enqueue a PING operation.
        self.user_ops
            .unbounded_send(UserOp::Ping { pong: sender })
            .map_err(|err| Error::new(ErrorKind::ConnectionReset, err))?;

        // Wait until the PONG operation is received.
        let _ = block_on(receiver);

        Ok(())
    }

    /// Close the connection.
    pub fn close(&mut self) -> io::Result<()> {
        // Enqueue a close operation.
        let _ = self.user_ops.unbounded_send(UserOp::Close);

        // Wait for the client thread to stop.
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .expect("client thread has panicked")
                .map_err(|err| Error::new(ErrorKind::ConnectionReset, err))?;
        }

        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Enqueue a close operation in case it hasn't been already.
        let _ = self.close();
    }
}
