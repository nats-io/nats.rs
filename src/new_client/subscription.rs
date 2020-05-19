use std::io::{self, ErrorKind};

use futures::channel::mpsc;
use futures::prelude::*;
use smol::block_on;

use crate::Message;
use crate::new_client::client::UserOp;

/// A subscription to a subject.
pub struct Subscription {
    /// Subscription ID.
    sid: usize,

    /// MSG operations received from the server.
    messages: mpsc::UnboundedReceiver<Message>,

    /// Enqueues user operations.
    user_ops: mpsc::UnboundedSender<UserOp>,
}

impl Subscription {
    /// Creates a subscription.
    pub(crate) fn new(
        subject: &str,
        sid: usize,
        user_ops: mpsc::UnboundedSender<UserOp>,
    ) -> Subscription {
        let (msg_sender, msg_receiver) = mpsc::unbounded();

        // Enqueue a SUB operation.
        let _ = user_ops.unbounded_send(UserOp::Sub {
            subject: subject.to_string(),
            queue_group: None,
            sid,
            messages: msg_sender,
        });

        Subscription {
            sid,
            messages: msg_receiver,
            user_ops,
        }
    }

    /// Blocks until the next message is received or the connection is closed.
    pub fn next(&mut self) -> io::Result<Message> {
        // Block on the next message in the channel.
        block_on(self.messages.next()).ok_or_else(|| ErrorKind::ConnectionReset.into())
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Enqueue an UNSUB operation.
        let _ = self.user_ops.unbounded_send(UserOp::Unsub {
            sid: self.sid,
            max_msgs: None,
        });
    }
}
