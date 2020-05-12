use std::io::{self, ErrorKind};

use futures::channel::mpsc;
use futures::prelude::*;
use smol::block_on;

use crate::Message;

/// A subscription to a subject.
pub struct Subscription {
    /// Subscription ID.
    sid: usize,

    /// MSG operations received from the server.
    messages: mpsc::UnboundedReceiver<Message>,

    /// Enqueues SUB and UNSUB operations.
    sub_ops: mpsc::UnboundedSender<SubscriptionOp>,
}

impl Subscription {
    /// Creates a subscription.
    pub(crate) fn new(
        subject: &str,
        sid: usize,
        sub_ops: mpsc::UnboundedSender<SubscriptionOp>,
    ) -> Subscription {
        let (msg_sender, msg_receiver) = mpsc::unbounded();

        // Enqueue a SUB operation.
        let _ = sub_ops.unbounded_send(SubscriptionOp::Sub {
            subject: subject.to_string(),
            queue_group: None,
            sid,
            messages: msg_sender,
        });

        Subscription {
            sid,
            messages: msg_receiver,
            sub_ops,
        }
    }

    /// Blocks until the next message is received or the connection is closed.
    pub fn next_msg(&mut self) -> io::Result<Message> {
        // Block on the next message in the channel.
        block_on(self.messages.next()).ok_or_else(|| ErrorKind::ConnectionReset.into())
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Enqueue an UNSUB operation.
        let _ = self.sub_ops.unbounded_send(SubscriptionOp::Unsub {
            sid: self.sid,
            max_msgs: None,
        });
    }
}

/// A SUB or UNSUB operation.
pub(crate) enum SubscriptionOp {
    Sub {
        subject: String,
        queue_group: Option<String>,
        sid: usize,
        messages: mpsc::UnboundedSender<Message>,
    },
    Unsub {
        sid: usize,
        max_msgs: Option<u64>,
    },
}
