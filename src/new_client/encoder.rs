use std::io;

use futures::prelude::*;

use crate::{connect::ConnectInfo, inject_io_failure};

/// A protocol operation sent by the client.
#[derive(Debug)]
pub(crate) enum ClientOp {
    /// CONNECT {["option_name":option_value],...}
    Connect(ConnectInfo),

    /// PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n
    Pub {
        subject: String,
        reply_to: Option<String>,
        payload: Vec<u8>,
    },

    /// SUB <subject> [queue group] <sid>\r\n
    Sub {
        subject: String,
        queue_group: Option<String>,
        sid: usize,
    },

    /// UNSUB <sid> [max_msgs]
    Unsub { sid: usize, max_msgs: Option<u64> },

    /// PING
    Ping,

    /// PONG
    Pong,
}

/// Encodes a single operation from the client.
pub(crate) async fn encode(mut stream: impl AsyncWrite + Unpin, op: ClientOp) -> io::Result<()> {
    // Inject random I/O failures when testing.
    inject_io_failure()?;

    match &op {
        ClientOp::Connect(connect_info) => {
            let op = format!(
                "CONNECT {}\r\nPING\r\n",
                serde_json::to_string(&connect_info)?
            );
            stream.write_all(op.as_bytes()).await?;
        }

        ClientOp::Pub {
            subject,
            reply_to,
            payload,
        } => {
            let op = if let Some(reply_to) = reply_to {
                format!("PUB {} {} {}\r\n", subject, reply_to, payload.len())
            } else {
                format!("PUB {} {}\r\n", subject, payload.len())
            };
            stream.write_all(op.as_bytes()).await?;
            stream.write_all(payload).await?;
            stream.write_all(b"\r\n").await?;
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
            stream.write_all(op.as_bytes()).await?;
        }

        ClientOp::Unsub { sid, max_msgs } => {
            let op = if let Some(max_msgs) = max_msgs {
                format!("UNSUB {} {}\r\n", sid, max_msgs)
            } else {
                format!("UNSUB {}\r\n", sid)
            };
            stream.write_all(op.as_bytes()).await?;
        }

        ClientOp::Ping => {
            stream.write_all(b"PING\r\n").await?;
        }

        ClientOp::Pong => {
            stream.write_all(b"PONG\r\n").await?;
        }
    }

    stream.flush().await?;
    Ok(())
}
