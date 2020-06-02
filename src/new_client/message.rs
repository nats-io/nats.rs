use std::fmt;
use std::io::{self, Error, ErrorKind};
use std::sync::Arc;

use async_mutex::Mutex;
use smol::block_on;

use crate::inject_io_failure;
use crate::new_client::encoder::{encode, ClientOp};
use crate::new_client::writer::Writer;

/// A `Message` that has been published to a NATS `Subject`.
pub struct Message {
    /// The NATS `Subject` that this `Message` has been published to.
    pub subject: String,
    /// The optional reply `Subject` that may be used for sending
    /// responses when using the request/reply pattern.
    pub reply: Option<String>,
    /// The `Message` contents.
    pub data: Vec<u8>,

    pub(crate) writer: Option<Arc<Mutex<Writer>>>,
}

impl Message {
    /// Respond to a request message.
    pub fn respond(&mut self, msg: impl AsRef<[u8]>) -> io::Result<()> {
        let writer = match self.writer.as_ref() {
            None => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "No reply subject available",
                ))
            }
            Some(w) => w,
        };

        block_on(async {
            let mut writer = writer.lock().await;

            // Inject random I/O failures when testing.
            inject_io_failure()?;

            encode(
                &mut *writer,
                ClientOp::Pub {
                    subject: self.subject.as_ref(),
                    reply_to: self.reply.as_ref().map(|s| s.as_str()),
                    payload: msg.as_ref(),
                },
            )
            .await?;

            writer.commit();
            Ok(())
        })
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        // TODO(stjepang): fill out fields
        write!(f, "Message")
    }
}
