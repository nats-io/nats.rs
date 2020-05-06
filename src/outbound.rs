use std::{
    collections::HashMap,
    io::{self, BufWriter, Error, ErrorKind, Write},
    net::{Shutdown, TcpStream},
};

use parking_lot::{Condvar, Mutex};

use crate::{inject_delay, inject_io_failure, SubscriptionState, TlsWriter};

#[derive(Debug)]
pub(crate) struct DisconnectWriter {
    buf: Box<[u8]>,
    len: usize,
}

impl DisconnectWriter {
    fn new(buf_sz: usize) -> DisconnectWriter {
        DisconnectWriter {
            buf: vec![0; buf_sz].into_boxed_slice(),
            len: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) enum Writer {
    Tcp(BufWriter<TcpStream>),
    Tls(BufWriter<TlsWriter>),
    Disconnected(DisconnectWriter),
    Closed,
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        inject_io_failure()?;
        match self {
            Writer::Tcp(bw) => bw.write(buf),
            Writer::Tls(bw) => bw.write(buf),
            Writer::Disconnected(db) => {
                if db.len + buf.len() > db.buf.len() {
                    Err(Error::new(
                        ErrorKind::Other,
                        "the disconnection buffer is full",
                    ))
                } else {
                    db.buf[db.len..db.len + buf.len()].copy_from_slice(buf);
                    db.len += buf.len();
                    Ok(buf.len())
                }
            }
            Writer::Closed => Err(Error::new(
                ErrorKind::Other,
                "the connection is permanently closed",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        inject_io_failure()?;
        match self {
            Writer::Tcp(bw) => bw.flush(),
            Writer::Tls(bw) => bw.flush(),
            Writer::Disconnected(_) => Ok(()),
            Writer::Closed => Err(Error::new(
                ErrorKind::Other,
                "the connection is permanently closed",
            )),
        }
    }
}

impl Writer {
    fn flusher_should_wait(&self) -> bool {
        match self {
            Writer::Tcp(bw) => bw.buffer().is_empty(),
            Writer::Tls(bw) => bw.buffer().is_empty(),
            Writer::Disconnected(_) => true,
            Writer::Closed => false,
        }
    }

    fn shutdown(&mut self) -> io::Result<()> {
        inject_io_failure()?;
        match self {
            Writer::Tcp(bw) => {
                inject_io_failure()?;
                bw.flush()?;
                inject_io_failure()?;
                bw.get_mut().shutdown(Shutdown::Both)?;
            }
            Writer::Tls(bw) => {
                bw.flush()?;
                bw.get_mut().shutdown()?;
            }
            Writer::Disconnected(_) | Writer::Closed => (),
        }
        Ok(())
    }

    pub(crate) fn is_disconnected(&self) -> bool {
        if let Writer::Disconnected(_) = self {
            true
        } else {
            false
        }
    }

    fn is_closed(&self) -> bool {
        if let Writer::Closed = self {
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub(crate) struct Outbound {
    writer: Mutex<Writer>,
    updated: Condvar,
}

impl Outbound {
    pub(crate) fn new(writer: Writer) -> Outbound {
        Outbound {
            writer: Mutex::new(writer),
            updated: Condvar::new(),
        }
    }

    pub(crate) fn flush_loop(&self) {
        inject_delay();
        let mut writer = self.writer.lock();
        loop {
            while writer.flusher_should_wait() {
                self.updated.wait(&mut writer);
            }

            if writer.is_closed() {
                log::info!("flusher thread shutting down");
                return;
            }

            if let Err(error) = writer.flush() {
                log::error!("Outbound thread failed to flush: {:?}", error);

                // wait on the Condvar here until the inbound thread
                // replaces our buffer
                self.updated.wait(&mut writer);
            }
        }
    }

    pub(crate) fn transition_to_disconnected(&self, buf_sz: usize) {
        inject_delay();
        let mut writer = self.writer.lock();
        match &mut *writer {
            &mut Writer::Disconnected(_) | &mut Writer::Closed => {
                // nothing to do
            }
            other => *other = Writer::Disconnected(DisconnectWriter::new(buf_sz)),
        }
    }

    pub(crate) fn shutdown(&self) {
        inject_delay();
        let mut writer = self.writer.lock();
        if writer.is_closed() {
            return;
        }

        if let Err(error) = writer.shutdown() {
            log::error!(
                "encountered error during outbound \
                transition to Closed state: {:?}",
                error
            );
        }
        *writer = Writer::Closed;
        drop(writer);
        self.updated.notify_all();
    }

    // Replaces the underlying stream with a new socket.
    // If the state was `Disconnected`, we will also try
    // to write and flush the entire disconnect buffer into
    // the new socket.
    pub(crate) fn replace_writer(&self, mut new_writer: Writer) -> io::Result<()> {
        inject_io_failure()?;
        inject_delay();
        let mut writer = self.writer.lock();
        if let Writer::Disconnected(ref db) = *writer {
            inject_io_failure()?;
            new_writer.write_all(&db.buf[..db.len])?;
            inject_io_failure()?;
            new_writer.flush()?;
        }
        *writer = new_writer;
        drop(writer);
        self.updated.notify_all();
        Ok(())
    }

    fn with_writer<F>(&self, f: F) -> io::Result<()>
    where
        F: FnOnce(&mut Writer) -> io::Result<()>,
    {
        inject_delay();
        inject_io_failure()?;
        let mut writer = self.writer.lock();
        match (f)(&mut *writer) {
            Ok(()) => Ok(()),
            Err(e) => {
                // Shutdown socket to ensure we propagate the error
                // to the Inbound reader.
                let _unchecked = writer.shutdown();
                Err(e)
            }
        }
    }

    pub(crate) fn send_unsub(&self, sid: usize) -> io::Result<()> {
        self.with_writer(|writer| {
            write!(writer, "UNSUB {}\r\n", sid)?;
            writer.flush()
        })
    }

    pub(crate) fn send_ping(&self) -> io::Result<()> {
        inject_io_failure()?;
        inject_delay();
        let mut writer = self.writer.lock();

        if writer.is_disconnected() {
            return Err(Error::new(
                ErrorKind::NotConnected,
                "The client is not currently connected to a server",
            ));
        }

        writer.write_all(b"PING\r\n")?;
        // Flush in place on pings.
        writer.flush()
    }

    pub(crate) fn send_pong(&self) -> io::Result<()> {
        self.with_writer(|writer| {
            if !writer.is_disconnected() {
                writer.write_all(b"PONG\r\n")?;
                // Flush in place on pings.
                writer.flush()
            } else {
                Ok(())
            }
        })
    }

    pub(crate) fn send_pub_msg(
        &self,
        subj: &str,
        reply: Option<&str>,
        msgb: &[u8],
    ) -> io::Result<()> {
        self.with_writer(|writer| {
            if let Some(reply) = reply {
                write!(writer, "PUB {} {} {}\r\n", subj, reply, msgb.len())?;
            } else {
                write!(writer, "PUB {} {}\r\n", subj, msgb.len())?;
            }
            writer.write_all(msgb)?;
            writer.write_all(b"\r\n")?;
            self.updated.notify_all();
            Ok(())
        })
    }

    pub(crate) fn send_sub_msg(
        &self,
        subject: &str,
        queue: Option<&str>,
        sid: usize,
    ) -> std::io::Result<()> {
        self.with_writer(|writer| {
            match queue {
                Some(q) => write!(writer, "SUB {} {} {}\r\n", subject, q, sid)?,
                None => write!(writer, "SUB {} {}\r\n", subject, sid)?,
            }
            self.updated.notify_all();
            Ok(())
        })
    }

    pub(crate) fn resend_subs(&self, subs: &HashMap<usize, SubscriptionState>) -> io::Result<()> {
        inject_io_failure()?;
        inject_delay();
        let mut writer = self.writer.lock();
        for (sid, SubscriptionState { subject, queue, .. }) in subs {
            match queue {
                Some(q) => write!(writer, "SUB {} {} {}\r\n", subject, q, sid)?,
                None => write!(writer, "SUB {} {}\r\n", subject, sid)?,
            }
        }
        drop(writer);
        self.updated.notify_all();
        Ok(())
    }

    pub(crate) fn send_response(&self, subj: &str, msgb: &[u8]) -> io::Result<()> {
        self.with_writer(|writer| {
            write!(writer, "PUB {} {}\r\n", subj, msgb.len())?;
            writer.write_all(msgb)?;
            writer.write_all(b"\r\n")?;
            self.updated.notify_all();
            Ok(())
        })
    }
}
