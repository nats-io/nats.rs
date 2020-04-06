use std::{
    collections::HashMap,
    io::{self, BufWriter, Error, ErrorKind, Write},
    net::{Shutdown, TcpStream},
    sync::atomic::{AtomicBool, Ordering},
};

use parking_lot::{Condvar, Mutex};

use crate::SubscriptionState;

#[derive(Debug)]
struct DisconnectBuffer {
    buf: Box<[u8]>,
    len: usize,
}

impl DisconnectBuffer {
    fn new(buf_sz: usize) -> DisconnectBuffer {
        DisconnectBuffer {
            buf: vec![0; buf_sz].into_boxed_slice(),
            len: 0,
        }
    }
}

#[derive(Debug)]
enum Buffer {
    Connected(BufWriter<TcpStream>),
    Disconnected(DisconnectBuffer),
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Buffer::Connected(bw) => bw.write(buf),
            Buffer::Disconnected(db) => {
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
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Buffer::Connected(bw) => bw.flush(),
            Buffer::Disconnected(_) => Ok(()),
        }
    }
}

impl Buffer {
    fn is_empty(&self) -> bool {
        match self {
            Buffer::Connected(bw) => bw.buffer().is_empty(),
            Buffer::Disconnected(db) => db.len == 0,
        }
    }

    // Replaces the underlying stream with a new socket.
    // If the state was `Disconnected`, we will also try
    // to write and flush the entire disconnect buffer into
    // the new socket.
    fn replace_stream(&mut self, stream: TcpStream) -> io::Result<()> {
        match self {
            Buffer::Connected(ref mut bw) => {
                *(bw.get_mut()) = stream;
                Ok(())
            }
            Buffer::Disconnected(db) => {
                let mut new_bw = BufWriter::with_capacity(64 * 1024, stream);
                new_bw.write_all(&db.buf[..db.len])?;
                new_bw.flush()?;
                *self = Buffer::Connected(new_bw);
                Ok(())
            }
        }
    }

    fn shutdown(&mut self) -> io::Result<()> {
        if let Buffer::Connected(bw) = self {
            bw.get_mut().shutdown(Shutdown::Both)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Outbound {
    writer: Mutex<Buffer>,
    updated: Condvar,
    shutting_down: AtomicBool,
}

impl Outbound {
    pub(crate) fn new(stream: TcpStream) -> Outbound {
        Outbound {
            writer: Mutex::new(Buffer::Connected(BufWriter::with_capacity(
                64 * 1024,
                stream,
            ))),
            updated: Condvar::new(),
            shutting_down: AtomicBool::new(false),
        }
    }

    pub(crate) fn flush_loop(&self) {
        while !self.shutting_down.load(Ordering::Acquire) {
            let mut writer = self.writer.lock();
            while writer.is_empty() {
                self.updated.wait(&mut writer);
            }

            if let Err(error) = writer.flush() {
                eprintln!("Outbound thread failed to flush: {:?}", error);

                // wait for our stream to be replaced by the Inbound during
                // reconnection.
                self.updated.wait(&mut writer);
            }
        }
    }

    pub(crate) fn transition_to_disconnect_buffer(&self, buf_sz: usize) {
        let mut writer = self.writer.lock();
        if let Buffer::Connected(_) = *writer {
            *writer = Buffer::Disconnected(DisconnectBuffer::new(buf_sz));
        }
    }

    pub(crate) fn replace_stream(&self, new_stream: TcpStream) -> io::Result<()> {
        let mut writer = self.writer.lock();
        writer.replace_stream(new_stream)?;
        drop(writer);
        self.updated.notify_all();
        Ok(())
    }

    pub(crate) fn signal_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        let _unchecked = self.writer.lock().shutdown();
    }

    fn with_writer<F>(&self, f: F) -> io::Result<()>
    where
        F: FnOnce(&mut Buffer) -> io::Result<()>,
    {
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
        let mut writer = self.writer.lock();
        writer.write_all(b"PING\r\n")?;
        // Flush in place on pings.
        writer.flush()
    }

    pub(crate) fn send_pong(&self) -> io::Result<()> {
        self.with_writer(|writer| {
            writer.write_all(b"PONG\r\n")?;
            // Flush in place on pings.
            writer.flush()
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
