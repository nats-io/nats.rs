use std::{
    collections::HashMap,
    io::{self, BufWriter, Write},
    net::{Shutdown, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Condvar, Mutex,
    },
};

use crossbeam_channel::Sender;

use crate::Message;

#[derive(Debug)]
pub(crate) struct Outbound {
    writer: Mutex<BufWriter<TcpStream>>,
    updated: Condvar,
    shutting_down: AtomicBool,
}

impl Outbound {
    pub(crate) fn new(stream: TcpStream) -> Outbound {
        Outbound {
            writer: Mutex::new(BufWriter::with_capacity(64 * 1024, stream)),
            updated: Condvar::new(),
            shutting_down: AtomicBool::new(false),
        }
    }

    pub(crate) fn flush_loop(&self) {
        while !self.shutting_down.load(Ordering::Acquire) {
            let mut writer = self.writer.lock().unwrap();
            writer = self
                .updated
                .wait_while(writer, |w| w.buffer().is_empty())
                .unwrap();

            if let Err(error) = writer.flush() {
                eprintln!("Outbound thread failed to flush: {:?}", error);

                // Shutdown socket to force the reader to handle reconnection.
                self.writer
                    .lock()
                    .unwrap()
                    .get_mut()
                    .shutdown(Shutdown::Both)
                    .unwrap();
            }
        }
    }

    pub(crate) fn replace_stream(&self, new_stream: TcpStream) {
        let mut writer = self.writer.lock().unwrap();
        *writer.get_mut() = new_stream;
    }

    pub(crate) fn signal_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);

        // Shutdown socket.
        self.writer
            .lock()
            .unwrap()
            .get_mut()
            .shutdown(Shutdown::Both)
            .unwrap();
    }

    fn with_writer<F>(&self, f: F) -> io::Result<()>
    where
        F: FnOnce(&mut BufWriter<TcpStream>) -> io::Result<()>,
    {
        let mut writer = self.writer.lock().unwrap();
        match (f)(&mut *writer) {
            Ok(()) => Ok(()),
            Err(e) => {
                // Shutdown socket to ensure we propagate the error
                // to the Inbound reader.
                let _ = writer.get_mut().shutdown(Shutdown::Both);
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
        let mut writer = self.writer.lock().unwrap();
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

    pub(crate) fn resend_subs(
        &self,
        subs: &HashMap<usize, (String, Option<String>, Sender<Message>)>,
    ) -> io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        for (sid, (subject, queue_group, _tx)) in subs {
            match queue_group {
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
