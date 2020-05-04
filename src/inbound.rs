use std::{
    io::{self, BufRead, BufReader, Error, ErrorKind, Read},
    net::TcpStream,
    sync::{atomic::Ordering, Arc},
};

use rand::{seq::SliceRandom, thread_rng};

use crate::{
    parser::{parse_control_op, ControlOp, MsgArgs},
    Message, Server, ServerInfo, SharedState, SubscriptionState, TlsReader,
};

#[derive(Debug)]
pub(crate) enum Reader {
    Tcp(BufReader<TcpStream>),
    Tls(BufReader<TlsReader>),
}

impl BufRead for Reader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        match self {
            Reader::Tcp(br) => br.fill_buf(),
            Reader::Tls(br) => br.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            Reader::Tcp(br) => br.consume(amt),
            Reader::Tls(br) => br.consume(amt),
        }
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Reader::Tcp(br) => br.read(buf),
            Reader::Tls(br) => br.read(buf),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Inbound {
    pub(crate) reader: Reader,
    pub(crate) configured_servers: Vec<Server>,
    pub(crate) learned_servers: Vec<Server>,
    pub(crate) shared_state: Arc<SharedState>,
}

impl Inbound {
    pub(crate) fn read_loop(&mut self) {
        loop {
            if self.shared_state.shutting_down.load(Ordering::Acquire) {
                return;
            }

            if let Err(e) = self.read_and_process_message() {
                log::error!("failed to process message: {:?}", e);
                log::info!("attempting reconnect after losing server connection");

                if self.reconnect().is_err() {
                    self.shared_state.shutdown();
                    return;
                }
            }
        }
    }

    fn read_and_process_message(&mut self) -> io::Result<()> {
        let parsed_op = parse_control_op(&mut self.reader)?;
        match parsed_op {
            ControlOp::Msg(msg_args) => self.process_msg(msg_args)?,
            ControlOp::Ping => self.shared_state.outbound.send_pong()?,
            ControlOp::Pong => self.process_pong(),
            ControlOp::Info(new_info) => self.process_info(new_info),
            ControlOp::Err(_) | ControlOp::Unknown(_) => {
                log::error!("Received unhandled message: {:?}", parsed_op)
            }
        }
        Ok(())
    }

    fn reconnect(&mut self) -> io::Result<()> {
        // we must hold this mutex while changing state to disconnected,
        // setting the outbound buffer to disconnected, and then clearing
        // all in-flight pongs.
        let mut pongs = self.shared_state.pongs.lock();

        // we must call this while holding the pongs lock to ensure that
        // any calls to `Connection::flush` / `Connection::flush_timeout`
        // witness a disconnected outbound buffer state
        self.shared_state
            .outbound
            .transition_to_disconnected(self.shared_state.options.reconnect_buffer_size);

        // flush outstanding pongs
        while let Some(s) = pongs.pop_front() {
            s.send(false).unwrap();
        }

        // we only need to hold this mutex while setting the outbound buffer
        // to disconnected, and clearing pending pongs.
        drop(pongs);

        // clear any captured errors
        *self.shared_state.last_error.write() = Ok(());

        // execute disconnect callback if registered
        if let Some(ref cb) = self.shared_state.options.disconnect_callback.0 {
            (cb)();
        }

        // loop through our known servers until we establish a connection, backing-off
        // more each time we cycle through the known set.
        'outer: loop {
            self.configured_servers.shuffle(&mut thread_rng());
            self.learned_servers.shuffle(&mut thread_rng());

            let max_reconnects = self.shared_state.options.max_reconnects;

            let servers = self
                .configured_servers
                .iter_mut()
                .chain(self.learned_servers.iter_mut())
                .filter(|s| {
                    if let Some(max) = max_reconnects {
                        s.reconnects < max
                    } else {
                        true
                    }
                });

            let mut attempted = false;

            for server in servers {
                attempted = true;
                if let Ok((reader, writer, info)) = server.try_connect(&self.shared_state.options) {
                    // replace our reader and writer to correspond with the new socket
                    self.reader = reader;
                    if self.shared_state.outbound.replace_writer(writer).is_err() {
                        // record retry stats
                        server.reconnects = server.reconnects.overflowing_add(1).0;
                    } else {
                        server.reconnects = 0;
                        self.learned_servers = info.learned_servers();
                        break 'outer;
                    }
                    *self.shared_state.info.write() = info;
                } else {
                    // record retry stats
                    server.reconnects = server.reconnects.overflowing_add(1).0;
                }
            }

            // If all servers have surpassed the configured reconnection
            // threshold, we will transition into the `Closed` state and shut
            // down this connection.
            if !attempted && self.shared_state.options.max_reconnects.is_some() {
                return Err(Error::new(
                    ErrorKind::TimedOut,
                    &*format!(
                        "failed to reconnect to any known \
                        servers ({:?}) within {} retries",
                        self.configured_servers
                            .iter()
                            .chain(self.learned_servers.iter())
                            .collect::<Vec<_>>(),
                        self.shared_state.options.max_reconnects.unwrap(),
                    ),
                ));
            }
        }

        // resend subscriptions
        self.shared_state
            .outbound
            .resend_subs(&self.shared_state.subs.read())?;

        // trigger reconnected callback
        if let Some(ref cb) = self.shared_state.options.reconnect_callback.0 {
            (cb)();
        }

        Ok(())
    }

    fn process_pong(&mut self) {
        let mut pongs = self.shared_state.pongs.lock();
        if let Some(s) = pongs.pop_front() {
            s.send(true).unwrap();
        }
    }

    fn process_info(&mut self, new_info: ServerInfo) {
        self.learned_servers = new_info.learned_servers();
        *self.shared_state.info.write() = new_info;
    }

    fn process_msg(&mut self, msg_args: MsgArgs) -> io::Result<()> {
        const CRLF_LEN: u32 = 2;

        let mut msg = Message {
            subject: msg_args.subject,
            reply: msg_args.reply,
            data: Vec::with_capacity(msg_args.mlen as usize + CRLF_LEN as usize),
            responder: None,
        };

        // Setup so we can send responses.
        if msg.reply.is_some() {
            msg.responder = Some(self.shared_state.clone());
        }

        let reader = &mut self.reader;
        // FIXME(dlc) - avoid copy if possible.
        reader
            .take(u64::from(msg_args.mlen + CRLF_LEN))
            .read_to_end(&mut msg.data)?;

        // truncate CRLF
        msg.data.truncate(msg_args.mlen as usize);

        // Now lookup the subscription's channel.
        let subs = self.shared_state.subs.read();
        if let Some(SubscriptionState { sender, .. }) = subs.get(&msg_args.sid) {
            sender.send(msg).unwrap();
        }
        Ok(())
    }
}
