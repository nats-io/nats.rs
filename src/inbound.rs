use std::{
    io::{self, BufReader, Read},
    net::TcpStream,
    sync::{atomic::Ordering, Arc},
};

use rand::{seq::SliceRandom, thread_rng};

use crate::{
    parser::{parse_control_op, ControlOp, MsgArgs},
    ConnectionStatus, FinalizedOptions, Message, Server, ServerInfo, SharedState,
    SubscriptionState,
};

#[derive(Debug)]
pub(crate) struct Inbound {
    pub(crate) inbound: BufReader<TcpStream>,
    pub(crate) configured_servers: Vec<Server>,
    pub(crate) learned_servers: Vec<Server>,
    pub(crate) shared_state: Arc<SharedState>,
    pub(crate) status: ConnectionStatus,
    pub(crate) info: ServerInfo,
    pub(crate) options: FinalizedOptions,
}

impl Drop for Inbound {
    fn drop(&mut self) {
        self.status = ConnectionStatus::Closed;
    }
}

impl Inbound {
    pub(crate) fn read_loop(&mut self) {
        loop {
            if self.shared_state.shutting_down.load(Ordering::Acquire) {
                return;
            }

            if let Err(e) = self.read_and_process_message() {
                eprintln!("failed to process message: {:?}", e);
                self.reconnect().unwrap();
            }
        }
    }

    fn read_and_process_message(&mut self) -> io::Result<()> {
        let parsed_op = parse_control_op(&mut self.inbound)?;
        match parsed_op {
            ControlOp::Msg(msg_args) => self.process_msg(msg_args)?,
            ControlOp::Ping => self.shared_state.outbound.send_pong()?,
            ControlOp::Pong => self.process_pong(),
            ControlOp::Info(new_info) => self.process_info(new_info),
            ControlOp::Err(_) | ControlOp::Unknown(_) => {
                eprintln!("Received unhandled message: {:?}", parsed_op)
            }
        }
        Ok(())
    }

    fn reconnect(&mut self) -> io::Result<()> {
        self.status = ConnectionStatus::Disconnected;
        self.shared_state
            .outbound
            .transition_to_disconnect_buffer(self.options.reconnect_buffer_size);
        // flush outstanding pongs
        {
            let mut pongs = self.shared_state.pongs.lock();
            while let Some(s) = pongs.pop_front() {
                s.send(true).unwrap();
            }
        }

        // clear any captured errors
        *self.shared_state.last_error.write() = Ok(());

        // execute disconnect callback if registered
        if let Some(ref cb) = &*self.shared_state.disconnect_callback.0.read() {
            (cb)();
        }

        self.status = ConnectionStatus::Reconnecting;

        // loop through our known servers until we establish a connection, backing-off
        // more each time we cycle through the known set.
        'outer: loop {
            self.configured_servers.shuffle(&mut thread_rng());
            self.learned_servers.shuffle(&mut thread_rng());

            let filter = if let Some(max_reconnects) = self.options.max_reconnects {
                // only filter servers out if there exists at least one server
                // that would NOT be filtered out.
                self.configured_servers
                    .iter()
                    .chain(self.learned_servers.iter())
                    .any(|s| s.reconnects <= max_reconnects)
            } else {
                false
            };

            for server in self
                .learned_servers
                .iter_mut()
                .chain(self.configured_servers.iter_mut())
            {
                if filter && server.reconnects > self.options.max_reconnects.unwrap() {
                    continue;
                }
                if let Ok((inbound, info)) = server.try_connect(&self.options) {
                    // replace our inbound and writer to correspond with the new socket
                    self.inbound = inbound;
                    self.info = info;
                    let stream: TcpStream = self.inbound.get_mut().try_clone().unwrap();
                    if self.shared_state.outbound.replace_stream(stream).is_err() {
                        // record retry stats
                        server.reconnects = server.reconnects.overflowing_add(1).0;
                    } else {
                        server.reconnects = 0;
                        self.learned_servers = self.info.learned_servers();
                        break 'outer;
                    }
                } else {
                    // record retry stats
                    server.reconnects = server.reconnects.overflowing_add(1).0;
                }
            }
        }

        // resend subscriptions
        self.shared_state
            .outbound
            .resend_subs(&self.shared_state.subs.read())?;

        // TODO(tan) send the buffered items

        self.status = ConnectionStatus::Connected;

        // trigger reconnected callback
        if let Some(ref cb) = &*self.shared_state.reconnect_callback.0.read() {
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
        self.info = new_info;
        self.learned_servers = self.info.learned_servers();
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

        let inbound = &mut self.inbound;
        // FIXME(dlc) - avoid copy if possible.
        inbound
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
