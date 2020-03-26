use std::{
    collections::{HashMap, VecDeque},
    io::{self, BufReader, BufWriter, Error, ErrorKind, Read, Write},
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    parser::{parse_control_op, ControlOp, MsgArgs},
    AuthStyle, FinalizedOptions, Message, Outbound, SharedState, LANG, VERSION,
};

/// A `ConnectionStatus` describes the current sub-status of a `Connection`.
#[derive(Debug, PartialEq, Clone, Copy)]
enum ConnectionStatus {
    /// An established connection.
    Connected,
    /// A permanently closed connection.
    Closed,
    /// A connection that has lost connectivity, but may reestablish connectivity.
    Disconnected,
    /// A connection in the process of reestablishing connectivity after a disconnect.
    Reconnecting,
}

#[derive(Serialize, Debug)]
struct Connect<'a> {
    #[serde(skip_serializing_if = "empty_or_none")]
    name: Option<&'a String>,
    verbose: bool,
    pedantic: bool,
    #[serde(skip_serializing_if = "if_true")]
    echo: bool,
    lang: &'a str,
    version: &'a str,

    // Authentication
    #[serde(skip_serializing_if = "empty_or_none")]
    user: Option<&'a String>,
    #[serde(skip_serializing_if = "empty_or_none")]
    pass: Option<&'a String>,
    #[serde(skip_serializing_if = "empty_or_none")]
    auth_token: Option<&'a String>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn if_true(field: &bool) -> bool {
    *field
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[inline]
fn empty_or_none(field: &Option<&String>) -> bool {
    match field {
        Some(_) => false,
        None => true,
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct ServerInfo {
    server_id: String,
    server_name: String,
    host: String,
    port: i16,
    version: String,
    #[serde(default)]
    auth_required: bool,
    #[serde(default)]
    tls_required: bool,
    max_payload: i32,
    proto: i8,
    client_id: u64,
    go: String,
    #[serde(default)]
    nonce: String,
    #[serde(default)]
    connect_urls: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) addr: String,
    pub(crate) retries: u32,
}

impl Server {
    fn try_connect(
        &mut self,
        options: &FinalizedOptions,
    ) -> io::Result<(BufReader<TcpStream>, ServerInfo)> {
        // wait for a truncated exponential backoff where it starts at 1ms and
        // doubles until it reaches 4 seconds;
        if self.retries > 0 {
            let log_2_four_seconds_in_ms = 12_u32;
            let truncated_exponent = std::cmp::min(log_2_four_seconds_in_ms, self.retries);
            let backoff_ms = 2_u64.checked_pow(truncated_exponent).unwrap();
            let backoff = Duration::from_millis(backoff_ms);
            std::thread::sleep(backoff);
        }

        let mut connect_op = Connect {
            name: options.name.as_ref(),
            pedantic: false,
            verbose: false,
            lang: LANG,
            version: VERSION,
            user: None,
            pass: None,
            auth_token: None,
            echo: !options.no_echo,
        };
        match &options.auth {
            AuthStyle::UserPass(user, pass) => {
                connect_op.user = Some(user);
                connect_op.pass = Some(pass);
            }
            AuthStyle::Token(token) => connect_op.auth_token = Some(token),
            _ => {}
        }
        let op = format!(
            "CONNECT {}\r\nPING\r\n",
            serde_json::to_string(&connect_op)?
        );

        let mut stream = TcpStream::connect(&self.addr)?;
        stream.write_all(op.as_bytes())?;

        let mut reader = BufReader::with_capacity(64 * 1024, stream.try_clone()?);
        let info = crate::parser::expect_info(&mut reader)?;

        let parsed_op = parse_control_op(&mut reader)?;

        match parsed_op {
            ControlOp::Pong => Ok((reader, info)),
            ControlOp::Err(e) => Err(Error::new(ErrorKind::ConnectionRefused, e)),
            ControlOp::Ping
            | ControlOp::Msg(_)
            | ControlOp::Info(_)
            | ControlOp::EOF
            | ControlOp::Unknown(_) => {
                eprintln!(
                    "encountered unexpected control op during connection: {:?}",
                    parsed_op
                );
                Err(Error::new(ErrorKind::ConnectionRefused, "Protocol Error"))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ServerManager {
    pub(crate) reader: BufReader<TcpStream>,
    pub(crate) server_pool: Vec<Server>,
    pub(crate) shared_state: Arc<SharedState>,
    status: ConnectionStatus,
    info: ServerInfo,
    options: FinalizedOptions,
}

impl Drop for ServerManager {
    fn drop(&mut self) {
        self.status = ConnectionStatus::Closed;
    }
}

impl ServerManager {
    pub(crate) fn read_loop(&mut self) -> io::Result<()> {
        loop {
            let parsed_op = parse_control_op(&mut self.reader)?;
            match parsed_op {
                ControlOp::Msg(msg_args) => self.process_msg(msg_args)?,
                ControlOp::Ping => self.send_pong()?,
                ControlOp::Pong => self.process_pong(),
                ControlOp::EOF => {
                    self.status = ConnectionStatus::Disconnected;
                    if self.shared_state.shutting_down.load(Ordering::Acquire) {
                        return Ok(());
                    } else {
                        self.reconnect()?;
                    }
                    self.status = ConnectionStatus::Connected;
                }
                ControlOp::Info(_) | ControlOp::Err(_) | ControlOp::Unknown(_) => {
                    eprintln!("Received unhandled message: {:?}", parsed_op)
                }
            }
        }
    }

    pub(crate) fn connect(
        options: FinalizedOptions,
        mut servers: Vec<Server>,
    ) -> io::Result<ServerManager> {
        let mut last_err_opt = None;
        let mut stream_opt = None;
        for server in &mut servers {
            match server.try_connect(&options) {
                Ok(stream) => {
                    stream_opt = Some(stream);
                    break;
                }
                Err(e) => {
                    // record retry stats
                    server.retries = server.retries.overflowing_add(1).0;
                    last_err_opt = Some(e);
                }
            }
        }

        if stream_opt.is_none() {
            // there are no reachable servers. return an error to the caller.
            return Err(last_err_opt.unwrap());
        }

        let (mut reader, info) = stream_opt.unwrap();

        // TODO(dlc) - Fix, but for now at least signal properly.
        if info.tls_required {
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                "TLS currently not supported",
            ));
        }

        let shared_state = Arc::new(SharedState {
            id: nuid::next(),
            shutting_down: AtomicBool::new(false),
            last_error: RwLock::new(Ok(())),
            subs: RwLock::new(HashMap::new()),
            pongs: Mutex::new(VecDeque::new()),
            writer: Mutex::new(Outbound {
                writer: BufWriter::with_capacity(64 * 1024, reader.get_mut().try_clone()?),
                flusher: None,
                should_flush: true,
                in_flush: false,
                closed: false,
            }),
            disconnect_callback: RwLock::new(None),
            reconnect_callback: RwLock::new(None),
        });

        Ok(ServerManager {
            reader,
            info,
            options,
            status: ConnectionStatus::Connected,
            server_pool: servers,
            shared_state,
        })
    }

    fn reconnect(&mut self) -> io::Result<()> {
        // flush outstanding pongs
        {
            let mut pongs = self.shared_state.pongs.lock().unwrap();
            while let Some(s) = pongs.pop_front() {
                s.send(true).unwrap();
            }
        }

        // clear any captured errors
        *self.shared_state.last_error.write().unwrap() = Ok(());

        // execute disconnect callback if registered
        if let Some(ref cb) = &*self.shared_state.disconnect_callback.read().unwrap() {
            (cb)();
        }

        self.status = ConnectionStatus::Reconnecting;

        // loop through our known servers until we establish a connection, backing-off
        // more each time we cycle through the known set.
        'outer: loop {
            for server in &mut self.server_pool {
                if let Ok((reader, info)) = server.try_connect(&self.options) {
                    // replace our reader and writer to correspond with the new socket
                    self.reader = reader;
                    self.info = info;
                    let stream: TcpStream = self.reader.get_mut().try_clone().unwrap();
                    let mut writer = self.shared_state.writer.lock().unwrap();
                    *writer.writer.get_mut() = stream;
                    server.retries = 0;
                    break 'outer;
                } else {
                    // record retry stats
                    server.retries = server.retries.overflowing_add(1).0;
                }
            }
        }

        // resend subscriptions
        let mut w = self.shared_state.writer.lock().unwrap();
        for (sid, (subject, queue_group, _tx)) in &*self.shared_state.subs.read().unwrap() {
            match queue_group {
                Some(q) => write!(w.writer, "SUB {} {} {}\r\n", subject, q, sid)?,
                None => write!(w.writer, "SUB {} {}\r\n", subject, sid)?,
            }
            if w.should_flush && !w.in_flush {
                w.kick_flusher();
            }
        }

        // TODO(tan) send the buffered items

        // trigger reconnected callback
        if let Some(ref cb) = &*self.shared_state.reconnect_callback.read().unwrap() {
            (cb)();
        }

        Ok(())
    }

    fn process_pong(&mut self) {
        let mut pongs = self.shared_state.pongs.lock().unwrap();
        if let Some(s) = pongs.pop_front() {
            s.send(true).unwrap();
        }
    }

    fn send_pong(&self) -> io::Result<()> {
        let w = &mut self.shared_state.writer.lock().unwrap().writer;
        w.write_all(b"PONG\r\n")?;
        w.flush()?;
        Ok(())
    }

    fn process_msg(&mut self, msg_args: MsgArgs) -> io::Result<()> {
        let mut msg = Message {
            subject: msg_args.subject,
            reply: msg_args.reply,
            data: Vec::with_capacity(msg_args.mlen as usize),
            responder: None,
        };

        // Setup so we can send responses.
        if msg.reply.is_some() {
            msg.responder = Some(self.shared_state.clone());
        }

        let reader = &mut self.reader;
        // FIXME(dlc) - avoid copy if possible.
        // FIXME(dlc) - Just read CRLF? Buffered so should be ok.
        reader
            .take(u64::from(msg_args.mlen))
            .read_to_end(&mut msg.data)?;

        let mut crlf = [0; 2];
        reader.read_exact(&mut crlf)?;

        // Now lookup the subscription's channel.
        let subs = self.shared_state.subs.read().unwrap();
        if let Some((_name, _group, tx)) = subs.get(&msg_args.sid) {
            tx.send(msg).unwrap();
        }
        Ok(())
    }
}
