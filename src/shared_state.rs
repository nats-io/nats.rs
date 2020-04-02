use std::{
    collections::{HashMap, VecDeque},
    fmt,
    io::{self, BufReader, Error, ErrorKind, Write},
    net::{SocketAddr, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use crate::{
    parser::{parse_control_op, ControlOp},
    AuthStyle, ConnectionStatus, FinalizedOptions, Inbound, Message, Outbound, LANG, VERSION,
};

use crossbeam_channel::Sender;

fn parse_server_addresses<S: AsRef<str>>(connect_str: S) -> std::io::Result<Vec<Server>> {
    fn check_port(nats_url: &str) -> String {
        match nats_url.parse::<SocketAddr>() {
            Ok(_) => nats_url.to_string(),
            Err(_) => {
                if nats_url.find(':').is_some() {
                    nats_url.to_string()
                } else {
                    format!("{}:4222", nats_url)
                }
            }
        }
    }

    let server_strings = connect_str.as_ref().split(',');

    let mut ret = vec![];
    for server_string in server_strings {
        let addr = if server_string.starts_with("nats://") {
            check_port(&server_string["nats://".len()..])
        } else {
            check_port(server_string)
        };
        ret.push(Server {
            addr: addr.to_string(),
            reconnects: 0,
        });
    }

    if ret.is_empty() {
        Err(Error::new(ErrorKind::InvalidInput, "No configured servers"))
    } else {
        Ok(ret)
    }
}

#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) addr: String,
    pub(crate) reconnects: usize,
}

impl Server {
    pub(crate) fn try_connect(
        &mut self,
        options: &FinalizedOptions,
    ) -> io::Result<(BufReader<TcpStream>, ServerInfo)> {
        // wait for a truncated exponential backoff where it starts at 1ms and
        // doubles until it reaches 4 seconds;
        if self.reconnects > 0 {
            let log_2_four_seconds_in_ms = 12_u32;
            let truncated_exponent =
                std::cmp::min(log_2_four_seconds_in_ms, self.reconnects as u32);
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

        let mut inbound = BufReader::with_capacity(64 * 1024, stream.try_clone()?);
        let info = crate::parser::expect_info(&mut inbound)?;

        let parsed_op = parse_control_op(&mut inbound)?;

        match parsed_op {
            ControlOp::Pong => Ok((inbound, info)),
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
pub(crate) struct WorkerThreads {
    inbound: Option<thread::JoinHandle<io::Result<()>>>,
    outbound: Option<thread::JoinHandle<()>>,
}

#[derive(Default)]
pub(crate) struct Callback(pub(crate) RwLock<Option<Box<dyn Fn() + Send + Sync + 'static>>>);

impl fmt::Debug for Callback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let cb = self.0.read();

        f.debug_map()
            .entry(&"callback", if cb.is_some() { &"set" } else { &"unset" })
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct SubscriptionState {
    pub(crate) subject: String,
    pub(crate) queue: Option<String>,
    pub(crate) sender: Sender<Message>,
}

#[derive(Debug)]
pub(crate) struct SharedState {
    pub(crate) threads: Mutex<Option<WorkerThreads>>,
    pub(crate) id: String,
    pub(crate) shutting_down: AtomicBool,
    pub(crate) last_error: RwLock<io::Result<()>>,
    pub(crate) subs: RwLock<HashMap<usize, SubscriptionState>>,
    pub(crate) pongs: Mutex<VecDeque<Sender<bool>>>,
    pub(crate) outbound: Outbound,
    pub(crate) disconnect_callback: Callback,
    pub(crate) reconnect_callback: Callback,
}

impl SharedState {
    pub(crate) fn connect(
        options: FinalizedOptions,
        nats_url: &str,
    ) -> io::Result<Arc<SharedState>> {
        let mut servers = parse_server_addresses(nats_url)?;

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
                    server.reconnects = server.reconnects.overflowing_add(1).0;
                    last_err_opt = Some(e);
                }
            }
        }

        if stream_opt.is_none() {
            // there are no reachable servers. return an error to the caller.
            return Err(last_err_opt.unwrap());
        }

        let (mut inbound, info) = stream_opt.unwrap();

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
            outbound: Outbound::new(inbound.get_mut().try_clone()?),
            disconnect_callback: Callback::default(),
            reconnect_callback: Callback::default(),
            threads: Mutex::new(None),
        });

        let mut inbound = Inbound {
            inbound,
            info,
            options,
            status: ConnectionStatus::Connected,
            server_pool: servers,
            shared_state: shared_state.clone(),
        };

        let inbound_thread = thread::spawn(move || inbound.read_loop());

        let outbound_state = shared_state.clone();
        let outbound_thread = thread::spawn(move || outbound_state.outbound.flush_loop());

        {
            let mut threads = shared_state.threads.lock();
            *threads = Some(WorkerThreads {
                inbound: Some(inbound_thread),
                outbound: Some(outbound_thread),
            });
        }

        Ok(shared_state)
    }

    pub(crate) fn shut_down(&self) {
        let last = self.shutting_down.swap(true, Ordering::SeqCst);
        if !last {
            // already shutting down.
            return;
        }

        self.outbound.signal_shutdown();
        let mut threads = self.threads.lock().take().unwrap();
        let inbound = threads.inbound.take().unwrap();
        let outbound = threads.outbound.take().unwrap();

        inbound.thread().unpark();
        outbound.thread().unpark();

        if let Err(error) = inbound.join() {
            eprintln!("error encountered in inbound thread: {:?}", error);
        }
        if let Err(error) = outbound.join() {
            eprintln!("error encountered in outbound thread: {:?}", error);
        }
    }
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
