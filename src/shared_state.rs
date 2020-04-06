use std::{
    collections::{HashMap, VecDeque},
    convert::TryFrom,
    fmt,
    io::{self, BufReader, Error, ErrorKind, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use parking_lot::{Mutex, RwLock};
use rand::{seq::SliceRandom, thread_rng};
use serde::{Deserialize, Serialize};

use crate::{
    parser::{parse_control_op, ControlOp},
    AuthStyle, ConnectionStatus, FinalizedOptions, Inbound, Message, Outbound, LANG, VERSION,
};

use crossbeam_channel::Sender;

// Accepts any input that can be treated as an Iterator over string-like objects
pub(crate) fn parse_server_addresses(
    input: impl IntoIterator<Item = impl AsRef<str>>,
) -> Vec<Server> {
    let mut ret: Vec<Server> = input.into_iter().map(|s| Server::new(s.as_ref())).collect();
    ret.shuffle(&mut thread_rng());
    ret
}

#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) url: String,
    pub(crate) reconnects: usize,
}

impl Server {
    pub(crate) fn new(input: &str) -> Server {
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

        let url = if input.starts_with("nats://") {
            check_port(&input["nats://".len()..])
        } else {
            check_port(input)
        };

        Server {
            url: url,
            reconnects: 0,
        }
    }

    pub(crate) fn try_connect(
        &mut self,
        options: &FinalizedOptions,
    ) -> io::Result<(BufReader<TcpStream>, ServerInfo)> {
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

        // wait for a truncated exponential backoff where it starts at 1ms and
        // doubles until it reaches 4 seconds;
        let backoff_ms = if self.reconnects > 0 {
            let log_2_four_seconds_in_ms = 12_u32;
            let truncated_exponent = std::cmp::min(
                log_2_four_seconds_in_ms,
                u32::try_from(std::cmp::min(u32::max_value() as usize, self.reconnects)).unwrap(),
            );
            2_u64.checked_pow(truncated_exponent).unwrap()
        } else {
            0
        };

        let backoff = Duration::from_millis(backoff_ms);

        // look up network addresses and shuffle them
        let mut addrs: Vec<SocketAddr> = self.url.to_socket_addrs()?.collect();
        addrs.shuffle(&mut thread_rng());

        let mut last_err = Error::new(ErrorKind::AddrNotAvailable, "no results");

        for addr in addrs {
            std::thread::sleep(backoff);

            match self.try_connect_addr(addr, &op) {
                Ok(result) => return Ok(result),
                Err(e) => last_err = e,
            };
        }

        self.reconnects += 1;

        Err(last_err)
    }

    // we split the specific connection function into its own
    // function so we can use the try operator and have it more
    // gracefully feed into `last_err` at the call site.
    fn try_connect_addr(
        &mut self,
        addr: SocketAddr,
        op: &str,
    ) -> io::Result<(BufReader<TcpStream>, ServerInfo)> {
        let mut stream = TcpStream::connect(&addr)?;
        stream.write_all(op.as_bytes())?;

        let mut inbound = BufReader::with_capacity(64 * 1024, stream.try_clone()?);
        let info = crate::parser::expect_info(&mut inbound)?;

        let parsed_op = parse_control_op(&mut inbound)?;

        match parsed_op {
            ControlOp::Pong => {
                self.reconnects = 0;
                Ok((inbound, info))
            }
            ControlOp::Err(e) => Err(Error::new(ErrorKind::ConnectionRefused, e)),
            ControlOp::Ping | ControlOp::Msg(_) | ControlOp::Info(_) | ControlOp::Unknown(_) => {
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
    inbound: Option<thread::JoinHandle<()>>,
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
        let mut servers = parse_server_addresses(nats_url.split(','));

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
                    last_err_opt = Some(e);
                }
            }
        }

        if stream_opt.is_none() {
            // there are no reachable servers. return an error to the caller.
            return Err(last_err_opt.expect("expected at least one valid server URL"));
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
            learned_servers: parse_server_addresses(&info.connect_urls),
            inbound,
            info,
            options,
            status: ConnectionStatus::Connected,
            configured_servers: servers,
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

impl ServerInfo {
    pub(crate) fn learned_servers(&self) -> Vec<Server> {
        parse_server_addresses(&self.connect_urls)
    }
}
