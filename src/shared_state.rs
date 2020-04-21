use std::{
    collections::{HashMap, VecDeque},
    convert::TryFrom,
    io::{self, BufReader, BufWriter, Error, ErrorKind, Write},
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
use serde::Serialize;

use crate::{
    parser::{parse_control_op, ControlOp},
    split_tls, AuthStyle, ConnectionStatus, FinalizedOptions, Inbound, Message, Outbound, Reader,
    ServerInfo, Writer, LANG, VERSION,
};

use crossbeam_channel::Sender;

// Accepts any input that can be treated as an Iterator over string-like objects
pub(crate) fn parse_server_addresses(
    input: impl IntoIterator<Item = impl AsRef<str>>,
) -> Vec<Server> {
    let mut ret: Vec<Server> = input
        .into_iter()
        .filter_map(|s| Server::new(s.as_ref()).ok())
        .collect();
    ret.shuffle(&mut thread_rng());
    ret
}

#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) tls_required: bool,
    pub(crate) reconnects: usize,
}

impl Server {
    pub(crate) fn new(input: &str) -> io::Result<Server> {
        if input.contains(',') {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "only one server URL should be passed to Server::new",
            ));
        }

        let tls_required = if let Some("tls") = input.split("://").next() {
            true
        } else {
            false
        };

        let scheme_separator = "://";
        let host_port = if let Some(idx) = input.find(&scheme_separator) {
            &input[idx + scheme_separator.len()..]
        } else {
            input
        };

        let mut host_port_splits = host_port.split(':');
        let host_opt = host_port_splits.next();
        if host_opt.map(|h| h.is_empty()).unwrap_or(true) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid URL provided: {}", input),
            ));
        };
        let host = host_opt.unwrap().to_string();

        let port_opt = host_port_splits
            .next()
            .and_then(|port_str| port_str.parse().ok());
        let port = port_opt.unwrap_or(4222);

        Ok(Server {
            host,
            port,
            tls_required,
            reconnects: 0,
        })
    }

    pub(crate) fn try_connect(
        &mut self,
        options: &FinalizedOptions,
    ) -> io::Result<(Reader, Writer, ServerInfo)> {
        let mut connect_op = Connect {
            tls_required: self.tls_required,
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
        let mut addrs: Vec<SocketAddr> = (&*self.host, self.port).to_socket_addrs()?.collect();
        addrs.shuffle(&mut thread_rng());

        let mut last_err = Error::new(ErrorKind::AddrNotAvailable, "no results");

        for addr in addrs {
            std::thread::sleep(backoff);

            match self.try_connect_inner(addr, &op) {
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
    fn try_connect_inner(
        &mut self,
        addr: SocketAddr,
        op: &str,
    ) -> io::Result<(Reader, Writer, ServerInfo)> {
        let mut stream = TcpStream::connect(&addr)?;
        stream.write_all(op.as_bytes())?;

        let mut inbound = BufReader::with_capacity(64 * 1024, stream.try_clone()?);
        let info = crate::parser::expect_info(&mut inbound)?;

        if self.tls_required || info.tls_required {}

        let parsed_op = parse_control_op(&mut inbound)?;

        match parsed_op {
            ControlOp::Pong => {
                self.reconnects = 0;
                if info.tls_required || self.tls_required {
                    let connector = native_tls::TlsConnector::new().unwrap();
                    match connector.connect(&self.host, stream) {
                        Ok(tls) => {
                            let (tls_reader, tls_writer) = split_tls(tls);
                            let reader =
                                Reader::Tls(BufReader::with_capacity(64 * 1024, tls_reader));
                            let writer =
                                Writer::Tls(BufWriter::with_capacity(64 * 1024, tls_writer));
                            Ok((reader, writer, info))
                        }
                        Err(e) => {
                            eprintln!("failed to upgrade TLS: {:?}", e);
                            Err(Error::new(ErrorKind::PermissionDenied, e))
                        }
                    }
                } else {
                    let reader =
                        Reader::Tcp(BufReader::with_capacity(64 * 1024, stream.try_clone()?));
                    let writer = Writer::Tcp(BufWriter::with_capacity(64 * 1024, stream));
                    Ok((reader, writer, info))
                }
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

#[derive(Debug)]
pub(crate) struct SubscriptionState {
    pub(crate) subject: String,
    pub(crate) queue: Option<String>,
    pub(crate) sender: Sender<Message>,
}

#[derive(Debug)]
pub(crate) struct SharedState {
    pub(crate) options: FinalizedOptions,
    pub(crate) threads: Mutex<Option<WorkerThreads>>,
    pub(crate) id: String,
    pub(crate) shutting_down: AtomicBool,
    pub(crate) last_error: RwLock<io::Result<()>>,
    pub(crate) subs: RwLock<HashMap<usize, SubscriptionState>>,
    pub(crate) pongs: Mutex<VecDeque<Sender<bool>>>,
    pub(crate) outbound: Outbound,
}

impl SharedState {
    pub(crate) fn connect(
        options: FinalizedOptions,
        nats_url: &str,
    ) -> io::Result<Arc<SharedState>> {
        let mut servers = parse_server_addresses(nats_url.split(','));

        let mut last_err_opt = None;
        let mut connected_opt = None;
        'outer: for _ in 0..options.max_reconnects.unwrap_or(5) {
            for server in &mut servers {
                match server.try_connect(&options) {
                    Ok(connected) => {
                        connected_opt = Some(connected);
                        break 'outer;
                    }
                    Err(e) => {
                        // record retry stats
                        last_err_opt = Some(e);
                    }
                }
            }
        }

        if connected_opt.is_none() {
            // there are no reachable servers. return an error to the caller.
            return Err(last_err_opt.expect("expected at least one valid server URL"));
        }

        let (reader, writer, info) = connected_opt.unwrap();

        let outbound = Outbound::new(writer);

        let shared_state = Arc::new(SharedState {
            id: nuid::next(),
            shutting_down: AtomicBool::new(false),
            last_error: RwLock::new(Ok(())),
            subs: RwLock::new(HashMap::new()),
            pongs: Mutex::new(VecDeque::new()),
            outbound,
            threads: Mutex::new(None),
            options,
        });

        let mut inbound = Inbound {
            learned_servers: parse_server_addresses(&info.connect_urls),
            reader,
            info,
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
    #[serde(default)]
    tls_required: bool,

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
        Some(inner) => inner.is_empty(),
        None => true,
    }
}
