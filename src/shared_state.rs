use std::{
    collections::{HashMap, VecDeque},
    io::{self, Error, ErrorKind},
    net::{SocketAddr, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

use parking_lot::{Mutex, RwLock};
use rand::{seq::SliceRandom, thread_rng};

use crate::{
    inject_delay, inject_io_failure, FinalizedOptions, Inbound, Message, Outbound, Reader,
    ServerInfo, Writer,
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
        if host_opt.map_or(true, str::is_empty) {
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
        inject_io_failure()?;

        // wait for a truncated exponential backoff where it starts at 1ms and
        // doubles until it reaches 4 seconds;
        let backoff = (options.reconnect_delay_callback.0)(self.reconnects);

        // look up network addresses and shuffle them
        let mut addrs: Vec<SocketAddr> = (&*self.host, self.port).to_socket_addrs()?.collect();
        addrs.shuffle(&mut thread_rng());

        let mut last_err = Error::new(ErrorKind::AddrNotAvailable, "no results");

        for addr in addrs {
            std::thread::sleep(backoff);

            match crate::connect::connect_to_socket_addr(
                addr,
                &self.host,
                self.tls_required,
                options,
            ) {
                Ok(result) => return Ok(result),
                Err(e) => last_err = e,
            };
        }

        self.reconnects += 1;

        Err(last_err)
    }
}

#[derive(Debug)]
pub(crate) struct WorkerThreads {
    pub(crate) inbound: Option<thread::JoinHandle<()>>,
    pub(crate) outbound: Option<thread::JoinHandle<()>>,
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
    pub(crate) info: RwLock<ServerInfo>,
}

impl SharedState {
    pub(crate) fn connect(
        options: FinalizedOptions,
        nats_url: &str,
    ) -> io::Result<Arc<SharedState>> {
        inject_io_failure()?;
        let mut servers = parse_server_addresses(nats_url.split(','));

        let mut last_err_opt = None;
        let mut connected_opt = None;
        for server in &mut servers {
            match server.try_connect(&options) {
                Ok(connected) => {
                    connected_opt = Some(connected);
                    break;
                }
                Err(e) => {
                    // record retry stats
                    last_err_opt = Some(e);
                }
            }
        }

        if connected_opt.is_none() {
            // there are no reachable servers. return an error to the caller.
            return Err(last_err_opt.expect("expected at least one valid server URL"));
        }

        let (reader, writer, info) = connected_opt.unwrap();

        let outbound = Outbound::new(writer, options.reconnect_buffer_size);

        let learned_servers = parse_server_addresses(&info.connect_urls);

        let shared_state = Arc::new(SharedState {
            id: nuid::next(),
            shutting_down: AtomicBool::new(false),
            last_error: RwLock::new(Ok(())),
            subs: RwLock::new(HashMap::new()),
            pongs: Mutex::new(VecDeque::new()),
            outbound,
            threads: Mutex::new(None),
            options,
            info: RwLock::new(info),
        });

        let mut inbound = Inbound {
            learned_servers,
            reader,
            configured_servers: servers,
            shared_state: shared_state.clone(),
        };

        let inbound_thread = thread::spawn(move || inbound.read_loop());

        let outbound_state = shared_state.clone();
        let outbound_thread = thread::spawn(move || outbound_state.outbound.flush_loop());

        {
            inject_delay();
            let mut threads = shared_state.threads.lock();
            *threads = Some(WorkerThreads {
                inbound: Some(inbound_thread),
                outbound: Some(outbound_thread),
            });
        }

        Ok(shared_state)
    }

    pub(crate) fn close(&self) {
        self.shutting_down.store(true, Ordering::Release);
        self.outbound.close();
    }
}
