use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt::Display,
    io::{Read, Write},
    mem::ManuallyDrop,
    net::SocketAddr,
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use rand::{thread_rng, Rng};
use serde_derive::{Deserialize, Serialize};

fn default_echo() -> bool {
    true
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_true(field: &bool) -> bool {
    *field
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[inline]
fn is_empty_or_none(field: &Option<String>) -> bool {
    match field {
        Some(inner) => inner.is_empty(),
        None => true,
    }
}

/// Info to construct a CONNECT message.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[doc(hidden)]
#[allow(clippy::module_name_repetitions)]
pub struct ConnectInfo {
    /// Turns on +OK protocol acknowledgements.
    pub verbose: bool,

    /// Turns on additional strict format checking, e.g. for properly formed
    /// subjects.
    pub pedantic: bool,

    /// User's JWT.
    #[serde(rename = "jwt", skip_serializing_if = "is_empty_or_none")]
    pub user_jwt: Option<String>,

    /// Signed nonce, encoded to Base64URL.
    #[serde(rename = "sig", skip_serializing_if = "is_empty_or_none")]
    pub signature: Option<String>,

    /// Optional client name.
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub name: Option<String>,

    /// If set to `true`, the server (version 1.2.0+) will not send originating
    /// messages from this connection to its own subscriptions. Clients
    /// should set this to `true` only for server supporting this feature,
    /// which is when proto in the INFO protocol is set to at least 1.
    #[serde(skip_serializing_if = "is_true", default = "default_echo")]
    pub echo: bool,

    /// The implementation language of the client.
    pub lang: String,

    /// The version of the client.
    pub version: String,

    /// Indicates whether the client requires an SSL connection.
    #[serde(default)]
    pub tls_required: bool,

    /// Connection username (if `auth_required` is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub user: Option<String>,

    /// Connection password (if auth_required is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub pass: Option<String>,

    /// Client authorization token (if auth_required is set)
    #[serde(skip_serializing_if = "is_empty_or_none")]
    pub auth_token: Option<String>,
}

struct Client {
    client_id: usize,
    socket: TcpStream,
    has_sent_ping: bool,
    last_ping: Instant,
    outstanding_pings: usize,
    subs: HashMap<String, HashSet<String>>,
}

fn read_line(stream: &mut TcpStream) -> Option<String> {
    fn ends_with_crlf(buf: &[u8]) -> bool {
        buf.len() >= 2
            && buf[buf.len() - 2] == b'\r'
            && buf[buf.len() - 1] == b'\n'
    }

    let mut buf = vec![];
    while !ends_with_crlf(&buf) {
        let mut read_buf = [0];
        if let Ok(1) = stream.read(&mut read_buf) {
            buf.push(read_buf[0]);
        } else {
            break;
        }
    }
    if buf.len() <= 2 {
        return None;
    }
    if buf.pop() != Some(b'\n') || buf.pop() != Some(b'\r') {
        None
    } else {
        String::from_utf8(buf).ok()
    }
}

/// A test server for NATS-based systems that can inject
/// failures.
pub struct NatsTestServer {
    address: SocketAddr,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

pub struct NatsTestServerBuilder<A> {
    baddr: A,
    hop_ports: bool,
    bugginess: Option<u32>,
}

/// A NATS test server, will be stopped on drop
impl NatsTestServer {
    pub fn build() -> NatsTestServerBuilder<&'static str> {
        NatsTestServerBuilder {
            baddr: "127.0.0.1:0",
            hop_ports: false,
            bugginess: None,
        }
    }

    /// Get the socket address on which the test server is listening
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Consume and stop this server, start building a new one on the same port,
    /// the return value is a builder and so you'll need to call `.spawn()` on
    /// it.
    pub fn restart(self) -> NatsTestServerBuilder<SocketAddr> {
        NatsTestServerBuilder {
            baddr: self.address,
            hop_ports: false,
            bugginess: None,
        }
    }

    /// Leave the server running and join
    pub fn join(self) -> Result<(), Box<dyn Any + Send>> {
        let mut server = ManuallyDrop::new(self);
        server.handle.take().unwrap().join()
    }
}

impl Drop for NatsTestServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            if let Err(_err) = handle.join() {
                log::warn!("Error joining TestNats server thread for shutdown");
            }
            log::debug!("Stopped server");
        }
    }
}

impl<A: ToSocketAddrs + Display + Send + 'static> NatsTestServerBuilder<A> {
    ///  Address for server to listen for NATS connections
    pub fn address<B>(self, baddr: B) -> NatsTestServerBuilder<B> {
        NatsTestServerBuilder {
            baddr,
            hop_ports: self.hop_ports,
            bugginess: self.bugginess,
        }
    }

    /// Set the denominator of the probablity of a bug
    pub fn bugginess(self, bugginess: u32) -> Self {
        Self {
            bugginess: Some(bugginess),
            ..self
        }
    }

    /// Whether to hop ports on connection
    pub fn hop_ports(self, hop_ports: bool) -> Self {
        Self { hop_ports, ..self }
    }

    /// Spawn the server on a thread, returns controller struct which will stop
    /// the server on drop
    pub fn spawn(self) -> NatsTestServer {
        let listener = TcpListener::bind(&self.baddr).unwrap();
        let listen_addr = listener.local_addr().unwrap();
        log::info!(
            "nats test server started on {} (requested {})",
            listen_addr,
            &self.baddr,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = Some({
            let shutdown = shutdown.clone();
            thread::spawn(move || self.run(listener, shutdown))
        });

        NatsTestServer {
            address: listen_addr,
            handle,
            shutdown,
        }
    }

    fn run(self, mut listener: TcpListener, shutdown: Arc<AtomicBool>) {
        let hop_ports = self.hop_ports;
        let bugginess = self.bugginess;

        let baddr = listener.local_addr().unwrap();
        let host = baddr.ip();
        let mut port = baddr.port();

        let mut max_client_id = 0;
        #[rustfmt::skip]
        let server_info = |client_id, port| {
            format!(
                "INFO {{  \
                    \"server_id\": \"test\", \
                    \"server_name\": \"test\", \
                    \"host\": \"{}\", \
                    \"port\": {}, \
                    \"version\": \"bad\", \
                    \"go\": \"bad\", \
                    \"max_payload\": 4096, \
                    \"proto\": 0, \
                    \"client_id\": {}, \
                    \"connect_urls\": [\"{}:{}\"] \
                    }}\r\n",
                host,
                port,
                client_id,
                host,
                if hop_ports { port + 1 } else { port }
            )
        };

        let mut clients: HashMap<usize, Client> = HashMap::new();

        loop {
            if shutdown.load(Ordering::Acquire) {
                return;
            }

            // this makes it nice and bad
            let simulated_failure = !clients.is_empty()
                && bugginess.map_or(false, |bugginess| {
                    thread_rng().gen_bool(1. / bugginess as f64)
                });
            if simulated_failure {
                drop(listener);
                log::debug!("evicting all connected clients");
                clients.clear();
                let baddr = format!("{}:{}", host, port);
                log::debug!("nats test server restarted on {}:{}", host, port);
                listener = TcpListener::bind(baddr).unwrap();
                listener.set_nonblocking(true).unwrap();
            }

            // maybe accept a new client
            listener.set_nonblocking(true).unwrap();

            if let Ok((mut next, _addr)) = listener.accept() {
                log::debug!("new client connected");
                max_client_id += 1;
                let client_id = max_client_id;
                next.write_all(server_info(client_id, port).as_bytes())
                    .unwrap();
                let _unchecked =
                    next.set_read_timeout(Some(Duration::from_millis(1)));
                clients.insert(
                    client_id,
                    Client {
                        client_id,
                        socket: next,
                        has_sent_ping: false,
                        last_ping: Instant::now(),
                        outstanding_pings: 0,
                        subs: HashMap::new(),
                    },
                );
            }

            let mut to_evict = vec![];
            let mut in_flight = vec![];

            for (client_id, client) in &mut clients {
                if client.outstanding_pings > 3 {
                    log::debug!(
                        "{}: outstanding pings {} caused eviction",
                        client_id,
                        client.outstanding_pings
                    );
                    to_evict.push(*client_id);
                    continue;
                }

                if client.has_sent_ping
                    && client.last_ping.elapsed() > Duration::from_millis(50)
                {
                    log::trace!("{}: sending ping", client_id);
                    if let Err(err) = client.socket.write_all(b"PING\r\n") {
                        log::debug!(
                            "{}: socket error {} caused eviction",
                            client_id,
                            err
                        );
                        to_evict.push(*client_id);
                        continue;
                    }
                    client.last_ping = Instant::now();
                    client.outstanding_pings += 1;
                }

                if let Some(command) = read_line(&mut client.socket) {
                    log::trace!(
                        "{}: got command {}",
                        client.client_id,
                        &command
                    );

                    let action = client.handle_command(command, hop_ports);
                    log::trace!(
                        "{}: causes action {:?}",
                        client.client_id,
                        &action
                    );

                    match action {
                        ClientAction::None => {}
                        ClientAction::Evict => {
                            to_evict.push(*client_id);
                        }
                        ClientAction::HopPorts => {
                            port += 1;
                        }
                        ClientAction::Publish {
                            subject,
                            msg,
                            reply,
                        } => {
                            in_flight.push((subject, msg, reply));
                        }
                    }
                }
            }

            for (subject, msg, reply) in in_flight {
                log::trace!("emitting msg [{:?}]", (&subject, &msg, &reply));
                for (client_id, client) in clients.iter_mut() {
                    for sub_id in subject_matches(&subject, &client.subs) {
                        let out = if let Some(group) = &reply {
                            format!(
                                "MSG {} {} {} {}\r\n{}\r\n",
                                subject,
                                sub_id,
                                group,
                                msg.len(),
                                msg
                            )
                        } else {
                            format!(
                                "MSG {} {} {}\r\n{}\r\n",
                                subject,
                                sub_id,
                                msg.len(),
                                msg
                            )
                        };
                        log::trace!("{}: sending [{}]", client_id, out);

                        if let Err(err) =
                            client.socket.write_all(&out.as_bytes())
                        {
                            log::debug!(
                                "{}: socket error {} caused eviction",
                                client_id,
                                err
                            );
                            to_evict.push(*client_id);
                            continue;
                        }
                    }
                }
            }

            while let Some(client_id) = to_evict.pop() {
                log::debug!("client {} evicted", client_id);
                clients.remove(&client_id);
            }
        }
    }
}

#[derive(Debug)]
enum ClientAction {
    None,
    Evict,
    HopPorts,
    Publish {
        subject: String,
        msg: String,
        reply: Option<String>,
    },
}

impl Client {
    fn handle_command(
        &mut self,
        command: String,
        hop_ports: bool,
    ) -> ClientAction {
        let mut parts = command.split(' ');

        match parts.next().unwrap() {
            "PONG" => {
                assert!(self.outstanding_pings > 0);
                self.outstanding_pings -= 1;
                assert_eq!(parts.next(), None);
                ClientAction::None
            }
            "PING" => {
                assert_eq!(parts.next(), None);
                if self.socket.write_all(b"PONG\r\n").is_err() {
                    return ClientAction::Evict;
                }
                self.has_sent_ping = true;
                if hop_ports {
                    // we hop to a new port because we have sent the client the
                    // new server information.
                    return ClientAction::HopPorts;
                }
                ClientAction::None
            }
            "CONNECT" => {
                let _: ConnectInfo =
                    serde_json::from_str(parts.next().unwrap()).unwrap();
                assert_eq!(parts.next(), None);
                ClientAction::None
            }
            "SUB" => {
                let subject = parts.next().unwrap();
                let sid = parts.next().unwrap();
                assert_eq!(parts.next(), None);
                let entry = self
                    .subs
                    .entry(subject.to_string())
                    .or_insert_with(HashSet::new);
                entry.insert(sid.to_string());
                ClientAction::None
            }
            "PUB" => {
                let (subject, reply, len) =
                    match (parts.next(), parts.next(), parts.next()) {
                        (Some(subject), Some(reply), Some(len)) => {
                            (subject, Some(reply), len)
                        }
                        (Some(subject), Some(len), None) => {
                            (subject, None, len)
                        }
                        other => panic!("unknown args: {:?}", other),
                    };

                assert_eq!(parts.next(), None);

                let next_line =
                    if let Some(next_line) = read_line(&mut self.socket) {
                        next_line
                    } else {
                        return ClientAction::Evict;
                    };

                let parsed_len = if let Ok(parsed_len) = len.parse::<usize>() {
                    parsed_len
                } else {
                    return ClientAction::Evict;
                };

                if parsed_len != next_line.len() {
                    return ClientAction::Evict;
                }

                ClientAction::Publish {
                    subject: subject.to_owned(),
                    msg: next_line,
                    reply: reply.map(|r| r.to_owned()),
                }
            }
            "UNSUB" => {
                let sid = parts.next().unwrap();
                assert_eq!(parts.next(), None);
                self.subs.remove(sid);
                ClientAction::None
            }
            other => panic!("unknown command {}", other),
        }
    }
}

/// Find any subscriptions which match the subject
fn subject_matches<'s>(
    subject: &str,
    subscriptions: &'s HashMap<String, HashSet<String>>,
) -> HashSet<&'s String> {
    let mut matches = HashSet::new();
    for (sub_pattern, ids) in subscriptions.iter() {
        if subject_match(subject, sub_pattern) {
            matches.extend(ids);
        }
    }
    matches
}

/// Does the subject match the pattern
fn subject_match(subject: &str, subject_pattern: &str) -> bool {
    let mut pattern_parts = subject_pattern.split('.');
    for subject_part in subject.split('.') {
        if let Some(pattern_part) = pattern_parts.next() {
            if pattern_part == ">" {
                return true;
            } else if pattern_part == subject_part || pattern_part == "*" {
                continue;
            }
        }
        return false;
    }
    true
}

#[test]
fn test_subject_match() {
    assert!(subject_match("sub", "sub"));
    assert!(subject_match("sub", "*"));
    assert!(subject_match("sub", ">"));
    assert!(!subject_match("pub", "sub"));
    assert!(subject_match("sub.pub", "sub.pub"));
    assert!(subject_match("sub.pub", "sub.*"));
    assert!(subject_match("sub.pub", "*.pub"));
    assert!(subject_match("sub.pub", "*.*"));
    assert!(subject_match("sub.pub", ">"));
    assert!(!subject_match("sub.pub", "sub"));
    assert!(!subject_match("sub.pub", "pub"));
}

#[test]
fn test_unused_server_cleanup() {
    let success = Arc::new(AtomicBool::new(false));
    {
        let success = success.clone();
        std::thread::spawn(move || {
            let server = NatsTestServer::build().spawn();
            std::thread::sleep(Duration::from_millis(1));
            std::mem::drop(server);
            success.store(true, Ordering::Release);
        });
    }
    std::thread::sleep(Duration::from_millis(2));
    assert!(success.load(Ordering::Acquire));
}

#[test]
fn test_pub_sub_2_clients() {
    env_logger::init();
    let server = NatsTestServer::build().spawn();

    let conn1 = nats::connect(&server.address().to_string()).unwrap();
    let conn2 = nats::connect(&server.address().to_string()).unwrap();

    let sub = conn1.subscribe("*").unwrap();
    conn2.publish("subject", "message").unwrap();

    sub.next_timeout(Duration::from_millis(100)).unwrap();
}
