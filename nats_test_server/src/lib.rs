use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
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

    /// Turns on additional strict format checking, e.g. for properly formed subjects.
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

    /// If set to `true`, the server (version 1.2.0+) will not send originating messages from this
    /// connection to its own subscriptions. Clients should set this to `true` only for server
    /// supporting this feature, which is when proto in the INFO protocol is set to at least 1.
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
    socket: TcpStream,
    has_sent_ping: bool,
    last_ping: Instant,
    outstanding_pings: usize,
}

fn read_line(stream: &mut TcpStream) -> Option<String> {
    fn ends_with_crlf(buf: &[u8]) -> bool {
        buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n'
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
pub fn nats_test_server(
    host: &str,
    mut port: u16,
    barrier: Arc<Barrier>,
    shutdown: Arc<AtomicBool>,
    restart: Arc<AtomicBool>,
    bugginess: u32,
    hop_ports: bool,
) {
    let mut max_client_id = 0;
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

    let baddr = format!("{}:{}", host, port);
    let mut listener = TcpListener::bind(baddr).unwrap();
    log::info!("nats test server started on {}:{}", host, port);

    barrier.wait();

    let mut clients: HashMap<usize, Client> = HashMap::new();
    let mut subs = HashMap::new();

    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }

        // this makes it nice and bad
        if !clients.is_empty() && thread_rng().gen_bool(1. / bugginess as f64)
            || restart.load(Ordering::Acquire)
        {
            drop(listener);
            log::debug!("evicting all connected clients");
            clients.clear();
            subs.clear();
            let baddr = format!("{}:{}", host, port);
            log::debug!("nats test server restarted on {}:{}", host, port);
            listener = TcpListener::bind(baddr).unwrap();
            listener.set_nonblocking(true).unwrap();
        }

        // maybe accept a new client
        if clients.is_empty() {
            listener.set_nonblocking(false).unwrap();
        } else {
            listener.set_nonblocking(true).unwrap();
        }

        if let Ok((mut next, _addr)) = listener.accept() {
            log::debug!("new client connected");
            max_client_id += 1;
            let client_id = max_client_id;
            next.write_all(server_info(client_id, port).as_bytes())
                .unwrap();
            let _unchecked = next.set_read_timeout(Some(Duration::from_millis(1)));
            clients.insert(
                client_id,
                Client {
                    socket: next,
                    has_sent_ping: false,
                    last_ping: Instant::now(),
                    outstanding_pings: 0,
                },
            );
        }

        let mut to_evict = vec![];
        let mut outbound = vec![];

        for (client_id, client) in &mut clients {
            if client.outstanding_pings > 3 {
                to_evict.push(*client_id);
                continue;
            }

            if client.has_sent_ping && client.last_ping.elapsed() > Duration::from_micros(50) {
                if client.socket.write_all(b"PING\r\n").is_err() {
                    to_evict.push(*client_id);
                    continue;
                }
                client.last_ping = Instant::now();
                client.outstanding_pings += 1;
            }

            let command = if let Some(command) = read_line(&mut client.socket) {
                command
            } else {
                continue;
            };

            log::trace!("got command {}", command);

            let mut parts = command.split(' ');

            match parts.next().unwrap() {
                "PONG" => {
                    assert!(client.outstanding_pings > 0);
                    client.outstanding_pings -= 1;
                    assert_eq!(parts.next(), None);
                }
                "PING" => {
                    assert_eq!(parts.next(), None);
                    if client.socket.write_all(b"PONG\r\n").is_err() {
                        to_evict.push(*client_id);
                        continue;
                    }
                    client.has_sent_ping = true;
                    if hop_ports {
                        // we hop to a new port because we have sent the client the new
                        // server information.
                        port += 1;
                    }
                }
                "CONNECT" => {
                    let _: ConnectInfo = serde_json::from_str(parts.next().unwrap()).unwrap();
                    assert_eq!(parts.next(), None);
                }
                "SUB" => {
                    let subject = parts.next().unwrap();
                    let sid = parts.next().unwrap();
                    assert_eq!(parts.next(), None);
                    let entry = subs.entry(subject.to_string()).or_insert(HashSet::new());
                    entry.insert(sid.to_string());
                }
                "PUB" => {
                    let (subject, reply, len) = match (parts.next(), parts.next(), parts.next()) {
                        (Some(subject), Some(reply), Some(len)) => (subject, Some(reply), len),
                        (Some(subject), Some(len), None) => (subject, None, len),
                        other => panic!("unknown args: {:?}", other),
                    };

                    assert_eq!(parts.next(), None);

                    let next_line = if let Some(next_line) = read_line(&mut client.socket) {
                        next_line
                    } else {
                        to_evict.push(*client_id);
                        continue;
                    };

                    let parsed_len = if let Ok(parsed_len) = len.parse::<usize>() {
                        parsed_len
                    } else {
                        to_evict.push(*client_id);
                        continue;
                    };

                    if parsed_len != next_line.len() {
                        to_evict.push(*client_id);
                        continue;
                    }

                    for sub in subs.get(subject).unwrap_or(&HashSet::new()) {
                        let out = if let Some(group) = reply {
                            format!(
                                "MSG {} {} {} {}\r\n{}\r\n",
                                subject,
                                sub,
                                group,
                                next_line.len(),
                                next_line
                            )
                        } else {
                            format!(
                                "MSG {} {} {}\r\n{}\r\n",
                                subject,
                                sub,
                                next_line.len(),
                                next_line
                            )
                        };

                        outbound.push(out.into_bytes());
                    }
                }
                "UNSUB" => {
                    let sid = parts.next().unwrap();
                    assert_eq!(parts.next(), None);
                    subs.remove(sid);
                }
                other => panic!("unknown command {}", other),
            }
        }

        for out in outbound {
            for (client_id, client) in clients.iter_mut() {
                if client.socket.write_all(&out).is_err() {
                    to_evict.push(*client_id);
                    continue;
                }
            }
        }

        while let Some(client_id) = to_evict.pop() {
            log::debug!("client {} evicted", client_id);
            clients.remove(&client_id);
        }
    }
}
