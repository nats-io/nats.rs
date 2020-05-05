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

fn bad_server(
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
            clients.clear();
            subs.clear();
            let baddr = format!("{}:{}", host, port);
            log::info!("bad server listening on {}:{}", host, port);
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

            if client.has_sent_ping && client.last_ping.elapsed() > Duration::from_millis(50) {
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

            log::debug!("got command {}", command);
            let mut parts = command.split(' ');

            match parts.next().unwrap() {
                "PONG" => {
                    assert!(client.outstanding_pings > 0);
                    client.outstanding_pings -= 1;
                }
                "PING" => {
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
                "CONNECT" => (),
                "SUB" => {
                    let subject = parts.next().unwrap();
                    let sid = parts.next().unwrap();
                    let entry = subs.entry(subject.to_string()).or_insert(HashSet::new());
                    entry.insert(sid.to_string());
                }
                "PUB" => {
                    let (subject, reply, len) = match (parts.next(), parts.next(), parts.next()) {
                        (Some(subject), Some(reply), Some(len)) => (subject, Some(reply), len),
                        (Some(subject), Some(len), None) => (subject, None, len),
                        other => panic!("unknown args: {:?}", other),
                    };

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
                    subs.remove(sid);
                }
                other => log::error!("unknown command {}", other),
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
            clients.remove(&client_id);
        }
    }
}

#[test]
fn simple_reconnect() {
    env_logger::init();

    let shutdown = Arc::new(AtomicBool::new(false));
    let restart = Arc::new(AtomicBool::new(false));
    let success = Arc::new(AtomicBool::new(false));

    let barrier = Arc::new(Barrier::new(2));
    let server = std::thread::spawn({
        let barrier = barrier.clone();
        let shutdown = shutdown.clone();
        let hop_ports = false;
        let bugginess = 200;
        move || {
            bad_server(
                "localhost",
                22222,
                barrier,
                shutdown,
                restart,
                bugginess,
                hop_ports,
            )
        }
    });

    barrier.wait();

    let nc = loop {
        if let Ok(nc) = nats::ConnectionOptions::new()
            .max_reconnects(None)
            .connect("localhost:22222")
        {
            break Arc::new(nc);
        }
    };

    let tx = std::thread::spawn({
        let nc = nc.clone();
        let success = success.clone();
        let shutdown = shutdown.clone();
        move || {
            const EXPECTED_SUCCESSES: usize = 100;
            let mut received = 0;

            while received < EXPECTED_SUCCESSES && !shutdown.load(Ordering::Acquire) {
                if nc
                    .request_timeout(
                        "rust.tests.faulty_requests",
                        "Help me?",
                        std::time::Duration::from_secs(2),
                    )
                    .is_ok()
                {
                    received += 1;
                } else {
                    log::debug!("timed out before we received a response :(");
                }
            }

            if received == EXPECTED_SUCCESSES {
                success.store(true, Ordering::Release);
            }
        }
    });

    let subscriber = nc.subscribe("rust.tests.faulty_requests").unwrap();

    while !success.load(Ordering::Acquire) {
        for msg in subscriber.timeout_iter(Duration::from_millis(10)) {
            msg.respond("Anything for the story");
        }
    }

    shutdown.store(true, Ordering::Release);

    tx.join().unwrap();
    server.join().unwrap();
}
