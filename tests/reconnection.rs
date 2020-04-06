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

fn ends_with_crlf(buf: &[u8]) -> bool {
    buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n'
}

fn read_line(stream: &mut TcpStream) -> Option<String> {
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
    assert_eq!(buf.pop().unwrap(), b'\n');
    assert_eq!(buf.pop().unwrap(), b'\r');
    String::from_utf8(buf).ok()
}

fn bad_server(
    host: &str,
    mut port: u16,
    barrier: Arc<Barrier>,
    shutdown: Arc<AtomicBool>,
    restart: Arc<AtomicBool>,
    chance: u32,
    hop_ports: bool,
) {
    let mut max_client_id = 0;
    let mut server_info = |client_id, port| {
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
            port + 1
        )
    };

    let baddr = format!("{}:{}", host, port);
    let mut listener = TcpListener::bind(baddr).unwrap();
    listener.set_nonblocking(true).unwrap();

    barrier.wait();

    let mut clients = HashMap::new();
    let mut subs = HashMap::new();

    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }

        // this makes it nice and bad
        if thread_rng().gen_bool(1. / chance as f64) || restart.load(Ordering::Acquire) {
            clients.clear();
            subs.clear();
            drop(listener);
            let baddr = format!("{}:{}", host, port);
            listener = TcpListener::bind(baddr).unwrap();
            listener.set_nonblocking(true).unwrap();
        }

        // maybe accept a new client
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

            // we hop to a new port because we have sent the client the new
            // server information.
            port += 1;
        }

        let mut to_evict = vec![];
        let mut outbound = vec![];

        for (client_id, client) in &mut clients {
            if client.outstanding_pings > 3 {
                to_evict.push(*client_id);
                continue;
            }

            if client.has_sent_ping && client.last_ping.elapsed() > Duration::from_millis(50) {
                client.socket.write_all(b"PING\r\n").unwrap();
                client.last_ping = Instant::now();
                client.outstanding_pings += 1;
            }

            let command = if let Some(command) = read_line(&mut client.socket) {
                command
            } else {
                continue;
            };

            println!("got command {}", command);
            let mut parts = command.split(' ');

            match parts.next().unwrap() {
                "PONG" => {
                    assert!(client.outstanding_pings > 0);
                    client.outstanding_pings -= 1;
                }
                "PING" => {
                    client.socket.write_all(b"PONG\r\n").unwrap();
                    client.has_sent_ping = true;
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

                    let next_line = read_line(&mut client.socket).unwrap();

                    assert_eq!(len.parse::<usize>().unwrap(), next_line.len());

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
                other => eprintln!("unknown command {}", other),
            }
        }

        while let Some(client_id) = to_evict.pop() {
            clients.remove(&client_id);
        }

        for out in outbound {
            for (_id, client) in clients.iter_mut() {
                client.socket.write_all(&out).unwrap();
            }
        }
    }
}

#[test]
fn simple_reconnect() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let restart = Arc::new(AtomicBool::new(false));
    let success = Arc::new(AtomicBool::new(false));

    let barrier = Arc::new(Barrier::new(2));
    let server = std::thread::spawn({
        let barrier = barrier.clone();
        let shutdown = shutdown.clone();
        move || bad_server("localhost", 22222, barrier, shutdown, restart, 200, true)
    });

    barrier.wait();

    let nc = Arc::new(nats::connect("localhost:22222").unwrap());

    let tx = std::thread::spawn({
        let nc = nc.clone();
        let success = success.clone();
        move || {
            const EXPECTED_SUCCESSES: usize = 100;
            let mut received = 0;

            while received < EXPECTED_SUCCESSES {
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
                    println!("timed out before we received a response :(");
                }
            }

            success.store(true, Ordering::Release);
        }
    });

    let subscriber = nc.subscribe("rust.tests.faulty_requests").unwrap();

    while !success.load(Ordering::Acquire) {
        for msg in subscriber.timeout_iter(Duration::from_millis(10)) {
            msg.respond("Anything for the story").unwrap();
        }
    }

    shutdown.store(true, Ordering::Release);

    tx.join().unwrap();
    server.join().unwrap();
}
