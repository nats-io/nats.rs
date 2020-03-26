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

/// Generates a random number in `0..n`.
fn random(n: u32) -> u32 {
    use std::cell::Cell;
    use std::num::Wrapping;

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1_406_868_647));
    }

    RNG.with(|rng| {
        // This is the 32-bit variant of Xorshift.
        //
        // Source: https://en.wikipedia.org/wiki/Xorshift
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng.set(x);

        x.0 % n
    })
}

struct Client {
    partial_cmd: Vec<u8>,
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
    port: u16,
    barrier: Arc<Barrier>,
    shutdown: Arc<AtomicBool>,
    chance: u32,
) {
    let mut max_client_id = 0;
    let server_info = |client_id| {
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
                \"client_id\": {} \
            }}\r\n",
            host, port, client_id,
        )
    };

    let baddr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(baddr).unwrap();
    listener.set_nonblocking(true).unwrap();

    barrier.wait();

    let mut clients = HashMap::new();
    let mut subs = HashMap::new();

    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }

        // this makes it nice and bad
        if random(chance) == 1 {
            clients.clear();
            subs.clear();
        }

        // maybe accept a new client
        if let Ok((mut next, _addr)) = listener.accept() {
            println!("got a client");
            max_client_id += 1;
            let client_id = max_client_id;
            next.write_all(server_info(client_id).as_bytes()).unwrap();
            next.set_read_timeout(Some(Duration::from_millis(1)));
            clients.insert(
                client_id,
                Client {
                    partial_cmd: vec![],
                    socket: next,
                    has_sent_ping: false,
                    last_ping: Instant::now(),
                    outstanding_pings: 0,
                },
            );
        }

        let mut to_evict = vec![];
        let mut outbound = vec![];

        // maybe read a byte from some clients
        for (client_id, client) in &mut clients {
            if client.outstanding_pings > 3 {
                println!(
                    "evicting client who isn't responding to pings: {}",
                    client_id
                );
                to_evict.push(*client_id);
                continue;
            }

            if client.has_sent_ping && client.last_ping.elapsed() > Duration::from_millis(50) {
                client.socket.write_all(b"PING\r\n").unwrap();
                client.last_ping = Instant::now();
                client.outstanding_pings += 1;
            }

            let command = if let Some(command) = read_line(&mut client.socket) {
                println!("got command {}", command);
                command
            } else {
                continue;
            };

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
                    println!("sub");
                    let subject = parts.next().unwrap();
                    let sid = parts.next().unwrap();
                    let entry = subs.entry(subject.to_string()).or_insert(HashSet::new());
                    entry.insert(sid.to_string());
                }
                "PUB" => {
                    println!("pub");
                    let subject = parts.next().unwrap();
                    let len = parts.next().unwrap();
                    let next_line = read_line(&mut client.socket).unwrap();

                    assert_eq!(len.parse::<usize>().unwrap(), next_line.len());

                    for sub in subs.get(subject).unwrap_or(&HashSet::new()) {
                        let out = format!(
                            "MSG {} {} {}\r\n{}\r\n",
                            subject,
                            sub,
                            next_line.len(),
                            next_line
                        );

                        outbound.push(out.into_bytes());
                    }
                }
                other => println!("unknown command {}", other),
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
#[ignore]
fn simple_reconnect() -> std::io::Result<()> {
    let barrier = Arc::new(Barrier::new(2));
    let shutdown = Arc::new(AtomicBool::new(false));

    std::thread::spawn({
        let barrier = barrier.clone();
        let shutdown = shutdown.clone();
        move || bad_server("localhost", 22222, barrier, shutdown, 10)
    });

    barrier.wait();

    let nc = Arc::new(nats::connect("localhost:22222")?);

    let tx = std::thread::spawn({
        let nc = nc.clone();
        move || {
            //let nc = nats::connect("demo.nats.io").unwrap();
            for i in 0..10 {
                println!("tests/reconnection.rs:175 publishing {}", i);
                nc.publish("rust.tests.reconnect", format!("{}", i))?;
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(())
        }
    });

    let subscriber = nc.subscribe("rust.tests.reconnect")?;

    let mut received = 0;
    for msg in subscriber.timeout_iter(Duration::from_millis(10)) {
        println!("subscriber got msg: {:?}", msg);
        received += 1;
    }

    assert_eq!(received, 10, "did not receive the expected messages");

    shutdown.store(true, Ordering::Release);

    tx.join().unwrap()
}
