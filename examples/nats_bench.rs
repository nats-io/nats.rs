use std::{
    num::NonZeroUsize,
    sync::{Arc, Barrier},
    thread,
    time::Instant,
};

use historian::Histo;
use nats;
use structopt::StructOpt;

lazy_static::lazy_static! {
    static ref HISTOGRAM: Histo = Default::default();
}

/// Simple NATS bench tool
#[derive(Debug, StructOpt)]
struct Args {
    /// The nats server URLs (separated by comma) (default
    /// "nats://127.0.0.1:4222")
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    url: String,

    /// User Credentials File
    #[structopt(long = "creds")]
    creds: Option<String>,

    /// Size of the message. (default 128)
    #[structopt(long, default_value = "128")]
    message_size: usize,

    /// Number of Messages to Publish (default 100000)
    #[structopt(long, short, default_value = "100000")]
    number_of_messages: NonZeroUsize,

    /// Number of Concurrent Publishers (default 1)
    #[structopt(short, long, default_value = "1")]
    publishers: NonZeroUsize,

    /// Number of Concurrent Subscribers
    #[structopt(short = "s", long, default_value = "0")]
    subscribers: usize,

    /// Use TLS Secure Connection
    #[structopt(short = "tls")]
    tls: bool,

    /// The subject to use
    subject: String,
}

fn main() -> std::io::Result<()> {
    let args = Args::from_args();
    let tls = args.tls;
    let url = args.url;
    let creds = args.creds;
    let connect = || {
        let opts = if let Some(creds_path) = creds.clone() {
            nats::Options::with_credentials(creds_path)
        } else {
            nats::Options::new()
        };
        opts.with_name("nats_bench rust client")
            .tls_required(tls)
            .connect(&url)
            .expect("failed to connect to NATS server")
    };

    let messages = if args.number_of_messages.get() % args.publishers.get() != 0
    {
        let bumped_idx =
            (args.number_of_messages.get() / args.publishers.get()) + 1;
        bumped_idx * args.publishers.get()
    } else {
        args.number_of_messages.get()
    };

    let message_size = args.message_size;

    let barrier =
        Arc::new(Barrier::new(1 + args.publishers.get() + args.subscribers));

    let mut threads = vec![];

    let pubs = args.publishers.get();
    for _ in 0..pubs {
        let barrier = barrier.clone();
        let nc = connect();
        let subject = args.subject.clone();
        threads.push(thread::spawn(move || {
            let msg: String = (0..message_size).map(|_| 'a').collect();
            barrier.wait();
            for _ in 0..messages / pubs {
                let before = Instant::now();
                nc.publish(&subject, &msg).unwrap();
                HISTOGRAM.measure(before.elapsed().as_nanos() as f64);
            }
        }));
    }

    for _ in 0..args.subscribers {
        let barrier = barrier.clone();
        let subject = args.subject.clone();
        let nc = connect();
        threads.push(thread::spawn(move || {
            let s = nc.subscribe(&subject).unwrap();
            barrier.wait();
            for _ in 0..messages {
                s.next().unwrap();
            }
        }));
    }

    barrier.wait();

    println!(
        "Starting benchmark [msgs={}, msgsize={}, pubs={}, subs={}]",
        messages,
        args.message_size,
        args.publishers.get(),
        args.subscribers
    );

    let start = Instant::now();

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }

    let end = start.elapsed();

    let millis = std::cmp::max(1, end.as_millis() as u64);
    let frequency = 1000 * messages as u64 / millis;
    let mbps = (args.message_size * messages) as u64 / millis / 1024;

    println!(
        "duration: {:?} frequency: {} mbps: {}",
        end, frequency, mbps
    );

    println!("publish latency breakdown in nanoseconds:");
    println!("                min: {:10.0} ns", HISTOGRAM.percentile(0.0));
    for pctl in &[50., 75., 90., 95., 97.5, 99.0, 99.99, 99.999] {
        println!(
            "{:6.}th percentile: {:10.0} ns",
            pctl,
            HISTOGRAM.percentile(*pctl)
        );
    }
    println!(
        "                max: {:10.0} ns",
        HISTOGRAM.percentile(100.0)
    );

    Ok(())
}
