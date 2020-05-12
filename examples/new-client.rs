use std::io;
use std::thread;
use std::time::Duration;

use nats::new_client::Connection;

fn main() -> io::Result<()> {
    // Useful commands for testing:
    // nats-sub -s nats://demo.nats.io hello
    // nats-pub -s nats://demo.nats.io hello 'hi from nats-pub'

    let mut nc = Connection::connect("demo.nats.io:4222")?;
    let mut sub = nc.subscribe("hello");

    thread::sleep(Duration::from_secs(1));

    nc.publish("hello", "hi from new-client")?;

    loop {
        let msg = sub.next_msg()?;
        println!("{}", msg);
    }
}
