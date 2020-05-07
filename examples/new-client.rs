use nats::*;
use std::io;

fn main() -> io::Result<()> {
    // Useful commands for testing:
    // nats-sub -s nats://demo.nats.io:4222 hello
    // nats-pub -s nats://demo.nats.io:4222 hello 'hi from nats-pub'

    let mut nc = nats::new_client::Conn::connect("demo.nats.io:4222")?;
    nc.publish("hello", "hi from new_client")?;

    std::thread::sleep_ms(10000);

    Ok(())
}
