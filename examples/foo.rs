use nats::*;
use std::io;

fn main() -> io::Result<()> {
    // let nc = ConnectionOptions::new()
    //     .with_credentials("/home/stjepan/.nkeys/creds/synadia/First/First.creds")
    //     .connect("connect.ngs.global")?;
    // nc.publish("help", "just testing")?;

    // nats::new_client::Conn::connect("connect.ngs.global:4222")?;
    let mut nc = nats::new_client::Connection::connect("demo.nats.io:4222")?;
    nc.publish("hello", "world")?;
    std::thread::sleep_ms(10000);

    Ok(())
}
