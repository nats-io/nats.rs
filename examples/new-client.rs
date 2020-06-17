use std::io;
use std::thread;
use std::time::Duration;

use nats::new_client::{Connection, Options};

fn main() -> io::Result<()> {
    // Useful commands for testing:
    // nats-sub -s nats://demo.nats.io hello
    // nats-pub -s nats://demo.nats.io hello 'hi from nats-pub'

    let mut nc = Options::new()
        // .with_credentials("/home/stjepan/.nkeys/creds/synadia/First/First.creds")
        // .connect("connect.ngs.global")?;
        .connect("localhost:4222")?;

    let mut sub = nc.subscribe("hello")?;

    nc.publish("hello", "hi from new-client")?;
    nc.flush()?;

    let mut i = 1;
    loop {
        println!("publish {}", i);
        dbg!(nc.publish("hello", format!("msg {}", i)));
        i += 1;
        thread::sleep(Duration::from_secs(1));

        // let msg = dbg!(sub.next());
        // println!("{:?}", msg);
    }

    Ok(())
}
