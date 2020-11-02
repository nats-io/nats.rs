use std::io;

use smol::prelude::*;

#[test]
fn sync_drop_flushes() -> io::Result<()> {
    let nc1 = nats::connect("demo.nats.io")?;
    let nc2 = nats::connect("demo.nats.io")?;

    let inbox = nc1.new_inbox();
    let sub = nc2.subscribe(&inbox)?;
    nc2.flush()?;

    nc1.publish(&inbox, b"hello")?;
    drop(nc1); // Dropping should flush the published message.

    assert_eq!(sub.next().unwrap().data, b"hello");

    Ok(())
}

#[test]
fn async_drop_flushes() -> io::Result<()> {
    smol::block_on(async {
        let nc1 = nats::asynk::connect("demo.nats.io").await?;
        let nc2 = nats::asynk::connect("demo.nats.io").await?;

        let inbox = nc1.new_inbox();
        let mut sub = nc2.subscribe(&inbox).await?;
        nc2.flush().await?;

        nc1.publish(&inbox, b"hello").await?;
        drop(nc1); // Dropping doesn't flush, but the client thread will flush before shutdown.

        assert_eq!(sub.next().await.unwrap().data, b"hello");

        Ok(())
    })
}

#[test]
fn two_connections() -> io::Result<()> {
    smol::block_on(async {
        let nc1 = nats::asynk::connect("demo.nats.io").await?;
        let nc2 = nc1.clone();

        nc1.publish("foo", b"bar").await?;
        nc2.publish("foo", b"bar").await?;

        drop(nc1);
        nc2.publish("foo", b"bar").await?;

        Ok(())
    })
}
