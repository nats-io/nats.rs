use std::io;

#[test]
fn drop_flushes() -> io::Result<()> {
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
fn two_connections() -> io::Result<()> {
    let nc1 = nats::connect("demo.nats.io")?;
    let nc2 = nc1.clone();

    nc1.publish("foo", b"bar")?;
    nc2.publish("foo", b"bar")?;

    drop(nc1);
    nc2.publish("foo", b"bar")?;

    Ok(())
}
