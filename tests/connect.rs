use std::io;

#[test]
fn success() -> io::Result<()> {
    nats::Options::new().connect("demo.nats.io")?;
    Ok(())
}

#[test]
fn failure() {
    assert!(nats::Options::with_credentials("non-existent-file")
        .connect("demo.nats.io")
        .is_err());
}

#[test]
fn tls() -> io::Result<()> {
    nats::Options::new().connect("tls://demo.nats.io:4443")?;
    Ok(())
}
