#[test]
fn connect_success() {
    assert!(nats::Options::new().connect("demo.nats.io").is_ok());
}

#[test]
fn connect_failure() {
    assert!(nats::Options::with_credentials("non-existent-file")
        .connect("demo.nats.io")
        .is_err());
}
