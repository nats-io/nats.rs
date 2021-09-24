use nats::MessageError;

#[test]
fn no_responders() -> Result<(), String> {
    let nc = nats::Options::new().connect("demo.nats.io").unwrap();
    let res = nc.request("noresponders", "message").unwrap();
    match res.err {
        Some(MessageError::NoResponders) => Ok(()),
        None => Err(String::from("should be no responders error, got None")),
    }
}