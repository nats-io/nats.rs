mod nats_server;
pub use nats_server::*;

#[test]
#[should_panic(expected = "no responders")]
fn no_responders() {
    let s = nats_server::run().unwrap();
    let nc = nats::connect(&s.client_url()).expect("could not connect");
    nc.request("nobody-home", "hello").unwrap();
}
