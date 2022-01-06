mod util;
pub use util::*;

#[test]
#[should_panic(expected = "no responders")]
fn no_responders() {
    let s = util::run_basic_server();
    let nc = nats::connect(&s.client_url()).expect("could not connect");
    nc.request("nobody-home", "hello").unwrap();
}
