mod util;
use std::time::Duration;

use crossbeam_channel::bounded;
pub use util::*;

#[test]
#[cfg_attr(target_os = "windows", ignore)]
fn lame_duck_mode() {
    let (ltx, lrx) = bounded(1);

    let s = util::run_basic_server();
    let nc = nats::Options::new()
        .lame_duck_callback(move || ltx.send(true).unwrap())
        .connect(s.client_url().as_str())
        .expect("could not connect to the server");
    let _sub = nc.subscribe("foo").unwrap();
    set_lame_duck_mode(&s);
    let r = lrx.recv_timeout(Duration::from_millis(500));
    assert!(r.is_ok(), "expected lame duck response, got nothing");
}
