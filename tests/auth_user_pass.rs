use std::process::{Child, Command};
use std::sync::{Mutex, MutexGuard};

use once_cell::sync::Lazy;

struct Server {
    child: Child,
    _lock: MutexGuard<'static, ()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

/// Starts a local NATS server that gets killed on drop.
fn server() -> Server {
    // A lock to make sure there is only one nats-server at a time.
    static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
    let _lock = LOCK.lock().unwrap();

    let child = Command::new("nats-server")
        .args(&["--port", "8232"])
        .args(&["--user", "derek"])
        .args(&["--pass", "foo"])
        .spawn()
        .unwrap();

    Server { child, _lock }
}

#[test]
fn smoke() {
    let _s = server();
    assert!(nats::connect("nats://127.0.0.1:8232").is_err());

    // TODO(stjepang): Make it possible to specify user and pass in the URL.
    // - Also make sure to test that credentials in the URL take precedence over
    //   Options.
    // assert!(nats::connect("nats://derek:foo@127.0.0.1:8232").is_ok());

    assert!(nats::Options::with_user_pass("derek", "foo")
        .connect("nats://127.0.0.1:8232")
        .is_ok());
}
