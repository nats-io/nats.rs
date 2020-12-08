use std::io;
use std::path::PathBuf;
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

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let child = Command::new("nats-server")
        .arg("--config")
        .arg(path.join("tests/configs/tls.conf"))
        .spawn()
        .unwrap();

    Server { child, _lock }
}

#[test]
fn smoke() -> io::Result<()> {
    let _s = server();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    assert!(nats::connect("nats://127.0.0.1:4443").is_err());

    nats::Options::with_user_pass("derek", "porkchop")
        .add_root_certificate(path.join("tests/configs/certs/rootCA.pem"))
        .client_cert(
            path.join("tests/configs/certs/client-cert.pem"),
            path.join("tests/configs/certs/client-key.pem"),
        )
        .connect("tls://127.0.0.1:4443")?;

    Ok(())
}
