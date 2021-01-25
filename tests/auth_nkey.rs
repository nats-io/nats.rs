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
        .arg("-V")
        .arg("-D")
        .arg("--config")
        .arg(path.join("tests/configs/nkey.conf"))
        .spawn()
        .unwrap();

    Server { child, _lock }
}

#[test]
fn nkey() -> io::Result<()> {
    let _s = server();

    let nkey = "UAMMBNV2EYR65NYZZ7IAK5SIR5ODNTTERJOBOF4KJLMWI45YOXOSWULM";
    let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    let kp = nkeys::KeyPair::from_seed(seed).unwrap();

    nats::Options::with_nkey(nkey, move |nonce| kp.sign(nonce).unwrap())
        .connect("nats://127.0.0.1:4442")?;

    Ok(())
}
