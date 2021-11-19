use std::io;
use std::path::PathBuf;

mod nats_server;
pub use nats_server::*;

#[test]
fn basic_tls() -> io::Result<()> {
    let s = nats_server::run_with_config("tests/configs/tls.conf").unwrap();

    assert!(nats::connect("nats://127.0.0.1").is_err());

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    nats::Options::with_user_pass("derek", "porkchop")
        .add_root_certificate(path.join("tests/configs/certs/rootCA.pem"))
        .client_cert(
            path.join("tests/configs/certs/client-cert.pem"),
            path.join("tests/configs/certs/client-key.pem"),
        )
        .connect(&s.client_url())?;

    nats::Options::with_user_pass("derek", "porkchop")
        .add_root_certificate(path.join("tests/configs/certs/rootCA.pem"))
        .client_cert(
            path.join("tests/configs/certs/client-cert.pem"),
            path.join("tests/configs/certs/client-key.pem"),
        )
        .connect(&s.client_url())?;

    Ok(())
}
