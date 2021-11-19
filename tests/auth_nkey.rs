use std::io;

mod nats_server;
pub use nats_server::*;

#[test]
fn basic_nkey_auth() -> io::Result<()> {
    let s = nats_server::run_server("tests/configs/nkey.conf").unwrap();

    let nkey = "UAMMBNV2EYR65NYZZ7IAK5SIR5ODNTTERJOBOF4KJLMWI45YOXOSWULM";
    let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    let kp = nkeys::KeyPair::from_seed(seed).unwrap();

    nats::Options::with_nkey(nkey, move |nonce| kp.sign(nonce).unwrap())
        .connect(&s.client_url())?;

    Ok(())
}
