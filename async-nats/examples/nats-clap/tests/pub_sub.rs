use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;
use std::{thread, time};

#[tokio::test]
async fn pub_sub_message() -> Result<(), Box<dyn std::error::Error>> {
    let mut sub_cmd = Command::cargo_bin("nats-clap")?;

    let handle = thread::spawn(move || {
        sub_cmd
            .arg("--server")
            .arg("nats://localhost:4222")
            .arg("sub")
            .arg("--no-watch")
            .arg("foobar");

        sub_cmd.assert().success().stdout(predicate::str::contains(
            "Awaiting messages\nReceived: 'foobarmessage'\n",
        ));
    });

    thread::sleep(time::Duration::new(1, 0));

    let mut pub_cmd = Command::cargo_bin("nats-clap")?;

    pub_cmd
        .arg("--server")
        .arg("nats://localhost:4222")
        .arg("pub")
        .arg("foobar")
        .arg("foobarmessage");

    pub_cmd.assert().success().stdout(predicate::str::contains(
        "Published to 'foobar': 'foobarmessage'",
    ));

    handle.join().unwrap();

    Ok(())
}
