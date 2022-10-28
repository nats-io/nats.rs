use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;

#[tokio::test]
async fn kv() -> Result<(), Box<dyn std::error::Error>> {
    let mut key_missing_cmd = Command::cargo_bin("nats-clap")?;

    key_missing_cmd
        .arg("--server")
        .arg("nats://localhost:4222")
        .arg("get")
        .arg("foo");

    key_missing_cmd
        .assert()
        .success()
        .stdout(predicate::str::contains("key 'foo' not found"));

    let mut set_key_cmd = Command::cargo_bin("nats-clap")?;

    set_key_cmd
        .arg("--server")
        .arg("nats://localhost:4222")
        .arg("set")
        .arg("foo")
        .arg("bar");

    set_key_cmd
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "setting key 'foo' to 'bar'\nset key\n",
        ));

    let mut get_key_cmd = Command::cargo_bin("nats-clap")?;

    get_key_cmd
        .arg("--server")
        .arg("nats://localhost:4222")
        .arg("get")
        .arg("foo");

    get_key_cmd
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "reading key 'foo'\n'foo': 'bar'\n",
        ));

    let mut delete_key_cmd = Command::cargo_bin("nats-clap")?;

    delete_key_cmd
        .arg("--server")
        .arg("nats://localhost:4222")
        .arg("delete")
        .arg("foo");

    delete_key_cmd
        .assert()
        .success()
        .stdout(predicate::str::contains("'foo' value deleted\n"));

    Ok(())
}
