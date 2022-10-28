use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;
use std::{thread, time};

#[tokio::test]
async fn request_reply() -> Result<(), Box<dyn std::error::Error>> {
    let mut reply_cmd = Command::cargo_bin("nats-clap")?;

    let handle = thread::spawn(move || {
        reply_cmd
            .arg("--server")
            .arg("nats://localhost:4222")
            .arg("reply")
            .arg("--reply-limit")
            .arg("3")
            .arg("sprint.guy")
            .arg("GOOD");

        reply_cmd
            .assert()
            .success()
            .stdout(predicate::str::contains(
                r#"Listening for requests on 'sprint.guy'
Received a request 'Can you hear me now?'
Published a response
Received a request 'Can you hear me now?'
Published a response
Received a request 'Can you hear me now?'
Published a response
Response limit reached
"#,
            ));
    });

    thread::sleep(time::Duration::new(1, 0));

    for _ in 0..3 {
        let mut req_cmd = Command::cargo_bin("nats-clap")?;

        req_cmd
            .arg("--server")
            .arg("nats://localhost:4222")
            .arg("request")
            .arg("sprint.guy")
            .arg("Can you hear me now?");

        req_cmd.assert().success().stdout(predicate::str::contains(
            "Waiting on response for \'sprint.guy\'\nResponse is: \'GOOD\'\n",
        ));
    }

    handle.join().unwrap();

    Ok(())
}
