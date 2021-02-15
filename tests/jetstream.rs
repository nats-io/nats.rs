#![cfg(feature = "jetstream")]
use std::io;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering::SeqCst};

struct Server {
    child: Child,
    port: u16,
    storage_dir: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        std::fs::remove_dir_all(&self.storage_dir).unwrap();
    }
}

/// Starts a local NATS server that gets killed on drop.
fn server() -> Server {
    static PORT: AtomicU16 = AtomicU16::new(4333);
    let port = PORT.fetch_add(1, SeqCst);
    let storage_dir = format!("jetstream_test_{}", port);
    let _ = std::fs::remove_dir_all(&storage_dir);

    let child = Command::new("nats-server")
        .args(&["--port", &port.to_string()])
        .arg("-js")
        .args(&["-sd", &storage_dir])
        .arg("-V")
        .arg("-D")
        .spawn()
        .unwrap();

    Server {
        child,
        port,
        storage_dir,
    }
}

use nats::jetstream::*;

#[test]
fn jetstream_create_consumer() -> io::Result<()> {
    let server = server();

    let nc = nats::connect(&format!("localhost:{}", server.port)).unwrap();

    nc.create_stream("stream1")?;
    let consumer = nc.create_consumer("stream1", "consumer1")?;
    Ok(())
}

#[test]
fn jetstream_basics() -> io::Result<()> {
    let server = server();

    let nc = nats::connect(&format!("localhost:{}", server.port)).unwrap();

    let _ = nc.delete_stream("test1");
    let _ = nc.delete_stream("test2");

    nc.create_stream(StreamConfig {
        name: "test1".to_string(),
        retention: RetentionPolicy::WorkQueue,
        ..Default::default()
    })?;

    nc.create_stream("test2")?;
    nc.stream_info("test2")?;
    nc.create_consumer("test2", "consumer1")?;

    let consumer2_cfg = ConsumerConfig {
        durable_name: Some("consumer2".to_string()),
        ack_policy: AckPolicy::All,
        deliver_subject: Some("consumer2_ds".to_string()),
        ..Default::default()
    };
    nc.create_consumer("test2", &consumer2_cfg)?;
    nc.consumer_info("test2", "consumer1")?;

    for i in 1..=1000 {
        nc.publish("test2", format!("{}", i))?;
    }

    assert_eq!(nc.stream_info("test2")?.state.messages, 1000);

    let consumer1 = Consumer::existing(nc.clone(), "test2", "consumer1")?;

    for _ in 1..=1000 {
        consumer1.process(|_msg| {})?;
    }

    let consumer2 = Consumer::existing(nc.clone(), "test2", consumer2_cfg)?;

    let mut count = 0;
    while count != 1000 {
        consumer2.process_batch(128, |_msg| {
            count += 1;
        })?;
    }
    assert_eq!(count, 1000);

    // sequence numbers start with 1
    for i in 1..=500 {
        nc.delete_message("test2", i)?;
    }

    assert_eq!(nc.stream_info("test2")?.state.messages, 500);

    nc.create_consumer("test2", "consumer3")?;

    let consumer3 = Consumer::existing(nc.clone(), "test2", "consumer3")?;

    let _ = dbg!(nc.account_info());

    // cleanup
    let streams: io::Result<Vec<StreamInfo>> = nc.list_streams().collect();

    for stream in streams? {
        let consumers: io::Result<Vec<ConsumerInfo>> =
            nc.list_consumers(&stream.config.name)?.collect();

        for consumer in consumers? {
            nc.delete_consumer(&stream.config.name, &consumer.name)?;
        }

        nc.purge_stream(&stream.config.name)?;

        assert_eq!(nc.stream_info(&stream.config.name)?.state.messages, 0);

        nc.delete_stream(&stream.config.name)?;
    }

    Ok(())
}
