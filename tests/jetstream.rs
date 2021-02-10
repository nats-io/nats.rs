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
fn jetstream_basics() -> io::Result<()> {
    let server = server();

    let nc = nats::connect(&format!("localhost:{}", server.port)).unwrap();

    let manager = Manager { nc };

    let _ = manager.delete_stream("test1");
    let _ = manager.delete_stream("test2");

    manager.add_stream(StreamConfig {
        name: "test1".to_string(),
        retention: RetentionPolicy::WorkQueue,
        ..Default::default()
    })?;

    manager.add_stream("test2")?;
    manager.stream_info("test2")?;
    manager.add_consumer("test2", "consumer1")?;

    let consumer2_cfg = ConsumerConfig {
        durable_name: Some("consumer2".to_string()),
        ack_policy: AckPolicy::All,
        deliver_subject: Some("consumer2_ds".to_string()),
        ..Default::default()
    };
    manager.add_consumer("test2", &consumer2_cfg)?;
    manager.consumer_info("test2", "consumer1")?;

    for i in 1..=1000 {
        manager.nc.publish("test2", format!("{}", i))?;
    }

    assert_eq!(manager.stream_info("test2")?.state.messages, 1000);

    let consumer1 = Consumer::new(manager.nc.clone(), "test2", "consumer1");

    for _ in 1..=1000 {
        consumer1.process(|_msg| {})?;
    }

    let consumer2 = Consumer::new(manager.nc.clone(), "test2", consumer2_cfg);

    let mut count = 0;
    consumer2.process_batch(1000, |_msg| {
        count += 1;
    })?;
    assert_eq!(count, 1000);

    // sequence numbers start with 1
    for i in 1..=500 {
        manager.delete_message("test2", i)?;
    }

    assert_eq!(manager.stream_info("test2")?.state.messages, 500);

    let _ = dbg!(manager.account_info());

    // cleanup
    let streams: io::Result<Vec<StreamInfo>> = manager.list_streams().collect();

    for stream in streams? {
        let consumers: io::Result<Vec<ConsumerInfo>> =
            manager.list_consumers(&stream.config.name)?.collect();

        for consumer in consumers? {
            manager.delete_consumer(&stream.config.name, &consumer.name)?;
        }

        manager.purge_stream(&stream.config.name)?;

        assert_eq!(manager.stream_info(&stream.config.name)?.state.messages, 0);

        manager.delete_stream(&stream.config.name)?;
    }

    Ok(())
}
