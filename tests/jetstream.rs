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

    let child = Command::new("nats-server")
        .args(&["--port", "4446"])
        .arg("-js")
        .arg("-V")
        .arg("-D")
        .spawn()
        .unwrap();

    Server { child, _lock }
}

use nats::jetstream::*;

#[test]
fn jetstream_basics() -> io::Result<()> {
    let _s = server();

    let nc = nats::connect("localhost:4446").unwrap();
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
