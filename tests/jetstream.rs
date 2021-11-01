use std::io;

mod util;
use nats::jetstream::*;
pub use util::*;

#[test]
fn jetstream_create_stream_and_consumer() -> io::Result<()> {
    let (_s, nc) = run_basic_jetstream();
    nc.create_stream("stream1")?;
    nc.create_consumer("stream1", "consumer1")?;
    Ok(())
}

#[test]
fn jetstream_queue_process() -> io::Result<()> {
    let (_s, nc) = run_basic_jetstream();

    let _ = nc.delete_stream("qtest1");

    nc.create_stream(StreamConfig {
        name: "qtest1".to_string(),
        retention: RetentionPolicy::WorkQueue,
        storage: StorageType::File,
        ..Default::default()
    })?;

    let mut consumer1 = nc.create_consumer(
        "qtest1",
        ConsumerConfig {
            max_deliver: 5,
            durable_name: Some("consumer1".to_string()),
            ack_policy: AckPolicy::Explicit,
            replay_policy: ReplayPolicy::Instant,
            deliver_policy: DeliverPolicy::All,
            ack_wait: 30 * 1_000_000_000,
            deliver_subject: None,
            ..Default::default()
        },
    )?;

    for i in 1..=1000 {
        nc.publish("qtest1", format!("{}", i))?;
    }

    for _ in 1..=1000 {
        consumer1.process(|_msg| Ok(()))?;
    }

    Ok(())
}

#[test]
fn jetstream_basics() -> io::Result<()> {
    let (_s, nc) = run_basic_jetstream();

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

    let mut consumer1 = Consumer::existing(nc.clone(), "test2", "consumer1")?;

    for _ in 1..=1000 {
        consumer1.process(|_msg| Ok(()))?;
    }

    let mut consumer2 = Consumer::existing(nc.clone(), "test2", consumer2_cfg)?;

    let mut count = 0;
    while count != 1000 {
        let _: Vec<()> = consumer2
            .process_batch(128, |_msg| {
                count += 1;
                Ok(())
            })
            .into_iter()
            .collect::<std::io::Result<Vec<()>>>()?;
    }
    assert_eq!(count, 1000);

    // sequence numbers start with 1
    for i in 1..=500 {
        nc.delete_message("test2", i)?;
    }

    assert_eq!(nc.stream_info("test2")?.state.messages, 500);

    nc.create_consumer("test2", "consumer3")?;

    Consumer::existing(nc.clone(), "test2", "consumer3")?;

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

#[test]
fn jetstream_libdoc_test() {
    use nats::jetstream::Consumer;

    let (_s, nc) = run_basic_jetstream();

    nc.create_stream("my_stream").unwrap();
    nc.publish("my_stream", "1").unwrap();
    nc.publish("my_stream", "2").unwrap();
    nc.publish("my_stream", "3").unwrap();
    nc.publish("my_stream", "4").unwrap();

    let mut consumer =
        Consumer::create_or_open(nc, "my_stream", "existing_or_created_consumer").unwrap();

    // set this very high for CI
    consumer.timeout = std::time::Duration::from_millis(1500);

    consumer.process(|msg| Ok(msg.data.len())).unwrap();

    consumer.process_timeout(|msg| Ok(msg.data.len())).unwrap();

    let msg = consumer.pull().unwrap();
    msg.ack().unwrap();

    let batch_size = 128;
    let results: Vec<std::io::Result<usize>> =
        consumer.process_batch(batch_size, |msg| Ok(msg.data.len()));
    let flipped: std::io::Result<Vec<usize>> = results.into_iter().collect();
    let _sizes: Vec<usize> = flipped.unwrap();
}
