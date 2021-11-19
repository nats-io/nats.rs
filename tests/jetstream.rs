use std::{collections::HashSet, io, iter::FromIterator, time::Duration};

mod nats_server;
use nats::jetstream;
use nats::jetstream::*;
pub use nats_server::*;

#[test]
#[ignore]
fn jetstream_not_enabled() {
    let s = nats_server::run_basic_server();
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = nats::jetstream::new(nc);

    let err = js.account_info().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error")
        .to_owned();

    assert_eq!(err.error_code(), jetstream::ErrorCode::NotEnabled);
}

#[test]
fn jetstream_account_not_enabled() {
    let s = nats_server::run_server("tests/configs/jetstream_account_not_enabled.conf");
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = nats::jetstream::new(nc);

    let err = js.account_info().unwrap_err();
    println!("{:?}", err);
    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into jetstream::Error")
        .to_owned();

    assert_eq!(err.error_code(), jetstream::ErrorCode::NotEnabledForAccount);
}

#[test]
fn jetstream_publish() {
    let (_s, nc, js) = run_basic_jetstream();

    // Create the stream using our client API.
    js.add_stream(StreamConfig {
        name: "TEST".to_string(),
        subjects: Some(vec![
            "test".to_string(),
            "foo".to_string(),
            "bar".to_string(),
        ]),
        ..Default::default()
    })
    .unwrap();

    // Lookup the stream for testing.
    js.stream_info("TEST").unwrap();

    let msg = b"Hello JS";

    // Basic publish like NATS core.
    let ack = js.publish("foo", &msg).unwrap();
    assert_eq!(ack.stream, "TEST");
    assert_eq!(ack.sequence, 1);
    assert_eq!(js.stream_info("TEST").unwrap().state.messages, 1);

    // Test stream expectation.
    let err = js
        .publish_with_options(
            "foo",
            &msg,
            &PublishOptions {
                expected_stream: Some("ORDERS".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error")
        .to_owned();

    assert_eq!(err.error_code(), jetstream::ErrorCode::StreamNotMatch);

    // Test last sequence expectation.
    let err = js
        .publish_with_options(
            "foo",
            &msg,
            &PublishOptions {
                expected_last_sequence: Some(10),
                ..Default::default()
            },
        )
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error")
        .to_owned();

    assert_eq!(
        err.error_code(),
        jetstream::ErrorCode::StreamWrongLastSequence
    );

    // Messages should have been rejected
    assert_eq!(js.stream_info("TEST").unwrap().state.messages, 1);

    // Send in a stream with a message id
    let ack = js
        .publish_with_options(
            "foo",
            &msg,
            &PublishOptions {
                id: Some("ZZZ".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(ack.stream, "TEST");
    assert_eq!(ack.sequence, 2);
    assert_eq!(js.stream_info("TEST").unwrap().state.messages, 2);

    // Send in the same message with same message id.
    let ack = js
        .publish_with_options(
            "foo",
            &msg,
            &PublishOptions {
                id: Some("ZZZ".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(ack.stream, "TEST");
    assert_eq!(ack.duplicate, true);
    assert_eq!(ack.sequence, 2);
    assert_eq!(js.stream_info("TEST").unwrap().state.messages, 2);

    // Now try to send one in with the wrong last msgId.
    let err = js
        .publish_with_options(
            "foo",
            msg,
            &PublishOptions {
                expected_last_msg_id: Some("AAA".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error")
        .to_owned();

    assert_eq!(err.error_code(), jetstream::ErrorCode::StreamWrongLastMsgId);
    assert_eq!(js.stream_info("TEST").unwrap().state.messages, 2);

    // Make sure expected sequence works.
    let err = js
        .publish_with_options(
            "foo",
            msg,
            &PublishOptions {
                expected_last_sequence: Some(22),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into error")
        .to_owned();

    assert_eq!(
        err.error_code(),
        jetstream::ErrorCode::StreamWrongLastSequence
    );
    assert_eq!(js.stream_info("TEST").unwrap().state.messages, 2);

    let ack = js
        .publish_with_options(
            "foo",
            msg,
            &PublishOptions {
                expected_last_sequence: Some(2),
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(ack.stream, "TEST");
    assert_eq!(ack.sequence, 3);
    assert_eq!(js.stream_info("TEST").unwrap().state.messages, 3);

    // Test expected last subject sequence.
    // Just make sure that we set the header.
    let sub = nc.subscribe("test").unwrap();

    js.publish_with_options(
        "test",
        msg,
        &PublishOptions {
            expected_last_subject_sequence: Some(1),
            ..Default::default()
        },
    )
    .ok();

    let msg = sub.next_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        msg.headers
            .unwrap()
            .inner
            .get("Nats-Expected-Last-Subject-Sequence")
            .unwrap(),
        &HashSet::from_iter(vec!["1".to_string()])
    );
}

#[test]
fn jetstream_create_stream_and_consumer() -> io::Result<()> {
    let (_s, _nc, js) = run_basic_jetstream();
    js.add_stream("stream1")?;
    js.add_consumer("stream1", "consumer1")?;
    Ok(())
}

#[test]
fn jetstream_queue_process() -> io::Result<()> {
    let (_s, nc, js) = run_basic_jetstream();

    let _ = js.delete_stream("qtest1");

    js.add_stream(StreamConfig {
        name: "qtest1".to_string(),
        retention: RetentionPolicy::WorkQueue,
        storage: StorageType::File,
        ..Default::default()
    })?;

    let mut consumer1 = js.add_consumer(
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
    let (_s, nc, js) = run_basic_jetstream();

    let _ = js.delete_stream("test1");
    let _ = js.delete_stream("test2");

    js.add_stream(StreamConfig {
        name: "test1".to_string(),
        retention: RetentionPolicy::WorkQueue,
        ..Default::default()
    })?;

    js.add_stream("test2")?;
    js.stream_info("test2")?;
    js.add_consumer("test2", "consumer1")?;

    let consumer2_cfg = ConsumerConfig {
        durable_name: Some("consumer2".to_string()),
        ack_policy: AckPolicy::All,
        deliver_subject: Some("consumer2_ds".to_string()),
        ..Default::default()
    };
    js.add_consumer("test2", &consumer2_cfg)?;
    js.consumer_info("test2", "consumer1")?;

    for i in 1..=1000 {
        nc.publish("test2", format!("{}", i))?;
    }

    assert_eq!(js.stream_info("test2")?.state.messages, 1000);

    let mut consumer1 = js.existing("test2", "consumer1")?;

    for _ in 1..=1000 {
        consumer1.process(|_msg| Ok(()))?;
    }

    let mut consumer2 = js.existing("test2", consumer2_cfg)?;

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
        js.delete_message("test2", i)?;
    }

    assert_eq!(js.stream_info("test2")?.state.messages, 500);

    js.add_consumer("test2", "consumer3")?;

    js.existing("test2", "consumer3")?;

    // cleanup
    let streams: io::Result<Vec<StreamInfo>> = js.list_streams().collect();

    for stream in streams? {
        let consumers: io::Result<Vec<ConsumerInfo>> =
            js.list_consumers(&stream.config.name)?.collect();

        for consumer in consumers? {
            js.delete_consumer(&stream.config.name, &consumer.name)?;
        }

        js.purge_stream(&stream.config.name)?;

        assert_eq!(js.stream_info(&stream.config.name)?.state.messages, 0);

        js.delete_stream(&stream.config.name)?;
    }

    Ok(())
}

#[test]
fn jetstream_libdoc_test() {
    let (_s, nc, js) = run_basic_jetstream();

    js.add_stream("my_stream").unwrap();
    nc.publish("my_stream", "1").unwrap();
    nc.publish("my_stream", "2").unwrap();
    nc.publish("my_stream", "3").unwrap();
    nc.publish("my_stream", "4").unwrap();

    let mut consumer = js
        .create_or_bind("my_stream", "existing_or_created_consumer")
        .unwrap();

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
