// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{io, time::Duration};

use nats::jetstream::*;
use nats::{jetstream, Connection};

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
        .expect("should be able to downcast into error");

    assert_eq!(err.error_code(), jetstream::ErrorCode::NotEnabled);
}

#[test]
fn jetstream_account_not_enabled() {
    let s = nats_server::run_server("tests/configs/jetstream_account_not_enabled.conf");
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = nats::jetstream::new(nc);

    let err = js.account_info().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);

    let err = err
        .into_inner()
        .expect("should be able to convert error into inner")
        .downcast::<jetstream::Error>()
        .expect("should be able to downcast into jetstream::Error");

    assert_eq!(err.error_code(), jetstream::ErrorCode::NotEnabledForAccount);
}

#[test]
fn jetstream_publish() {
    let (_s, nc, js) = run_basic_jetstream();

    // Create the stream using our client API.
    js.add_stream(StreamConfig {
        name: "TEST".to_string(),
        subjects: vec!["test".to_string(), "foo".to_string(), "bar".to_string()],
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
        .expect("should be able to downcast into error");

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
        .expect("should be able to downcast into error");

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

    assert!(ack.duplicate);
    assert_eq!(ack.stream, "TEST");
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
        .expect("should be able to downcast into error");

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
        .expect("should be able to downcast into error");

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
            .get("Nats-Expected-Last-Subject-Sequence"),
        Some(&"1".to_string())
    );
}

#[test]
fn jetstream_subscribe() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = nats::jetstream::new(nc.clone());

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .unwrap();

    js.stream_info("TEST").unwrap();

    let sub = js.subscribe("foo").unwrap();

    let payload = b"hello js";
    for _ in 0..10 {
        js.publish("foo", payload).unwrap();
    }

    for _ in 0..10 {
        // And receive it on our subscription
        let msg = sub.next().unwrap();
        msg.ack().unwrap();
        assert_eq!(msg.data, payload);
    }

    nc.flush().unwrap();
    // Check the state of consumer matches up with our expectations
    let info = sub.consumer_info().unwrap();
    assert_eq!(info.config.ack_policy, AckPolicy::Explicit);
    assert_eq!(info.delivered.consumer_seq, 10);
    assert_eq!(info.ack_floor.consumer_seq, 10);

    // publish one more message to check drain behaviour
    js.publish("foo", payload).unwrap();
    // check if we still get messages from drain
    sub.drain().unwrap();
    assert!(sub.next().is_some());

    // check if we are really unsubscribed and cannot get further messages
    js.publish("foo", payload).unwrap();
    assert!(sub.next().is_none());
}

#[test]
fn jetstream_subscribe_durable() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = nats::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .unwrap();

    js.stream_info("TEST").unwrap();

    // Create a durable subscription.
    let sub = js
        .subscribe_with_options(
            "foo",
            &SubscribeOptions::new().durable_name("foobar".to_string()),
        )
        .unwrap();

    let info = sub.consumer_info().unwrap();
    assert_eq!(info.config.durable_name, Some("foobar".to_string()));

    // Drain to delete the consumer.
    sub.drain().unwrap();

    // Re-create the subscription
    let sub = js
        .subscribe_with_options(
            "foo",
            &SubscribeOptions::new().durable_name("foobar".to_string()),
        )
        .unwrap();

    // Check that it has a new delivery subject.
    let old_info = info;
    let new_info = sub.consumer_info().unwrap();

    assert_ne!(
        new_info.config.deliver_subject,
        old_info.config.deliver_subject
    );

    // Unsubscribe to delete the consumer.
    sub.unsubscribe().unwrap();

    // Create again and make sure that works.
    js.subscribe_with_options(
        "foo",
        &SubscribeOptions::new().durable_name("foobar".to_string()),
    )
    .unwrap();
}

#[test]
fn jetstream_queue_subscribe() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = nats::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .unwrap();

    js.stream_info("TEST").unwrap();

    for _ in 0..10 {
        js.publish("bar", b"hello js").unwrap();
    }

    // Create a queue group on "bar" with no explicit durable name, which
    // means that the queue name will be used as the durable name.
    let sub1 = js.queue_subscribe("bar", "v0").unwrap();

    // Since the above JS consumer is created on subject "bar", trying to
    // add a member to the same group but on subject "baz" should fail.
    js.queue_subscribe("baz", "v0").unwrap_err();

    // If the queue group is different, but we try to attach to the existing
    // JS consumer that is created for group "v0", then this should fail.
    js.queue_subscribe_with_options(
        "bar",
        "v1",
        &SubscribeOptions::new().durable_name("v0".to_string()),
    )
    .unwrap_err();

    // However, if a durable name is specified, creating a queue sub with
    // the same queue name is ok, but will feed from a different JS consumer.
    let sub2 = js
        .queue_subscribe_with_options(
            "bar",
            "v0",
            &SubscribeOptions::new().durable_name("other_queue_durable".to_string()),
        )
        .unwrap();

    let msg = sub1.next().unwrap();
    msg.ack().unwrap();
    assert_eq!(msg.data, b"hello js");

    let msg = sub2.next_timeout(Duration::from_secs(1)).unwrap();
    msg.ack().unwrap();
    assert_eq!(msg.data, b"hello js");

    sub1.unsubscribe().unwrap();
    sub2.unsubscribe().unwrap();
}

/// this is a regression test for a bug that caused checking sequence mismatch for all push
/// consumers, not only ordered ones. Because of it, preprocessor tried to recreate the consumer
/// which resulted in errors and not getting messages.
#[test]
fn jetstream_queue_subscribe_no_mismatch_handle() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let con = nats::connect(s.client_url()).unwrap();
    let jsm = nats::jetstream::new(con);

    jsm.add_stream(StreamConfig {
        name: "jobs_stream".to_string(),
        discard: DiscardPolicy::Old,
        subjects: vec!["waiting_jobs".to_string()],
        retention: RetentionPolicy::WorkQueue,
        storage: StorageType::File,
        ..Default::default()
    })
    .unwrap();

    jsm.add_consumer(
        "jobs_stream",
        ConsumerConfig {
            deliver_group: Some("dg".to_string()),
            durable_name: Some("durable".to_string()),
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            deliver_subject: Some("deliver_subject".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let job_sub = jsm
        .queue_subscribe_with_options(
            "waiting_jobs",
            "dg",
            &SubscribeOptions::bind("jobs_stream".to_string(), "durable".to_string())
                .deliver_subject("deliver_subject".to_string())
                .ack_explicit()
                .deliver_all()
                .replay_instant(),
        )
        .unwrap();

    jsm.publish("waiting_jobs", b"foo").unwrap();
    jsm.publish("waiting_jobs", b"foo").unwrap();
    let msg = job_sub.next().unwrap();
    msg.ack().unwrap();

    // simulate disconnection
    drop(job_sub);

    let job_sub = jsm
        .queue_subscribe_with_options(
            "waiting_jobs",
            "dg",
            &SubscribeOptions::bind("jobs_stream".to_string(), "durable".to_string())
                .deliver_subject("deliver_subject".to_string())
                .ack_explicit()
                .deliver_all()
                .replay_instant(),
        )
        .unwrap();

    jsm.publish("waiting_jobs", b"foo").unwrap();
    jsm.publish("waiting_jobs", b"foo").unwrap();
    // we should got this message if we really do not try to handle sequence mismatch for ordered
    // consumers
    let msg = job_sub
        .next_timeout(Duration::from_millis(100))
        .expect("should got a message");
    msg.ack().unwrap();
}

#[test]
fn jetstream_flow_control() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = nats::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .unwrap();

    js.stream_info("TEST").unwrap();

    let sub = js
        .subscribe_with_options(
            "foo",
            &SubscribeOptions::new()
                .durable_name("foo".to_string())
                .deliver_subject("fs".to_string())
                .idle_heartbeat(Duration::from_millis(300))
                .enable_flow_control(),
        )
        .unwrap();

    let info = sub.consumer_info().unwrap();
    assert!(info.config.flow_control);

    // Publish a some messages
    let data = b"hello";
    for _ in 0..250 {
        js.publish("foo", data).unwrap();
    }

    // Wait for a second to force a build up of idle heartbeats and a flow control to be
    std::thread::sleep(Duration::from_secs(1));

    // Make sure no control messages make it through `next`
    for _ in 0..250 {
        let message = sub.next().unwrap();
        assert_eq!(message.data, data);
    }
}

#[test]
fn jetstream_ordered() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = nats::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "foo.*".to_string(),
        ],
        ..Default::default()
    })
    .unwrap();

    js.stream_info("TEST").unwrap();

    let sub = js
        .subscribe_with_options("foo", &SubscribeOptions::ordered().deliver_all())
        .unwrap();

    let info = sub.consumer_info().unwrap();
    assert!(info.config.flow_control);

    for i in 0..250 {
        js.publish("foo", (i as i64).to_be_bytes()).unwrap();
    }

    for i in 0..250 {
        let message = sub.next().unwrap();
        assert_eq!(message.data, (i as i64).to_be_bytes());
    }
}

#[test]
fn jetstream_pull_subscribe_fetch() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::Options::new()
        .error_callback(|err| println!("error!: {}", err))
        .connect(&s.client_url())
        .unwrap();
    let js = nats::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec!["foo".to_string()],
        ..Default::default()
    })
    .unwrap();

    js.stream_info("TEST").unwrap();
    js.add_consumer(
        "TEST",
        ConsumerConfig {
            durable_name: Some("CONSUMER".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    for _ in 0..1000 {
        js.publish("foo", b"lorem").unwrap();
    }

    let consumer = js
        .pull_subscribe_with_options(
            "foo",
            &PullSubscribeOptions::new().durable_name("CONSUMER".to_string()),
        )
        .unwrap();

    let batch = consumer.fetch(10).unwrap();

    let mut i = 0;
    for _ in batch {
        i += 1;
    }
    assert_eq!(i, 10);

    let batch = consumer.fetch(10).unwrap();
    for _ in batch {
        i += 1;
    }
    assert_eq!(i, 20);
}

#[test]
fn jetstream_pull_subscribe_timeout_fetch() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::Options::new()
        .error_callback(|err| println!("error!: {}", err))
        .connect(&s.client_url())
        .unwrap();
    let js = nats::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec!["foo".to_string()],
        ..Default::default()
    })
    .unwrap();

    js.stream_info("TEST").unwrap();
    js.add_consumer(
        "TEST",
        ConsumerConfig {
            durable_name: Some("CONSUMER".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    for _ in 0..15 {
        js.publish("foo", b"lorem").unwrap();
    }

    let consumer = js
        .pull_subscribe_with_options(
            "foo",
            &PullSubscribeOptions::new().durable_name("CONSUMER".to_string()),
        )
        .unwrap();

    let batch = consumer
        .timeout_fetch(10, Duration::from_millis(100))
        .unwrap();

    for msg in batch {
        msg.unwrap().ack().unwrap();
    }

    let batch = consumer
        .timeout_fetch(10, Duration::from_millis(100))
        .unwrap();

    for (j, msg) in batch.enumerate() {
        if j >= 5 {
            msg.unwrap_err();
            break;
        }
        msg.unwrap().ack().unwrap();
    }
}

#[test]
fn jetstream_pull_subscribe_fetch_with_handler() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::Options::new()
        .error_callback(|err| println!("error!: {}", err))
        .connect(&s.client_url())
        .unwrap();
    let js = nats::jetstream::new(nc.clone());

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec!["foo".to_string()],
        ..Default::default()
    })
    .unwrap();

    js.stream_info("TEST").unwrap();
    js.add_consumer(
        "TEST",
        ConsumerConfig {
            durable_name: Some("CONSUMER".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    for _ in 0..15 {
        js.publish("foo", b"lorem").unwrap();
    }

    let consumer = js
        .pull_subscribe_with_options(
            "foo",
            &PullSubscribeOptions::new().durable_name("CONSUMER".to_string()),
        )
        .unwrap();

    let mut i = 0;
    consumer
        .fetch_with_handler(10, |_| {
            i += 1;
            Ok(())
        })
        .unwrap();

    nc.flush().unwrap();
    let info = js.consumer_info("TEST", "CONSUMER").unwrap();
    assert_eq!(info.num_ack_pending, 0);
    assert_eq!(info.num_pending, 5);
    assert_eq!(10, i);
}

#[test]
fn jetstream_pull_subscribe_ephemeral() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::Options::new()
        .error_callback(|err| println!("error!: {}", err))
        .connect(&s.client_url())
        .unwrap();
    let js = nats::jetstream::new(nc);

    js.add_stream(&StreamConfig {
        name: "TEST".to_string(),
        subjects: vec!["foo".to_string()],
        ..Default::default()
    })
    .unwrap();

    js.stream_info("TEST").unwrap();

    js.publish("foo", b"foo").unwrap();

    let consumer = js.pull_subscribe("foo").unwrap();

    consumer.request_batch(1).unwrap();
    consumer.next();
}

#[test]
fn jetstream_pull_subscribe_bad_stream() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::Options::new()
        .error_callback(|err| println!("error!: {}", err))
        .connect(&s.client_url())
        .unwrap();
    let js = nats::jetstream::new(nc);

    js.pull_subscribe("WRONG")
        .expect_err("expected not found stream for a given subject");
}

// Helper function to return server and client.
pub fn run_basic_jetstream() -> (nats_server::Server, Connection, JetStream) {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = JetStream::new(nc.clone(), JetStreamOptions::default());

    (s, nc, js)
}
