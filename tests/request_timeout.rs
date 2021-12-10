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

mod util;
use nats::jetstream::*;
use std::time::Duration;

#[test]
fn request_timeout() {
    let s = util::run_server("tests/configs/jetstream.conf");
    let nc = nats::connect(&s.client_url()).expect("could not connect");
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

    let consumer = js
        .pull_subscribe_with_options(
            "foo",
            &PullSubscibeOptions::new().bind_stream("TEST".to_string()),
        )
        .unwrap();

    std::thread::spawn(move || {
        for __ in 0..2 {
            js.publish("foo", b"data").unwrap();
            std::thread::sleep(Duration::from_millis(300));
        }
    });
    // set the expiration of request so only first message can keep up.
    consumer
        .request_batch(BatchOptions {
            expires: Some(Duration::from_millis(200).as_nanos() as usize),
            batch: 2,
            no_wait: false,
        })
        .unwrap();

    let msg = consumer.next_timeout(Duration::from_millis(1000)).unwrap();
    msg.ack().unwrap();

    let _ = consumer
        .next_timeout(Duration::from_millis(1000))
        .expect_err("should timeout");
}
