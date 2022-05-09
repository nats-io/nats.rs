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

use nats::jetstream::*;
pub use nats_server::*;

#[test]
fn no_messages() {
    let s = nats_server::run_server("tests/configs/jetstream.conf");
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
            &PullSubscribeOptions::new().durable_name("CONSUMER".to_string()),
        )
        .unwrap();
    let mut batch = consumer
        .fetch(BatchOptions {
            batch: 10,
            expires: None,
            no_wait: true,
        })
        .unwrap();

    // every fetch method checks against `404` error, so desipte using blocking iterator we should still get `None`.
    assert!(batch.next().is_none());
}
