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

use std::{
    sync::{atomic::AtomicUsize, Arc},
    thread,
    time::Duration,
};

#[test]
fn slow_consumers() {
    let dropped_messages = Arc::new(AtomicUsize::new(0));
    let s = nats_server::run_basic_server();
    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback({
            let dropped_messages = dropped_messages.clone();
            move |err| {
                if err.to_string()
                    == *"slow consumer detected for subscription on subject data. dropping messages"
                {
                    dropped_messages.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        })
        .connect(s.client_url())
        .expect("could not connect");

    let sub = nc.subscribe("data").unwrap();

    // set limits for number of messages
    sub.set_message_limits(100);

    // send messages
    for _ in 0..140 {
        nc.publish("data", b"test message").unwrap();
    }

    // wait a while to trigger slow consumers
    thread::sleep(Duration::from_millis(200));
    let mut i = 0;
    while i < 100 {
        sub.next();
        i += 1;
    }

    // check if numbers align between callback and registered dropped messages
    assert_eq!(
        sub.dropped_messages().unwrap(),
        dropped_messages.load(std::sync::atomic::Ordering::SeqCst)
    );

    // check if expected number of messages were dropped
    assert_eq!(sub.dropped_messages().unwrap(), 40);
}
