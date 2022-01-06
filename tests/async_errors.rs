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

use std::time::Duration;

use crossbeam_channel::bounded;

mod util;
pub use util::*;

#[test]
fn pub_perms() {
    let s = util::run_server("tests/configs/perms.conf");

    let (dtx, drx) = bounded(1);
    let (etx, erx) = bounded(1);

    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback(move |err| etx.send(err).unwrap())
        .disconnect_callback(move || {
            let _ = dtx.send(true);
        })
        .connect(&s.client_url())
        .expect("could not connect");

    nc.publish("foo", "NOT ALLOWED").unwrap();

    let r = erx.recv_timeout(Duration::from_millis(100));
    assert!(r.is_ok(), "expected an error callback, got none");
    assert_eq!(
        r.unwrap().to_string(),
        r#"Permissions Violation for Publish to "foo""#
    );

    let r = drx.recv_timeout(Duration::from_millis(100));
    assert!(r.is_err(), "we got disconnected on perm violation");
}

#[test]
fn sub_perms() {
    let s = util::run_server("tests/configs/perms.conf");

    let (dtx, drx) = bounded(1);
    let (etx, erx) = bounded(1);

    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback(move |err| etx.send(err).unwrap())
        .disconnect_callback(move || {
            let _ = dtx.send(true);
        })
        .connect(&s.client_url())
        .expect("could not connect");

    let _sub = nc.subscribe("foo").unwrap();

    let r = erx.recv_timeout(Duration::from_millis(100));
    assert!(r.is_ok(), "expected an error callback, got none");
    assert_eq!(
        r.unwrap().to_string(),
        r#"Permissions Violation for Subscription to "foo""#
    );

    let r = drx.recv_timeout(Duration::from_millis(100));
    assert!(r.is_err(), "we got disconnected on perm violation");
}
