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

#![cfg(feature = "unstable")]

use nats::jetstream::StreamConfig;

use nats::kv::*;

#[test]
fn key_value_entry() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&nats::kv::Config {
            bucket: "ENTRY".to_string(),
            history: 5,
            max_age: std::time::Duration::from_secs(3600),
            ..Default::default()
        })
        .unwrap();

    // Initial state
    assert!(kv.entry("foo").unwrap().is_none());

    // Put
    let revision = kv.put("foo", b"bar").unwrap();
    assert_eq!(revision, 1);

    let entry = kv.entry("foo").unwrap().unwrap();
    assert_eq!(entry.value, b"bar");
    assert_eq!(entry.revision, 1);

    let value = kv.get("foo").unwrap();
    assert_eq!(value, Some(b"bar".to_vec()));

    // Delete
    kv.delete("foo").unwrap();

    let entry = kv.entry("foo").unwrap().unwrap();
    assert_eq!(entry.operation, Operation::Delete);

    let value = kv.get("foo").unwrap();
    assert_eq!(value, None);

    // Create
    let revision = kv.create("foo", b"bar").unwrap();
    assert_eq!(revision, 3);

    // Test conditional updates
    let revision = kv.update("foo", b"rip", revision).unwrap();
    kv.update("foo", b"rip", revision).unwrap();

    let revision = kv.create("bar", b"baz").unwrap();
    kv.update("bar", b"baz", revision).unwrap();

    // Status
    let status = kv.status().unwrap();
    assert_eq!(status.history(), 5);
    assert_eq!(status.bucket(), "ENTRY");
    assert_eq!(status.max_age(), std::time::Duration::from_secs(3600));
    assert_eq!(status.values(), 7);
}

#[test]
fn key_value_short_history() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&nats::kv::Config {
            bucket: "HISTORY".to_string(),
            history: 5,
            ..Default::default()
        })
        .unwrap();

    for i in 0..10 {
        kv.put("value", [i as u8]).unwrap();
    }

    let mut history = kv.history("value").unwrap();
    for i in 5..10 {
        let entry = history.next().unwrap();

        assert_eq!(entry.key, "value");
        assert_eq!(entry.value, [i as u8]);
    }
}

#[test]
fn key_value_long_history() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&nats::kv::Config {
            bucket: "HISTORY".to_string(),
            history: 25,
            ..Default::default()
        })
        .unwrap();

    for i in 0..50 {
        kv.put("value", [i as u8]).unwrap();
    }

    let mut history = kv.history("value").unwrap();
    for i in 25..50 {
        let entry = history.next().unwrap();

        assert_eq!(entry.key, "value");
        assert_eq!(entry.value, [i as u8]);
    }
}

#[test]
fn key_value_watch() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&nats::kv::Config {
            bucket: "WATCH".to_string(),
            history: 10,
            ..Default::default()
        })
        .unwrap();
    let mut watch = kv.watch("foo.>").unwrap();

    // check if we get only foo.bar watch event.
    kv.create("foo", b"ignored").unwrap();
    kv.put("foo.bar", b"lorem").unwrap();
    let entry = watch.next().unwrap();
    assert_eq!(entry.key, "foo.bar".to_string());
    assert_eq!(entry.value, b"lorem");
    // expect revision 2, instead of 1 as two values were put.
    assert_eq!(entry.revision, 2);

    // check if we only get foo.bar.z events
    kv.put("foo", b"ignored").unwrap();
    kv.put("foo.bar.z", b"ipsum").unwrap();
    let entry = watch.next().unwrap();
    assert_eq!(entry.key, "foo.bar.z".to_string());
    assert_eq!(entry.value, b"ipsum");
    // expect revision 4, as two values were inserted.
    assert_eq!(entry.revision, 4);

    kv.delete("foo").unwrap();
    kv.delete("foo.bar").unwrap();
    let entry = watch.next().unwrap();
    assert_eq!(entry.operation, Operation::Delete);
}

#[test]
fn key_value_watch_all() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&nats::kv::Config {
            bucket: "WATCH".to_string(),
            history: 10,
            ..Default::default()
        })
        .unwrap();

    // create second Store to see if
    // https://github.com/nats-io/nats.rs/issues/286 is still affecting our codebase.
    // It was causing one store being able to read data from another.
    let skv = context
        .create_key_value(&nats::kv::Config {
            bucket: "TEST_CONFLICT".to_string(),
            history: 10,
            ..Default::default()
        })
        .unwrap();

    let mut watch = kv.watch_all().unwrap();

    // create some data in second Store to see if `watch` will catch this data and panic.
    skv.create("foo", b"loren").unwrap();

    // test watch
    kv.create("foo", b"lorem").unwrap();
    let entry = watch.next().unwrap();
    assert_eq!(entry.key, "foo".to_string());
    assert_eq!(entry.value, b"lorem");
    assert_eq!(entry.revision, 1);

    kv.put("foo", b"ipsum").unwrap();
    let entry = watch.next().unwrap();
    assert_eq!(entry.key, "foo".to_string());
    assert_eq!(entry.value, b"ipsum");
    assert_eq!(entry.revision, 2);

    kv.delete("foo").unwrap();
    let entry = watch.next().unwrap();
    assert_eq!(entry.operation, Operation::Delete);

    drop(watch);
}

#[test]
fn key_value_bind() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    context
        .create_key_value(&Config {
            bucket: "WATCH".to_string(),
            ..Default::default()
        })
        .unwrap();

    // Now bind to it..
    context.key_value("WATCH").unwrap();

    // Make sure we can't bind to a non-kv style stream.
    // We have some protection with stream name prefix.
    context
        .add_stream(&StreamConfig {
            name: "KV_TEST".to_string(),
            subjects: vec!["foo".to_string()],
            ..Default::default()
        })
        .unwrap();

    context.key_value("TEST").unwrap_err();
}

#[test]
fn key_value_delete() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    context
        .create_key_value(&Config {
            bucket: "TEST".to_string(),
            ..Default::default()
        })
        .unwrap();

    context.key_value("TEST").unwrap();

    context.delete_key_value("TEST").unwrap();
    context.key_value("TEST").unwrap_err();
}

#[test]
fn key_value_purge() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let bucket = context
        .create_key_value(&Config {
            bucket: "FOO".to_string(),
            history: 10,
            ..Default::default()
        })
        .unwrap();

    bucket.put("foo", "1").unwrap();
    bucket.put("foo", "2").unwrap();
    bucket.put("foo", "3").unwrap();

    bucket.put("bar", "1").unwrap();
    bucket.put("baz", "2").unwrap();
    bucket.put("baz", "3").unwrap();

    let entries = bucket.history("foo").unwrap();
    assert_eq!(entries.into_iter().count(), 3);

    bucket.purge("foo").unwrap();

    let value = bucket.get("foo").unwrap();
    assert_eq!(value, None);

    let entries = bucket.history("foo").unwrap();
    assert_eq!(entries.into_iter().count(), 1);
}

#[test]
fn key_value_keys() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let kv = context
        .create_key_value(&Config {
            bucket: "KVS".to_string(),
            history: 2,
            ..Default::default()
        })
        .unwrap();

    kv.put("foo", b"").unwrap();
    kv.put("bar", b"").unwrap();
    kv.put("baz", b"").unwrap();

    let mut keys: Vec<String> = Vec::new();
    for key in kv.keys().unwrap() {
        keys.push(key);
    }

    assert!(keys.iter().any(|s| s == "foo"));
    assert!(keys.iter().any(|s| s == "bar"));
    assert!(keys.iter().any(|s| s == "baz"));
    assert_eq!(keys.len(), 3);

    kv.delete("foo").unwrap();
    kv.purge("bar").unwrap();

    let mut keys: Vec<String> = Vec::new();
    for key in kv.keys().unwrap() {
        keys.push(key);
    }

    assert!(keys.iter().any(|s| s == "baz"));
    assert_eq!(keys.len(), 1);
}

#[test]
// Sanity check that everything works if you are using a JS domain across leaf nodes. This does not
// attempt to exercise all logic, just the happy path, as the other logic is exercised in the other
// tests
#[ignore]
fn key_value_domain() {
    let _server = nats_server::run_server("tests/configs/jetstream-domain.conf");
    let leaf_server = nats_server::run_server("tests/configs/jetstream-domain-leaf.conf");
    let client = nats::connect(&leaf_server.client_url()).unwrap();
    let opts = nats::jetstream::JetStreamOptions::new().domain("foobar");
    let context = nats::jetstream::JetStream::new(client, opts);

    let kv = context
        .create_key_value(&Config {
            bucket: "KVS".to_string(),
            history: 2,
            ..Default::default()
        })
        .unwrap();

    let revision = kv.put("foo", b"bar").expect("should be able put a value");
    let val = kv.get("foo").expect("should be able to get key").unwrap();
    assert_eq!(val, b"bar", "should have received the correct value");

    kv.update("foo", b"baz", revision)
        .expect("should be able to update");
    let val = kv.get("foo").expect("should be able to get key").unwrap();
    assert_eq!(
        val, b"baz",
        "should have received the correct value after update"
    );

    // Try a delete too for good measure
    kv.delete("bar").expect("should be able to delete");
}
