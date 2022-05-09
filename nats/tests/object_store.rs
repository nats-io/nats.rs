// Copyright 2021 The NATS Authors
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

use rand::prelude::*;
use std::io::Read;

#[test]
fn object_random() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let bucket = context
        .create_object_store(&nats::object_store::Config {
            bucket: "OBJECTS".to_string(),
            ..Default::default()
        })
        .unwrap();

    bucket.info("FOO").unwrap_err();

    let mut rng = rand::thread_rng();
    let mut bytes = vec![0; 1024 * 1024 + 22];
    rng.try_fill_bytes(&mut bytes).unwrap();

    bucket.put("FOO", &mut bytes.as_slice()).unwrap();

    let object_info = bucket.info("FOO").unwrap();
    assert!(!object_info.nuid.is_empty());
    assert_eq!(object_info.size, bytes.len());

    let mut result = Vec::new();
    bucket.get("FOO").unwrap().read_to_end(&mut result).unwrap();

    assert_eq!(result, bytes);

    let mut result = Vec::new();
    bucket.get("FOO").unwrap().read_to_end(&mut result).unwrap();

    assert_eq!(result, bytes);

    let mut bytes = vec![0; 1024 * 1024 + 22];
    rng.try_fill_bytes(&mut bytes).unwrap();
    bucket.put("FOO", &mut bytes.as_slice()).unwrap();

    let mut result = Vec::new();
    let mut object = bucket.get("FOO").unwrap();
    loop {
        let mut buffer = [0; 1];
        if let Ok(n) = object.read(&mut buffer) {
            if n == 0 {
                break;
            }

            result.extend_from_slice(&buffer[..n]);
        }
    }

    assert_eq!(result, bytes);

    let mut result = Vec::new();
    let mut object = bucket.get("FOO").unwrap();
    loop {
        let mut buffer = [0; 2];
        if let Ok(n) = object.read(&mut buffer) {
            if n == 0 {
                break;
            }

            result.extend_from_slice(&buffer[..n]);
        }
    }

    assert_eq!(result, bytes);

    let mut result = Vec::new();
    let mut object = bucket.get("FOO").unwrap();
    loop {
        let mut buffer = [0; 128];
        if let Ok(n) = object.read(&mut buffer) {
            if n == 0 {
                break;
            }

            result.extend_from_slice(&buffer[..n]);
        }
    }

    assert_eq!(result, bytes);
}

#[test]
fn object_sealed() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let bucket = context
        .create_object_store(&nats::object_store::Config {
            bucket: "OBJECTS".to_string(),
            ..Default::default()
        })
        .unwrap();

    bucket.seal().unwrap();

    let stream_info = context.stream_info("OBJ_OBJECTS").unwrap();
    assert!(stream_info.config.sealed);
}

#[test]
fn object_delete() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let bucket = context
        .create_object_store(&nats::object_store::Config {
            bucket: "OBJECTS".to_string(),
            ..Default::default()
        })
        .unwrap();

    let data = b"abc".repeat(100).to_vec();
    bucket.put("A", &mut data.as_slice()).unwrap();
    bucket.delete("A").unwrap();

    let stream_info = context.stream_info("OBJ_OBJECTS").unwrap();

    // We should have one message left. The delete marker.
    assert_eq!(stream_info.state.messages, 1);

    // Make sure we have a delete marker, this will be there to drive Watch functionality.
    let object_info = bucket.info("A").unwrap();
    assert!(object_info.deleted);
}

#[test]
fn object_multiple_delete() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let bucket = context
        .create_object_store(&nats::object_store::Config {
            bucket: "2OD".to_string(),
            ..Default::default()
        })
        .unwrap();

    let data = b"A".repeat(100).to_vec();
    bucket.put("A", &mut data.as_slice()).unwrap();

    // Hold onto this so we can make sure delete clears all messages, chunks and meta.
    let old_stream_info = context.stream_info("OBJ_2OD").unwrap();

    let data = b"B".repeat(200).to_vec();
    bucket.put("B", &mut data.as_slice()).unwrap();

    // Now delete B
    bucket.delete("B").unwrap();

    let new_stream_info = context.stream_info("OBJ_2OD").unwrap();
    assert_eq!(
        new_stream_info.state.messages,
        old_stream_info.state.messages + 1
    );
}

#[test]
fn object_names() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let bucket = context
        .create_object_store(&nats::object_store::Config {
            bucket: "NAMES".to_string(),
            ..Default::default()
        })
        .unwrap();

    let empty = Vec::new();

    // Test filename like naming.
    bucket.put("foo.bar", &mut empty.as_slice()).unwrap();

    // Spaces ok
    bucket.put("foo bar", &mut empty.as_slice()).unwrap();

    // Errors
    bucket.put("*", &mut empty.as_slice()).unwrap_err();
    bucket.put(">", &mut empty.as_slice()).unwrap_err();
    bucket.put("", &mut empty.as_slice()).unwrap_err();
}

#[test]
fn object_watch() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = nats::connect(&server.client_url()).unwrap();
    let context = nats::jetstream::new(client);

    let bucket = context
        .create_object_store(&nats::object_store::Config {
            bucket: "WATCH".to_string(),
            ..Default::default()
        })
        .unwrap();

    let mut watch = bucket.watch().unwrap();

    let bytes = vec![];
    bucket.put("foo", &mut bytes.as_slice()).unwrap();

    let info = watch.next().unwrap();
    assert_eq!(info.name, "foo");
    assert_eq!(info.size, bytes.len());

    let bytes = vec![1, 2, 3, 4, 5];
    bucket.put("bar", &mut bytes.as_slice()).unwrap();

    let info = watch.next().unwrap();
    assert_eq!(info.name, "bar");
    assert_eq!(info.size, bytes.len());
}
