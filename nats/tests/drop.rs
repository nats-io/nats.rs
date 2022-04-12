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

use nats::jetstream;
use smol::future::FutureExt;
use std::{
    io,
    time::{Duration, Instant},
};

mod util;
pub use util::*;

#[test]
fn drop_flushes() -> io::Result<()> {
    let s = util::run_basic_server();

    let nc1 = nats::connect(&s.client_url())?;
    let nc2 = nats::connect(&s.client_url())?;

    let inbox = nc1.new_inbox();
    let sub = nc2.subscribe(&inbox)?;
    nc2.flush()?;

    nc1.publish(&inbox, b"hello")?;
    drop(nc1); // Dropping should flush the published message.

    assert_eq!(sub.next().unwrap().data, b"hello");

    Ok(())
}

#[test]
fn two_connections() -> io::Result<()> {
    let s = util::run_basic_server();

    let nc1 = nats::connect(&s.client_url())?;
    let nc2 = nc1.clone();

    nc1.publish("foo", b"bar")?;
    nc2.publish("foo", b"bar")?;

    drop(nc1);
    nc2.publish("foo", b"bar")?;

    Ok(())
}

#[test]
fn async_subscription_drop() -> io::Result<()> {
    let s = util::run_basic_server();

    smol::block_on(async {
        let nc = nats::asynk::connect(s.client_url()).await?;

        let inbox = nc.new_inbox();

        // This makes sure the subscription is closed after being dropped. If it wasn't closed,
        // creating the 501st subscription would block forever due to the `blocking` crate's thread
        // pool being fully occupied.
        for _ in 0..600 {
            let sub = nc
                .subscribe(&inbox)
                .or(async {
                    smol::Timer::after(std::time::Duration::from_secs(2)).await;
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "unable to create subscription",
                    ))
                })
                .await?;
            sub.next()
                .or(async {
                    smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    None
                })
                .await;
        }

        Ok(())
    })
}

#[test]
fn shutdown_responsivness_regression_check() {
    let s = util::run_basic_server();
    let conn = nats::Options::new().connect(s.client_url()).unwrap();
    conn.rtt().unwrap();
    let sub = conn.subscribe("test").unwrap();
    conn.publish("test", b"msg").unwrap();
    sub.next().unwrap();

    let now = Instant::now();
    conn.close();
    assert!(now.elapsed().le(&Duration::from_secs(5)));
}

#[test]
fn drop_responsivness_regression_check() {
    let s = util::run_basic_server();
    let now;
    {
        let conn = nats::Options::new().connect(s.client_url()).unwrap();
        conn.rtt().unwrap();
        now = Instant::now();
    }
    assert!(now.elapsed().le(&Duration::from_secs(5)));
}

#[test]
fn close_responsiveness_regression_jetstream() {
    let (_s, nc, js) = run_basic_jetstream();

    js.add_stream(nats::jetstream::StreamConfig {
        name: "TEST".to_string(),
        subjects: vec!["subject".to_string()],
        ..Default::default()
    })
    .unwrap();

    for _ in 0..1000 {
        js.publish("subject", b"foo").unwrap();
    }
    let sub = js.subscribe("subject").expect("failed to subscribe");
    sub.with_process_handler(|_| {
        println!("message");
        Ok(())
    });

    nc.close();
}

#[test]
fn close_responsiveness_regression_jetstream_complex() {
    let (_s, conn, jetstream) = run_basic_jetstream();

    jetstream
        .add_stream(nats::jetstream::StreamConfig {
            name: "stream10".to_string(),
            subjects: vec!["subject11".to_string(), "subject12".to_string()],
            max_msgs_per_subject: 1,
            storage: nats::jetstream::StorageType::Memory,
            ..Default::default()
        })
        .unwrap();

    for _ in 0..99 {
        jetstream
            .publish("subject11", vec![1, 2, 3])
            .expect("failed to publish");
        jetstream
            .publish("subject12", vec![1, 2, 3])
            .expect("failed to publish");
    }

    std::thread::sleep(std::time::Duration::from_millis(1000));
    let sub = jetstream
        .subscribe("subject11")
        .expect("failed to subscribe");

    let (result_tx, result_rx): (
        crossbeam_channel::Sender<i32>,
        crossbeam_channel::Receiver<i32>,
    ) = crossbeam_channel::bounded(32);
    sub.clone().with_process_handler(move |msg| {
        result_tx
            .send(1)
            .expect("failed to report back over channel");
        Ok(())
    });

    let mut count = 0;
    loop {
        crossbeam_channel::select! {
            recv(result_rx) -> _msg => count += 1,
            default(std::time::Duration::from_millis(1000)) => {
                    assert_eq!(count, 1);
                    break
            }
        }
    }

    if let Err(e) = sub.unsubscribe() {
        panic!("failed to unsubscribe {:?}", e);
    }
    conn.close();
}
