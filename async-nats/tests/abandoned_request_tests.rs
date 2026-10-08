// Copyright 2020-2026 The NATS Authors
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

//! A request whose caller stops waiting for the reply, after a timeout or by dropping its
//! future, must not hold memory once the client has caught up.
//!
//! The tests count the bytes held on the heap with a counting global allocator, so they run one
//! at a time, and this binary holds no other tests.

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_nats::{Client, ConnectOptions, Request, RequestErrorKind};
use futures::StreamExt;

struct CountingAllocator;

static HEAP_BYTES: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            HEAP_BYTES.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        HEAP_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            HEAP_BYTES.fetch_add(
                new_size as isize - layout.size() as isize,
                Ordering::Relaxed,
            );
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const SUBJECT: &str = "service";
/// Requests abandoned to warm up the client, its buffers and the server before measuring.
const WARMUP: usize = 1_000;
/// Requests abandoned while measuring. Each one left in the multiplexer holds about 300 bytes,
/// so keeping all of them would grow the heap by about 6 MiB.
const ABANDONED: usize = 20_000;
const BATCH: usize = 500;
const WAIT: Duration = Duration::from_millis(10);
const MAX_GROWTH: isize = 1024 * 1024;
/// How long the client gets to free the abandoned requests.
const SETTLE: Duration = Duration::from_secs(5);
/// Timeout of requests abandoned all at once.
const BURST_WAIT: Duration = Duration::from_secs(1);
/// Ping interval of a client that must free abandoned requests on its timer within [`SETTLE`].
const PING_INTERVAL: Duration = Duration::from_millis(200);

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// Abandons [`ABANDONED`] requests to [`SUBJECT`], whose subscriber never replies, and checks
/// that the heap does not grow by more than [`MAX_GROWTH`] once the client caught up.
///
/// `abandon` sends `count` requests through the client and returns once their callers stopped
/// waiting. It first runs with [`WARMUP`] requests, then measured with [`ABANDONED`].
fn assert_abandoned_requests_are_freed<F, Fut>(options: ConnectOptions, abandon: F)
where
    F: Fn(Client, usize) -> Fut,
    Fut: Future<Output = ()>,
{
    let _one_at_a_time = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let server = nats_server::run_basic_server();
        let client = options.connect(server.client_url()).await.unwrap();

        // A responder that never replies, draining requests so they do not pile up.
        let received = Arc::new(AtomicUsize::new(0));
        let mut subscriber = client.subscribe(SUBJECT).await.unwrap();
        tokio::spawn({
            let received = received.clone();
            async move {
                while subscriber.next().await.is_some() {
                    received.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        client.flush().await.unwrap();

        let start = HEAP_BYTES.load(Ordering::Relaxed);
        abandon(client.clone(), WARMUP).await;
        wait_until("warm-up requests are received", || {
            received.load(Ordering::Relaxed) >= WARMUP
        })
        .await;
        settle(&client, start).await;
        let before = HEAP_BYTES.load(Ordering::Relaxed);

        abandon(client.clone(), ABANDONED).await;
        wait_until("abandoned requests are received", || {
            received.load(Ordering::Relaxed) >= WARMUP + ABANDONED
        })
        .await;
        let growth = settle(&client, before).await;
        println!("heap grew by {growth} bytes");
        assert!(
            growth < MAX_GROWTH,
            "heap grew by {growth} bytes, {} per abandoned request",
            growth / ABANDONED as isize
        );
    });
}

/// Gives the client up to [`SETTLE`] to free what it holds, and returns by how many bytes the
/// heap grew since `since`.
async fn settle(client: &Client, since: isize) -> isize {
    let deadline = Instant::now() + SETTLE;
    loop {
        client.flush().await.unwrap();
        let growth = HEAP_BYTES.load(Ordering::Relaxed) - since;
        if growth < MAX_GROWTH || Instant::now() > deadline {
            return growth;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Runs `count` copies of `abandon` in concurrent batches.
async fn in_batches<F, Fut>(count: usize, abandon: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    for start in (0..count).step_by(BATCH) {
        let batch = BATCH.min(count - start);
        futures::future::join_all((0..batch).map(|_| abandon())).await;
    }
}

#[test]
fn timed_out_requests_are_freed() {
    assert_abandoned_requests_are_freed(ConnectOptions::new(), |client, count| async move {
        in_batches(count, || async {
            let err = client
                .send_request(SUBJECT, Request::new().timeout(Some(WAIT)))
                .await
                .unwrap_err();
            assert_eq!(err.kind(), RequestErrorKind::TimedOut);
        })
        .await;
    });
}

#[test]
fn dropped_requests_are_freed() {
    assert_abandoned_requests_are_freed(ConnectOptions::new(), |client, count| async move {
        in_batches(count, || async {
            let request = client.request(SUBJECT, "data".into());
            tokio::time::timeout(WAIT, request).await.unwrap_err();
        })
        .await;
    });
}

#[cfg(feature = "jetstream")]
#[test]
fn timed_out_publish_acks_are_freed() {
    assert_abandoned_requests_are_freed(ConnectOptions::new(), |client, count| async move {
        let context = async_nats::jetstream::context::ContextBuilder::new()
            .timeout(WAIT)
            .build(client);
        in_batches(count, || async {
            let err = context
                .publish(SUBJECT, "data".into())
                .await
                .unwrap()
                .await
                .unwrap_err();
            assert_eq!(
                err.kind(),
                async_nats::jetstream::context::PublishErrorKind::TimedOut
            );
        })
        .await;
    });
}

#[cfg(feature = "jetstream")]
#[test]
fn dropped_publish_acks_are_freed() {
    assert_abandoned_requests_are_freed(ConnectOptions::new(), |client, count| async move {
        // Capping acks in flight lets the warm-up fill the acker's queue as far as the rest.
        let context = async_nats::jetstream::context::ContextBuilder::new()
            .ack_timeout(WAIT)
            .max_ack_inflight(BATCH)
            .backpressure_on_inflight(true)
            .build(client);
        in_batches(count, || async {
            drop(context.publish(SUBJECT, "data".into()).await.unwrap());
        })
        .await;
    });
}

/// Sends `count` requests at once and waits until all of them time out, so that the client holds
/// all of them at the same time, and no request follows that could prune them.
async fn abandon_at_once(client: &Client, count: usize) {
    let requests =
        (0..count).map(|_| client.send_request(SUBJECT, Request::new().timeout(Some(BURST_WAIT))));
    for result in futures::future::join_all(requests).await {
        assert_eq!(result.unwrap_err().kind(), RequestErrorKind::TimedOut);
    }
}

#[test]
fn abandoned_burst_is_freed_without_further_requests() {
    let options = ConnectOptions::new().ping_interval(PING_INTERVAL);
    assert_abandoned_requests_are_freed(options, |client, count| async move {
        abandon_at_once(&client, count).await;
    });
}

#[test]
fn abandoned_burst_is_freed_while_replies_arrive() {
    let options = ConnectOptions::new().ping_interval(PING_INTERVAL);
    let echoing = AtomicBool::new(false);
    assert_abandoned_requests_are_freed(options, |client, count| {
        let echoing = &echoing;
        async move {
            abandon_at_once(&client, count).await;

            // Requests answered one at a time never bring the client to its prune threshold,
            // and every reply resets its ping timer.
            if !echoing.swap(true, Ordering::Relaxed) {
                let mut echo = client.subscribe("echo").await.unwrap();
                tokio::spawn({
                    let client = client.clone();
                    async move {
                        while let Some(request) = echo.next().await {
                            let reply = request.reply.unwrap();
                            client.publish(reply, request.payload).await.unwrap();
                        }
                    }
                });
                tokio::spawn(
                    async move { while client.request("echo", "".into()).await.is_ok() {} },
                );
            }
        }
    });
}
