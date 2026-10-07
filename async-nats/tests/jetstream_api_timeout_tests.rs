// Copyright 2020-2023 The NATS Authors
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

//! A JetStream API request is bounded by the context timeout when one was set, otherwise by the
//! connection's request timeout. Ordered consumer recreation follows the same rule, with a
//! 10 second floor when neither is set. Tests put a proxy between the context and the server
//! that forwards, delays or swallows requests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::client::traits::Requester;
use async_nats::jetstream::{
    self,
    consumer::{pull, push},
    context::{AccountErrorKind, RequestErrorKind},
    stream,
};
use futures::StreamExt;

const PROXY_PREFIX: &str = "$JS.PROXY.API";

/// Stands between a JetStream context and the server. Requests on `$JS.PROXY.API.>` are
/// forwarded to `$JS.API.>` after `delay`, or never answered while `swallow` is set.
struct Proxy {
    /// While set, no request is answered.
    swallow: Arc<AtomicBool>,
    /// One-shot: the next consumer create is not answered.
    swallow_next_create: Arc<AtomicBool>,
}

async fn proxy(client: async_nats::Client, delay: Duration) -> Proxy {
    let swallow = Arc::new(AtomicBool::new(false));
    let swallow_next_create = Arc::new(AtomicBool::new(false));
    let mut requests = client.subscribe(format!("{PROXY_PREFIX}.>")).await.unwrap();
    client.flush().await.unwrap();
    tokio::spawn({
        let swallow = swallow.clone();
        let swallow_next_create = swallow_next_create.clone();
        async move {
            while let Some(request) = requests.next().await {
                if swallow.load(Ordering::SeqCst) {
                    continue;
                }
                let subject = request.subject.replacen(PROXY_PREFIX, "$JS.API", 1);
                if subject.starts_with("$JS.API.CONSUMER.CREATE.")
                    && swallow_next_create.swap(false, Ordering::SeqCst)
                {
                    continue;
                }
                let Some(reply) = request.reply else {
                    continue;
                };
                let client = client.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    client
                        .publish_with_reply(subject, reply, request.payload)
                        .await
                        .ok();
                });
            }
        }
    });
    Proxy {
        swallow,
        swallow_next_create,
    }
}

/// A connection with the request timeout disabled, so only a context timeout can end a request.
async fn connect_without_request_timeout(server: &nats_server::Server) -> async_nats::Client {
    async_nats::ConnectOptions::new()
        .request_timeout(None)
        .connect(server.client_url())
        .await
        .unwrap()
}

async fn setup() -> (nats_server::Server, async_nats::Client, jetstream::Context) {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = connect_without_request_timeout(&server).await;
    let context = jetstream::with_prefix(client.clone(), PROXY_PREFIX);
    (server, client, context)
}

/// Creates the `events` stream with one message, bypassing the proxy.
async fn seed_events_stream(server: &nats_server::Server) {
    let admin = jetstream::new(async_nats::connect(server.client_url()).await.unwrap());
    admin
        .create_stream(stream::Config {
            name: "events".to_string(),
            subjects: vec!["events".to_string()],
            allow_direct: true,
            ..Default::default()
        })
        .await
        .unwrap();
    admin
        .publish("events", "data".into())
        .await
        .unwrap()
        .await
        .unwrap();
}

/// Sends one request over each path that reads a JetStream API reply: `Context::request`,
/// direct get, and the raw `Requester` impl. Panics if any of them fails.
async fn every_request_path_succeeds(context: jetstream::Context) {
    // Does not send a request.
    let stream = context.get_stream_no_info("events").await.unwrap();
    let (info, message, raw) = tokio::join!(
        context.request::<_, _, serde_json::Value>("STREAM.INFO.events", &()),
        stream.direct_get(1),
        Requester::send_request(
            &context,
            format!("{PROXY_PREFIX}.STREAM.INFO.events"),
            async_nats::Request::new(),
        ),
    );
    info.unwrap();
    message.unwrap();
    raw.unwrap();
}

/// Awaits `op`, panicking with the call site when it does not return within `limit`. The
/// explicit `IntoFuture` call keeps builders like `Purge` working on the oldest supported tokio.
macro_rules! within {
    ($limit:expr, $op:expr) => {{
        let limit = $limit;
        tokio::time::timeout(limit, std::future::IntoFuture::into_future($op))
            .await
            .unwrap_or_else(|_| panic!("`{}` did not return within {:?}", stringify!($op), limit))
    }};
}

/// Awaits `op` and asserts it failed no earlier than `lower` and within `limit`.
macro_rules! fails_between {
    ($lower:expr, $limit:expr, $op:expr) => {{
        let start = std::time::Instant::now();
        let result = within!($limit, $op);
        let elapsed = start.elapsed();
        assert!(
            result.is_err(),
            "`{}` unexpectedly succeeded",
            stringify!($op)
        );
        assert!(
            elapsed >= $lower,
            "`{}` failed after {:?}, before the {:?} bound",
            stringify!($op),
            elapsed,
            $lower
        );
    }};
}

// A set context timeout bounds requests when the connection's request timeout is disabled.
#[tokio::test]
async fn set_context_timeout_bounds_requests_without_request_timeout() {
    let (_server, client, mut context) = setup().await;
    let proxy = proxy(client, Duration::ZERO).await;
    context.set_timeout(Duration::from_millis(300));
    proxy.swallow.store(true, Ordering::SeqCst);

    let start = Instant::now();
    let err = within!(
        Duration::from_secs(2),
        context.request::<_, _, serde_json::Value>("STREAM.INFO.events", &())
    )
    .unwrap_err();
    assert_eq!(err.kind(), RequestErrorKind::TimedOut);
    assert!(start.elapsed() >= Duration::from_millis(300));
}

// A set context timeout is the bound, also when the connection's request timeout is lower.
#[tokio::test]
async fn set_context_timeout_wins_over_lower_request_timeout() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    seed_events_stream(&server).await;
    let client = async_nats::ConnectOptions::new()
        .request_timeout(Some(Duration::from_millis(200)))
        .connect(server.client_url())
        .await
        .unwrap();
    let _proxy = proxy(client.clone(), Duration::from_millis(400)).await;
    let context = jetstream::ContextBuilder::new()
        .api_prefix(PROXY_PREFIX)
        .timeout(Duration::from_secs(3))
        .build(client);

    within!(Duration::from_secs(4), every_request_path_succeeds(context));
}

// A set context timeout is the bound, also when the connection's request timeout is higher,
// whether set through the builder or `set_timeout`.
#[tokio::test]
async fn set_context_timeout_wins_over_higher_request_timeout() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();
    let proxy = proxy(client.clone(), Duration::ZERO).await;
    proxy.swallow.store(true, Ordering::SeqCst);

    let built = jetstream::ContextBuilder::new()
        .api_prefix(PROXY_PREFIX)
        .timeout(Duration::from_millis(200))
        .build(client.clone());
    let mut set = jetstream::with_prefix(client, PROXY_PREFIX);
    set.set_timeout(Duration::from_millis(200));

    for context in [built, set] {
        let err = within!(
            Duration::from_secs(2),
            context.request::<_, _, serde_json::Value>("STREAM.INFO.events", &())
        )
        .unwrap_err();
        assert_eq!(err.kind(), RequestErrorKind::TimedOut);
    }
}

// Without a context timeout, the connection's request timeout ends every request path.
#[tokio::test]
async fn unset_context_timeout_times_out_at_request_timeout() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::ConnectOptions::new()
        .request_timeout(Some(Duration::from_millis(300)))
        .connect(server.client_url())
        .await
        .unwrap();
    let proxy = proxy(client.clone(), Duration::ZERO).await;
    proxy.swallow.store(true, Ordering::SeqCst);
    let context = jetstream::with_prefix(client, PROXY_PREFIX);
    let stream = context.get_stream_no_info("events").await.unwrap();
    let lower = Duration::from_millis(300);
    let limit = Duration::from_secs(2);

    fails_between!(
        lower,
        limit,
        context.request::<_, _, serde_json::Value>("STREAM.INFO.events", &())
    );
    fails_between!(lower, limit, stream.direct_get(1));
    fails_between!(
        lower,
        limit,
        Requester::send_request(
            &context,
            format!("{PROXY_PREFIX}.STREAM.INFO.events"),
            async_nats::Request::new(),
        )
    );
}

// Without a context timeout, requests wait as long as the connection's request timeout allows,
// default or raised: a reply 6 seconds late still arrives.
#[tokio::test]
async fn unset_context_timeout_follows_request_timeout() {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    seed_events_stream(&server).await;
    let _proxy = proxy(
        async_nats::connect(server.client_url()).await.unwrap(),
        Duration::from_secs(6),
    )
    .await;

    let default = async_nats::connect(server.client_url()).await.unwrap();
    let raised = async_nats::ConnectOptions::new()
        .request_timeout(Some(Duration::from_secs(30)))
        .connect(server.client_url())
        .await
        .unwrap();

    within!(Duration::from_secs(9), async {
        tokio::join!(
            every_request_path_succeeds(jetstream::with_prefix(default, PROXY_PREFIX)),
            every_request_path_succeeds(jetstream::with_prefix(raised, PROXY_PREFIX)),
        )
    });
}

// Without a context timeout, disabling the connection's request timeout leaves requests
// unbounded: a reply 6 seconds late still arrives.
#[tokio::test]
async fn unset_context_timeout_leaves_requests_unbounded_without_request_timeout() {
    let (server, client, context) = setup().await;
    seed_events_stream(&server).await;
    let _proxy = proxy(client, Duration::from_secs(6)).await;

    within!(Duration::from_secs(9), every_request_path_succeeds(context));
}

// `set_timeout` changes the bound of an existing context.
#[tokio::test]
async fn set_timeout_changes_the_bound_of_an_existing_context() {
    let (_server, client, mut context) = setup().await;
    let _proxy = proxy(client, Duration::from_millis(400)).await;

    context.set_timeout(Duration::from_secs(2));
    within!(Duration::from_secs(3), context.create_stream("events")).unwrap();

    context.set_timeout(Duration::from_millis(200));
    let err = within!(Duration::from_secs(2), context.query_account()).unwrap_err();
    assert_eq!(err.kind(), AccountErrorKind::TimedOut);
}

// The raw `Requester` impl, used by extension crates, applies the context timeout only to
// requests that do not set their own. An explicit timeout is the caller's choice, also when it
// is longer than the context timeout.
#[tokio::test]
async fn raw_requester_honors_an_explicit_request_timeout() {
    let (_server, client, mut context) = setup().await;
    let _proxy = proxy(client, Duration::from_millis(400)).await;
    context.set_timeout(Duration::from_millis(200));
    let subject = format!("{PROXY_PREFIX}.STREAM.INFO.events");

    let err = within!(
        Duration::from_secs(2),
        Requester::send_request(&context, subject.clone(), async_nats::Request::new())
    )
    .unwrap_err();
    assert_eq!(err.kind(), async_nats::RequestErrorKind::TimedOut);

    within!(
        Duration::from_secs(4),
        Requester::send_request(
            &context,
            subject,
            async_nats::Request::new().timeout(Some(Duration::from_secs(3))),
        )
    )
    .unwrap();
}

// An explicit `Request::timeout(None)` on the raw `Requester` impl removes the bound entirely,
// as documented on `Request::timeout`, also when a context timeout is set.
#[tokio::test]
async fn raw_requester_explicit_none_removes_the_bound() {
    let (_server, client, mut context) = setup().await;
    let _proxy = proxy(client, Duration::from_millis(400)).await;
    context.set_timeout(Duration::from_millis(200));

    within!(
        Duration::from_secs(2),
        Requester::send_request(
            &context,
            format!("{PROXY_PREFIX}.STREAM.INFO.events"),
            async_nats::Request::new().timeout(None),
        )
    )
    .unwrap();
}

// A sample of every API family is bounded by the context timeout, whichever error type the
// API maps it to. Handles are obtained while the proxy forwards, then the proxy stops
// answering. The context timeout is set before the handles are created, since each handle
// keeps its own copy of the context.
#[tokio::test]
async fn every_api_call_is_bounded() {
    let (_server, client, mut context) = setup().await;
    let proxy = proxy(client, Duration::ZERO).await;
    let timeout = Duration::from_millis(200);
    context.set_timeout(timeout);

    let mut stream = context
        .create_stream(stream::Config {
            name: "events".to_string(),
            subjects: vec!["events".to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    context
        .publish("events", "data".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let mut consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("durable".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
    #[cfg(feature = "kv")]
    let kv = context
        .create_key_value(jetstream::kv::Config {
            bucket: "bucket".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    proxy.swallow.store(true, Ordering::SeqCst);
    let limit = Duration::from_secs(2);

    // Context.
    fails_between!(timeout, limit, context.query_account());
    fails_between!(timeout, limit, context.create_stream("other"));
    fails_between!(timeout, limit, context.get_stream("events"));
    fails_between!(
        timeout,
        limit,
        context.update_stream(stream::Config {
            name: "events".to_string(),
            subjects: vec!["events".to_string(), "more".to_string()],
            ..Default::default()
        })
    );
    fails_between!(timeout, limit, context.delete_stream("events"));
    fails_between!(
        timeout,
        limit,
        context.get_consumer_from_stream::<pull::Config, _, _>("durable", "events")
    );
    fails_between!(
        timeout,
        limit,
        context.delete_consumer_from_stream("durable", "events")
    );
    fails_between!(timeout, limit, async {
        context.stream_names().next().await.unwrap()
    });
    fails_between!(timeout, limit, async {
        context.streams().next().await.unwrap()
    });
    #[cfg(feature = "kv")]
    {
        fails_between!(timeout, limit, context.get_key_value("bucket"));
        fails_between!(
            timeout,
            limit,
            context.create_key_value(jetstream::kv::Config {
                bucket: "other".to_string(),
                ..Default::default()
            })
        );
    }

    // Stream handle.
    fails_between!(timeout, limit, stream.info());
    fails_between!(
        timeout,
        limit,
        stream.get_consumer::<pull::Config>("durable")
    );
    fails_between!(
        timeout,
        limit,
        stream.create_consumer(pull::Config::default())
    );
    fails_between!(timeout, limit, stream.delete_consumer("durable"));
    fails_between!(timeout, limit, stream.purge());
    fails_between!(timeout, limit, stream.get_raw_message(1));
    fails_between!(timeout, limit, stream.direct_get(1));
    fails_between!(timeout, limit, stream.delete_message(1));
    fails_between!(timeout, limit, async {
        stream.consumer_names().next().await.unwrap()
    });

    // Consumer handle.
    fails_between!(timeout, limit, consumer.info());

    // Key-value reads go through direct get when the bucket allows it (the default).
    #[cfg(feature = "kv")]
    fails_between!(timeout, limit, kv.get("key"));
}

/// Starts a pull ordered consumer on `events` with one message consumed, then deletes its
/// consumer server-side after arming the proxy to swallow the next consumer create. Returns
/// the message stream and the moment the deletion was issued; the next message, `second`,
/// arrives once the recreate has retried the lost create.
async fn pull_ordered_consumer_with_lost_recreate(
    server: &nats_server::Server,
    context: &jetstream::Context,
    proxy: &Proxy,
) -> (pull::Ordered, Instant) {
    let stream = context
        .create_stream(stream::Config {
            name: "events".to_string(),
            subjects: vec!["events".to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    context
        .publish("events", "first".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let consumer = stream
        .create_consumer(pull::OrderedConfig::default())
        .await
        .unwrap();
    let name = consumer.cached_info().name.clone();
    let mut messages = consumer.messages().await.unwrap();
    let first = messages.next().await.unwrap().unwrap();
    assert_eq!(first.payload.as_ref(), b"first");

    proxy.swallow_next_create.store(true, Ordering::SeqCst);
    let admin = jetstream::new(async_nats::connect(server.client_url()).await.unwrap());
    let deleted_at = Instant::now();
    admin
        .delete_consumer_from_stream(&name, "events")
        .await
        .unwrap();
    admin
        .publish("events", "second".into())
        .await
        .unwrap()
        .await
        .unwrap();
    (messages, deleted_at)
}

// With the connection request timeout disabled, an ordered consumer recreate whose consumer
// create is lost retries it after the context timeout: one lost create costs one context
// timeout plus the first retry backoff of 500ms.
#[tokio::test]
async fn ordered_recreate_retries_a_lost_create_after_the_context_timeout() {
    let (server, client, mut context) = setup().await;
    let proxy = proxy(client, Duration::ZERO).await;
    context.set_timeout(Duration::from_secs(1));
    let (mut messages, deleted_at) =
        pull_ordered_consumer_with_lost_recreate(&server, &context, &proxy).await;

    let second = within!(Duration::from_secs(4), messages.next())
        .unwrap()
        .unwrap();
    assert_eq!(second.payload.as_ref(), b"second");
    assert!(deleted_at.elapsed() >= Duration::from_secs(1));
}

// The recreate waits for the full context timeout, also above 5 seconds: nothing caps it.
#[tokio::test]
async fn ordered_recreate_honors_a_context_timeout_above_five_seconds() {
    let (server, client, mut context) = setup().await;
    let proxy = proxy(client, Duration::ZERO).await;
    context.set_timeout(Duration::from_secs(7));
    let (mut messages, deleted_at) =
        pull_ordered_consumer_with_lost_recreate(&server, &context, &proxy).await;

    let second = within!(Duration::from_secs(12), messages.next())
        .unwrap()
        .unwrap();
    assert_eq!(second.payload.as_ref(), b"second");
    assert!(
        deleted_at.elapsed() >= Duration::from_secs(7),
        "recreate retried after {:?}, before the context timeout",
        deleted_at.elapsed()
    );
}

// With neither a context timeout nor a connection request timeout, nothing the user can set
// bounds the recreate, so it falls back to 10 seconds instead of waiting forever for a lost
// create.
#[tokio::test]
async fn ordered_recreate_falls_back_to_ten_seconds_without_any_timeout() {
    let (server, client, context) = setup().await;
    let proxy = proxy(client, Duration::ZERO).await;
    let (mut messages, deleted_at) =
        pull_ordered_consumer_with_lost_recreate(&server, &context, &proxy).await;

    let second = within!(Duration::from_secs(15), messages.next())
        .unwrap()
        .unwrap();
    assert_eq!(second.payload.as_ref(), b"second");
    assert!(
        deleted_at.elapsed() >= Duration::from_secs(10),
        "recreate retried after {:?}, before the 10 second floor",
        deleted_at.elapsed()
    );
}

// The push ordered consumer's recreate waits for the full context timeout too, also above 5
// seconds. A second consumer delivering to the same subject redelivers `first` with consumer
// sequence 1, a gap the ordered consumer recreates on at once; a server-side delete would
// only be noticed after two missed 5 second heartbeats.
#[tokio::test]
async fn push_ordered_recreate_honors_a_context_timeout_above_five_seconds() {
    let (server, client, mut context) = setup().await;
    let proxy = proxy(client.clone(), Duration::ZERO).await;
    context.set_timeout(Duration::from_secs(7));
    let stream = context
        .create_stream(stream::Config {
            name: "events".to_string(),
            subjects: vec!["events".to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    context
        .publish("events", "first".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let deliver_subject = client.new_inbox();
    let consumer = stream
        .create_consumer(push::OrderedConfig {
            deliver_subject: deliver_subject.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut messages = consumer.messages().await.unwrap();
    let first = messages.next().await.unwrap().unwrap();
    assert_eq!(first.payload.as_ref(), b"first");

    proxy.swallow_next_create.store(true, Ordering::SeqCst);
    let admin = jetstream::new(async_nats::connect(server.client_url()).await.unwrap());
    let gap_at = Instant::now();
    admin
        .create_consumer_on_stream(
            push::Config {
                deliver_subject,
                ..Default::default()
            },
            "events",
        )
        .await
        .unwrap();
    // Let the ordered consumer see the gap before `second` is published.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), messages.next())
            .await
            .is_err()
    );
    admin
        .publish("events", "second".into())
        .await
        .unwrap()
        .await
        .unwrap();

    let second = within!(Duration::from_secs(12), messages.next())
        .unwrap()
        .unwrap();
    assert_eq!(second.payload.as_ref(), b"second");
    assert!(
        gap_at.elapsed() >= Duration::from_secs(7),
        "recreate retried after {:?}, before the context timeout",
        gap_at.elapsed()
    );
}
