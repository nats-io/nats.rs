use std::time::Duration;

use async_nats::jetstream::{self, consumer::pull, stream};
use futures::StreamExt;

// Most tests use tests/configs/jetstream_max_request_batch.conf, which sets
// jetstream.limits.max_request_batch to 100. The server copies that into every
// pull consumer created with max_batch 0, and rejects larger pulls with a bare
// 409 "Exceeded MaxRequestBatch of 100" (no Nats-Pending-* headers).

const LIMITED: &str = "tests/configs/jetstream_max_request_batch.conf";
const UNLIMITED: &str = "tests/configs/jetstream.conf";

async fn setup(config: &str) -> (nats_server::Server, async_nats::Subscriber, stream::Stream) {
    let server = nats_server::run_server(config);
    let client = async_nats::connect(server.client_url()).await.unwrap();
    let js = jetstream::new(client.clone());
    let stream = js
        .create_stream(stream::Config {
            name: "events".into(),
            subjects: vec!["events".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    js.publish("events", "data".into())
        .await
        .unwrap()
        .await
        .unwrap();
    // Sees every pull request sent for the stream.
    let pulls = client
        .subscribe("$JS.API.CONSUMER.MSG.NEXT.events.>")
        .await
        .unwrap();
    client.flush().await.unwrap();
    (server, pulls, stream)
}

async fn count_for(sub: &mut async_nats::Subscriber, window: Duration) -> usize {
    let mut count = 0;
    let deadline = tokio::time::sleep(window);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            Some(_) = sub.next() => count += 1,
        }
    }
    count
}

/// The body of the next pull request on the wire.
async fn next_pull(sub: &mut async_nats::Subscriber) -> serde_json::Value {
    let request = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("no pull request sent")
        .unwrap();
    serde_json::from_slice(&request.payload).unwrap()
}

async fn first_message<E: std::fmt::Debug>(
    messages: &mut (impl futures::Stream<Item = Result<jetstream::Message, E>> + Unpin),
) -> jetstream::Message {
    tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await
        .expect("timed out waiting for a message")
        .expect("stream ended")
        .expect("expected a message, not an error")
}

async fn first_error<E>(
    messages: &mut (impl futures::Stream<Item = Result<jetstream::Message, E>> + Unpin),
) -> E {
    tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await
        .expect("timed out waiting for the rejection")
        .expect("stream ended without an error")
        .expect_err("expected an error, not a message")
}

fn nanos(value: &serde_json::Value) -> Duration {
    Duration::from_nanos(value.as_u64().unwrap())
}

// Plain pull messages() defaults to 200 per pull, over the server limit of 100.
// The default is capped by the consumer's max_batch, so the message is delivered
// with a single pull instead of re-pulling in a tight loop.
#[tokio::test]
async fn pull_messages_caps_default_batch_at_consumer_limit() {
    let (_server, mut pulls, stream) = setup(LIMITED).await;
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("plain".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(consumer.cached_info().config.max_batch, 100);
    let mut messages = consumer.messages().await.unwrap();

    let message = first_message(&mut messages).await;
    assert_eq!(message.payload.as_ref(), b"data");

    let pull = next_pull(&mut pulls).await;
    assert_eq!(pull["batch"], 100);
    assert_eq!(nanos(&pull["expires"]), Duration::from_secs(30));
    assert_eq!(nanos(&pull["idle_heartbeat"]), Duration::from_secs(15));
    assert_eq!(count_for(&mut pulls, Duration::from_secs(1)).await, 0);
}

// A limit above the default leaves the default alone.
#[tokio::test]
async fn pull_stream_keeps_defaults_below_consumer_limits() {
    let (_server, mut pulls, stream) = setup(UNLIMITED).await;
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("roomy".into()),
            max_batch: 300,
            max_expires: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut messages = consumer.stream().messages().await.unwrap();

    first_message(&mut messages).await;
    let pull = next_pull(&mut pulls).await;
    assert_eq!(pull["batch"], 200);
    assert_eq!(nanos(&pull["expires"]), Duration::from_secs(30));
    assert!(
        pull.get("idle_heartbeat").is_none(),
        "stream() has no default heartbeat"
    );
}

// A short max_expires caps the default expiry, and a user-set heartbeat is
// lowered with it; otherwise the server rejects the pull with 400 "heartbeat
// value too large".
#[tokio::test]
async fn pull_stream_caps_default_expires_and_lowers_heartbeat() {
    let (_server, mut pulls, stream) = setup(UNLIMITED).await;
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("short".into()),
            max_expires: Duration::from_secs(5),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut messages = consumer
        .stream()
        .heartbeat(Duration::from_secs(10))
        .messages()
        .await
        .unwrap();

    first_message(&mut messages).await;
    let pull = next_pull(&mut pulls).await;
    assert_eq!(pull["batch"], 200);
    assert_eq!(nanos(&pull["expires"]), Duration::from_secs(5));
    assert_eq!(nanos(&pull["idle_heartbeat"]), Duration::from_millis(2500));
}

// Values the user set explicitly are sent as given. If they exceed the
// consumer's limits the server rejects the pull once, the error carries the
// server's description, and the stream ends instead of re-pulling.
#[tokio::test]
async fn pull_stream_explicit_batch_over_limit_ends_with_error() {
    let (_server, mut pulls, stream) = setup(LIMITED).await;
    let mut messages = stream
        .create_consumer(pull::Config {
            durable_name: Some("limited".into()),
            max_batch: 10,
            ..Default::default()
        })
        .await
        .unwrap()
        .stream()
        .max_messages_per_batch(50)
        .messages()
        .await
        .unwrap();

    let err = first_error(&mut messages).await;
    assert_eq!(err.kind(), pull::MessagesErrorKind::RequestLimitExceeded);
    assert!(
        err.to_string().contains("Exceeded MaxRequestBatch of 10"),
        "unexpected error: {err}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), messages.next())
            .await
            .expect("iterator should have terminated")
            .is_none()
    );
    assert_eq!(count_for(&mut pulls, Duration::from_millis(500)).await, 1);
}

#[tokio::test]
async fn pull_stream_explicit_expires_over_limit_ends_with_error() {
    let (_server, mut pulls, stream) = setup(UNLIMITED).await;
    let mut messages = stream
        .create_consumer(pull::Config {
            durable_name: Some("short".into()),
            max_expires: Duration::from_secs(5),
            ..Default::default()
        })
        .await
        .unwrap()
        .stream()
        .expires(Duration::from_secs(20))
        .heartbeat(Duration::from_secs(10))
        .messages()
        .await
        .unwrap();

    let err = first_error(&mut messages).await;
    assert_eq!(err.kind(), pull::MessagesErrorKind::RequestLimitExceeded);
    assert!(
        err.to_string().contains("Exceeded MaxRequestExpires of 5s"),
        "unexpected error: {err}"
    );
    // Both values were explicit, so neither was touched.
    let pull = next_pull(&mut pulls).await;
    assert_eq!(nanos(&pull["expires"]), Duration::from_secs(20));
    assert_eq!(nanos(&pull["idle_heartbeat"]), Duration::from_secs(10));
}

// Ordered pull asks for 500 per pull, over the server limit of 100. The batch
// is capped to the limit, so the message is delivered with a single pull.
#[tokio::test]
async fn ordered_pull_caps_batch_at_consumer_limit() {
    let (_server, mut pulls, stream) = setup(LIMITED).await;
    let mut messages = stream
        .create_consumer(pull::OrderedConfig::default())
        .await
        .unwrap()
        .messages()
        .await
        .unwrap();

    let message = first_message(&mut messages).await;
    assert_eq!(message.payload.as_ref(), b"data");
    let pull = next_pull(&mut pulls).await;
    assert_eq!(pull["batch"], 100);
    assert_eq!(count_for(&mut pulls, Duration::from_secs(1)).await, 0);
}

// OrderedConfig limits reach the server, and the pull is capped to them. A 5s
// max_expires also forces the idle heartbeat below the hardcoded 15s.
#[tokio::test]
async fn ordered_config_limits_are_sent_to_server_and_respected() {
    let (_server, mut pulls, stream) = setup(LIMITED).await;
    let consumer = stream
        .create_consumer(pull::OrderedConfig {
            max_batch: 7,
            max_expires: Duration::from_secs(5),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(consumer.cached_info().config.max_batch, 7);
    assert_eq!(
        consumer.cached_info().config.max_expires,
        Duration::from_secs(5)
    );
    let mut messages = consumer.messages().await.unwrap();
    let message = first_message(&mut messages).await;
    assert_eq!(message.payload.as_ref(), b"data");
    let pull = next_pull(&mut pulls).await;
    assert_eq!(pull["batch"], 7);
    assert_eq!(nanos(&pull["expires"]), Duration::from_secs(5));
    assert_eq!(nanos(&pull["idle_heartbeat"]), Duration::from_millis(2500));
}

// An ordered consumer bound to a pre-existing consumer with a small max_expires
// (created by another client or the CLI) caps its pull expiry too.
#[tokio::test]
async fn ordered_pull_caps_expires_of_existing_consumer() {
    let (_server, mut pulls, stream) = setup(LIMITED).await;
    stream
        .create_consumer(pull::Config {
            name: Some("existing".into()),
            max_expires: Duration::from_secs(5),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut messages = stream
        .get_consumer::<pull::OrderedConfig>("existing")
        .await
        .unwrap()
        .messages()
        .await
        .unwrap();
    let message = first_message(&mut messages).await;
    assert_eq!(message.payload.as_ref(), b"data");
    let pull = next_pull(&mut pulls).await;
    assert_eq!(pull["batch"], 100);
    assert_eq!(nanos(&pull["expires"]), Duration::from_secs(5));
    assert_eq!(nanos(&pull["idle_heartbeat"]), Duration::from_millis(2500));
}

// The recreated consumer caps its batch as well, so an ordered consumer keeps
// delivering on a server with max_request_batch after a recreate.
#[tokio::test]
async fn ordered_pull_recreate_caps_batch() {
    let (server, mut pulls, stream) = setup(LIMITED).await;
    let js = jetstream::new(async_nats::connect(server.client_url()).await.unwrap());
    let consumer = stream
        .create_consumer(pull::OrderedConfig::default())
        .await
        .unwrap();
    let name = consumer.cached_info().name.clone();
    let mut messages = consumer.messages().await.unwrap();
    let first = first_message(&mut messages).await;
    assert_eq!(first.info().unwrap().stream_sequence, 1);

    // Force a recreate: the pending pull gets 409 Consumer Deleted.
    stream.delete_consumer(&name).await.unwrap();
    js.publish("events", "after".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(10), messages.next())
        .await
        .expect("no message after recreate")
        .unwrap()
        .unwrap();
    assert_eq!(second.payload.as_ref(), b"after");
    assert_eq!(second.info().unwrap().stream_sequence, 2);
    // Initial pull plus the recreated consumer's pull. No tight loop from either.
    assert!(
        count_for(&mut pulls, Duration::from_secs(1)).await <= 5,
        "too many pull requests"
    );
}
