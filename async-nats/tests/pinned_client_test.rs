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

#![cfg(feature = "server_2_11")]

use std::time::Duration;

use async_nats::jetstream::{
    self,
    consumer::{pull, AckPolicy, PriorityGroupState, PriorityPolicy, PullConsumer},
    stream::{self, ConsumerErrorKind},
    ErrorCode,
};
use futures::{StreamExt, TryStreamExt};

// `setup` creates a pinned client consumer named "pinned" with priority group "A" and a 3s
// priority_timeout. Pull streams expire their pulls after 500ms so a pinned stream refreshes its
// pin well within that timeout, even on a slow runner.

async fn setup() -> (nats_server::Server, jetstream::Context, stream::Stream) {
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();
    let js = jetstream::new(client);
    let stream = js
        .create_stream(stream::Config {
            name: "events".into(),
            subjects: vec!["events".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    stream
        .create_consumer(pull::Config {
            durable_name: Some("pinned".into()),
            ack_policy: AckPolicy::None,
            priority_policy: PriorityPolicy::PinnedClient,
            priority_groups: vec!["A".into()],
            priority_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .unwrap();
    (server, js, stream)
}

async fn publish_events(js: &jetstream::Context, count: usize) {
    for _ in 0..count {
        js.publish("events", "data".into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
}

async fn open_group_stream(consumer: &PullConsumer) -> pull::Stream {
    consumer
        .stream()
        .group("A")
        .expires(Duration::from_millis(500))
        .messages()
        .await
        .unwrap()
}

async fn take_messages(messages: &mut pull::Stream, count: usize) -> Vec<jetstream::Message> {
    tokio::time::timeout(
        Duration::from_secs(10),
        messages.by_ref().take(count).try_collect::<Vec<_>>(),
    )
    .await
    .expect("timed out waiting for messages")
    .unwrap()
}

fn read_pin_id(message: &jetstream::Message) -> String {
    message
        .headers
        .as_ref()
        .unwrap()
        .get_last("Nats-Pin-Id")
        .unwrap()
        .to_string()
}

async fn subscribe_to_pulls(js: &jetstream::Context, consumer: &str) -> async_nats::Subscriber {
    let pulls = js
        .client()
        .subscribe(format!("$JS.API.CONSUMER.MSG.NEXT.events.{consumer}"))
        .await
        .unwrap();
    js.client().flush().await.unwrap();
    pulls
}

/// Reads the `id` of the first pull in `pulls`. Polls `messages` meanwhile, so the pull is
/// re-issued as its expiry ends rather than by the task's fallback timer.
async fn read_first_pull_id(
    mut pulls: impl futures::Stream<Item = async_nats::Message> + Unpin,
    messages: &mut pull::Stream,
) -> serde_json::Value {
    let polled = tokio::time::timeout(
        Duration::from_secs(3),
        futures::future::select(pulls.next(), messages.next()),
    )
    .await
    .expect("timed out waiting for pulls");
    match polled {
        futures::future::Either::Left((pull, _)) => {
            serde_json::from_slice::<serde_json::Value>(&pull.unwrap().payload).unwrap()["id"]
                .clone()
        }
        futures::future::Either::Right((message, _)) => panic!("unexpected message: {message:?}"),
    }
}

/// Polls `messages` for `window` so it keeps re-issuing its expired pulls, and fails if it yields
/// anything.
async fn poll_idle_for(messages: &mut pull::Stream, window: Duration) {
    if let Ok(item) = tokio::time::timeout(window, messages.next()).await {
        panic!("unexpected item: {item:?}");
    }
}

#[tokio::test]
async fn pinned_stream_keeps_its_pin_id_past_priority_timeout() {
    let (_server, js, stream) = setup().await;
    let mut consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    publish_events(&js, 1).await;
    let mut messages = open_group_stream(&consumer).await;
    let pin_id = read_pin_id(&take_messages(&mut messages, 1).await[0]);

    poll_idle_for(&mut messages, Duration::from_secs(4)).await;

    let info = consumer.info().await.unwrap();
    assert_eq!(
        info.priority_groups[0].pinned_client_id.as_deref(),
        Some(pin_id.as_str())
    );
}

#[tokio::test]
async fn standby_stream_receives_nothing_while_pinned_stream_pulls() {
    let (_server, js, stream) = setup().await;
    let pinned_consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    let standby_consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    publish_events(&js, 1).await;
    let mut pinned = open_group_stream(&pinned_consumer).await;
    take_messages(&mut pinned, 1).await;
    let standby = open_group_stream(&standby_consumer).await;
    let (stop_standby, standby_stopped) = tokio::sync::oneshot::channel::<()>();
    let standby_items = tokio::spawn(standby.take_until(standby_stopped).count());
    poll_idle_for(&mut pinned, Duration::from_secs(4)).await;

    publish_events(&js, 10).await;
    take_messages(&mut pinned, 10).await;
    stop_standby.send(()).unwrap();

    assert_eq!(standby_items.await.unwrap(), 0);
}

#[tokio::test]
async fn standby_stream_takes_over_when_pinned_stream_is_dropped() {
    let (_server, js, stream) = setup().await;
    let pinned_consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    let standby_consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    publish_events(&js, 1).await;
    let mut pinned = open_group_stream(&pinned_consumer).await;
    take_messages(&mut pinned, 1).await;
    let mut standby = open_group_stream(&standby_consumer).await;

    drop(pinned);
    publish_events(&js, 10).await;

    assert_eq!(take_messages(&mut standby, 10).await.len(), 10);
}

#[tokio::test]
async fn pinned_stream_sends_its_pin_with_later_pulls() {
    let (_server, js, stream) = setup().await;
    let consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    let pulls = subscribe_to_pulls(&js, "pinned").await;
    publish_events(&js, 1).await;
    let mut messages = open_group_stream(&consumer).await;
    let pin_id = read_pin_id(&take_messages(&mut messages, 1).await[0]);

    let id = read_first_pull_id(pulls.skip(1), &mut messages).await;

    assert_eq!(id, pin_id.as_str());
}

#[tokio::test]
async fn pinned_stream_sends_the_server_pin_over_a_stored_one() {
    let (_server, js, stream) = setup().await;
    let mut consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    let pulls = subscribe_to_pulls(&js, "pinned").await;
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Pin-Id", "stored");
    js.publish_with_headers("events", headers, "data".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let mut messages = open_group_stream(&consumer).await;
    take_messages(&mut messages, 1).await;
    let pin_id = consumer.info().await.unwrap().priority_groups[0]
        .pinned_client_id
        .clone()
        .unwrap();

    let id = read_first_pull_id(pulls.skip(1), &mut messages).await;

    assert_eq!(id, pin_id.as_str());
}

#[tokio::test]
async fn pinned_stream_receives_again_soon_after_unpin() {
    let (_server, js, stream) = setup().await;
    let consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    publish_events(&js, 1).await;
    let mut messages = open_group_stream(&consumer).await;
    take_messages(&mut messages, 1).await;
    stream.unpin_consumer("pinned", "A").await.unwrap();

    publish_events(&js, 1).await;

    let received = tokio::time::timeout(Duration::from_millis(1500), messages.next()).await;
    assert!(
        received.is_ok(),
        "no message within half the priority_timeout"
    );
}

#[tokio::test]
async fn pinned_stream_sends_its_new_pin_after_unpin() {
    let (_server, js, stream) = setup().await;
    let consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    publish_events(&js, 1).await;
    let mut messages = open_group_stream(&consumer).await;
    take_messages(&mut messages, 1).await;
    stream.unpin_consumer("pinned", "A").await.unwrap();
    publish_events(&js, 1).await;
    let pin_id = read_pin_id(&take_messages(&mut messages, 1).await[0]);
    let pulls = subscribe_to_pulls(&js, "pinned").await;

    let id = read_first_pull_id(pulls, &mut messages).await;

    assert_eq!(id, pin_id.as_str());
}

#[tokio::test]
async fn standby_stream_takes_over_after_unpin() {
    let (_server, js, stream) = setup().await;
    let pinned_consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    let standby_consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    publish_events(&js, 1).await;
    let mut pinned = open_group_stream(&pinned_consumer).await;
    take_messages(&mut pinned, 1).await;
    let mut standby = open_group_stream(&standby_consumer).await;
    futures::join!(
        poll_idle_for(&mut pinned, Duration::from_millis(200)),
        poll_idle_for(&mut standby, Duration::from_millis(200)),
    );

    stream.unpin_consumer("pinned", "A").await.unwrap();
    publish_events(&js, 10).await;

    assert_eq!(take_messages(&mut standby, 10).await.len(), 10);
}

#[tokio::test]
async fn stream_of_unpinned_consumer_ignores_stored_pin_id() {
    let (_server, js, stream) = setup().await;
    let consumer: PullConsumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("plain".into()),
            ack_policy: AckPolicy::None,
            ..Default::default()
        })
        .await
        .unwrap();
    let pulls = subscribe_to_pulls(&js, "plain").await;
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Pin-Id", "stored");
    js.publish_with_headers("events", headers, "data".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let mut messages = consumer
        .stream()
        .expires(Duration::from_millis(500))
        .messages()
        .await
        .unwrap();
    messages.try_next().await.unwrap();

    let id = read_first_pull_id(pulls.skip(1), &mut messages).await;

    assert_eq!(id, serde_json::Value::Null);
}

#[tokio::test]
async fn fetch_rejects_pinned_client_consumer() {
    let (_server, _js, stream) = setup().await;
    let consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();

    let err = consumer.fetch().group("A").messages().await.err().unwrap();

    assert_eq!(err.kind(), pull::BatchErrorKind::PinnedClient);
}

#[tokio::test]
async fn batch_rejects_pinned_client_consumer() {
    let (_server, _js, stream) = setup().await;
    let consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();

    let err = consumer.batch().group("A").messages().await.err().unwrap();

    assert_eq!(err.kind(), pull::BatchErrorKind::PinnedClient);
}

#[tokio::test]
async fn sequence_rejects_pinned_client_consumer() {
    let (_server, _js, stream) = setup().await;
    let consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();

    let err = consumer.sequence(1).err().unwrap();

    assert_eq!(err.kind(), pull::BatchErrorKind::PinnedClient);
}

#[tokio::test]
async fn info_reports_the_pin_id_of_the_pinned_client() {
    let (_server, js, stream) = setup().await;
    let mut consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    publish_events(&js, 1).await;
    let mut messages = open_group_stream(&consumer).await;
    let pin_id = read_pin_id(&take_messages(&mut messages, 1).await[0]);

    let info = consumer.info().await.unwrap();

    assert_eq!(
        info.priority_groups[0].pinned_client_id.as_deref(),
        Some(pin_id.as_str())
    );
}

#[cfg(feature = "server_2_12")]
#[tokio::test]
async fn unpin_consumer_releases_the_pinned_client() {
    let (_server, js, stream) = setup().await;
    let mut consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    publish_events(&js, 1).await;
    let mut messages = open_group_stream(&consumer).await;
    take_messages(&mut messages, 1).await;
    drop(messages);

    stream.unpin_consumer("pinned", "A").await.unwrap();

    let info = consumer.info().await.unwrap();
    assert_eq!(info.priority_groups[0].pinned_client_id, None);
}

#[tokio::test]
async fn unpin_consumer_fails_for_a_missing_consumer() {
    let (_server, _js, stream) = setup().await;

    let err = stream.unpin_consumer("missing", "A").await.unwrap_err();

    assert!(matches!(
        err.kind(),
        ConsumerErrorKind::JetStream(error) if error.error_code() == ErrorCode::CONSUMER_NOT_FOUND
    ));
}

#[tokio::test]
async fn unpin_consumer_rejects_an_invalid_consumer_name() {
    let (_server, _js, stream) = setup().await;

    let err = stream.unpin_consumer("pinned.>", "A").await.unwrap_err();

    assert_eq!(err.kind(), ConsumerErrorKind::InvalidName);
}

#[test]
fn priority_group_state_maps_zero_pinned_ts_to_none() {
    let state: PriorityGroupState =
        serde_json::from_str(r#"{"group":"A","pinned_ts":"0001-01-01T00:00:00Z"}"#).unwrap();

    assert_eq!(state.pinned_ts, None);
}

#[test]
fn priority_group_state_parses_pinned_ts() {
    let state: PriorityGroupState = serde_json::from_str(
        r#"{"group":"A","pinned_client_id":"pin","pinned_ts":"2026-01-02T03:04:05Z"}"#,
    )
    .unwrap();

    assert_eq!(
        state.pinned_ts,
        Some(async_nats::datetime::parse_rfc3339("2026-01-02T03:04:05Z").unwrap())
    );
}

#[test]
fn priority_timeout_is_omitted_when_zero() {
    let config = serde_json::to_value(pull::Config::default()).unwrap();

    assert!(config.get("priority_timeout").is_none());
}

#[test]
fn priority_timeout_serializes_as_nanos() {
    let config = serde_json::to_value(pull::Config {
        priority_timeout: Duration::from_secs(1),
        ..Default::default()
    })
    .unwrap();

    assert_eq!(config["priority_timeout"], 1_000_000_000);
}
