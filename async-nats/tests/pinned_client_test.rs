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
use futures::TryStreamExt;

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
            ..Default::default()
        })
        .await
        .unwrap();
    (server, js, stream)
}

async fn fetch_from_group(consumer: &PullConsumer) -> Vec<jetstream::Message> {
    consumer
        .fetch()
        .group("A")
        .max_messages(5)
        .messages()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn pinned_client_info() {
    let (_server, js, stream) = setup().await;
    let mut consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    js.publish("events", "data".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let messages = fetch_from_group(&consumer).await;
    let pin_id = messages[0]
        .headers
        .as_ref()
        .unwrap()
        .get("Nats-Pin-Id")
        .unwrap()
        .to_string();

    let info = consumer.info().await.unwrap();

    assert_eq!(
        info.priority_groups[0].pinned_client_id.as_deref(),
        Some(pin_id.as_str())
    );
}

#[tokio::test]
async fn unpin_releases_pinned_client() {
    let (_server, js, stream) = setup().await;
    let mut consumer: PullConsumer = stream.get_consumer("pinned").await.unwrap();
    js.publish("events", "data".into())
        .await
        .unwrap()
        .await
        .unwrap();
    fetch_from_group(&consumer).await;

    stream.unpin_consumer("pinned", "A").await.unwrap();

    let info = consumer.info().await.unwrap();
    assert_eq!(info.priority_groups[0].pinned_client_id, None);
}

#[tokio::test]
async fn unpin_missing_consumer() {
    let (_server, _js, stream) = setup().await;

    let err = stream.unpin_consumer("missing", "A").await.unwrap_err();

    assert!(matches!(
        err.kind(),
        ConsumerErrorKind::JetStream(error) if error.error_code() == ErrorCode::CONSUMER_NOT_FOUND
    ));
}

#[tokio::test]
async fn unpin_invalid_consumer_name() {
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
fn priority_timeout_zero_is_omitted() {
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
