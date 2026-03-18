// Copyright 2026 The NATS Authors
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

#![cfg(all(target_arch = "wasm32", feature = "websockets-web"))]

use futures_util::StreamExt;
use wasm_bindgen_test::wasm_bindgen_test;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

const WS_SERVER_URL: &str = "ws://localhost:8444";

async fn connect_client() -> async_nats::Client {
    async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .connect(WS_SERVER_URL)
        .await
        .unwrap()
}

#[wasm_bindgen_test]
fn websocket_server_addr_is_supported() {
    let server = WS_SERVER_URL.parse::<async_nats::ServerAddr>().unwrap();

    assert!(server.is_websocket());
    assert_eq!(server.scheme(), "ws");
    assert_eq!(server.as_url_str(), "ws://localhost:8444/");
}

#[wasm_bindgen_test]
async fn websocket_core_pub_sub() {
    let client = connect_client().await;

    let mut sub = client.subscribe("foo").await.unwrap();
    client.publish("foo", "hello".into()).await.unwrap();

    assert_eq!(sub.next().await.unwrap().payload, "hello");
}

#[wasm_bindgen_test]
async fn websocket_large_messages() {
    let client = connect_client().await;
    let payload = bytes::Bytes::from(vec![7; 256 * 1024]);

    let mut sub = client.subscribe("foo.large").await.unwrap().take(4);
    for _ in 0..4 {
        client.publish("foo.large", payload.clone()).await.unwrap();
    }

    while let Some(msg) = sub.next().await {
        assert_eq!(msg.payload, payload);
    }
}

#[wasm_bindgen_test]
async fn websocket_request_reply() {
    let client = connect_client().await;
    let mut requests = client.subscribe("foo.request").await.unwrap();

    tokio::task::spawn_local({
        let client = client.clone();
        async move {
            let request = requests.next().await.unwrap();
            client
                .publish(request.reply.unwrap(), request.payload)
                .await
                .unwrap();
        }
    });

    let response = client.request("foo.request", "hello".into()).await.unwrap();
    assert_eq!(response.payload, "hello");
}
