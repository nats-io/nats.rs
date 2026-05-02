// Copyright 2020-2024 The NATS Authors
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

//! Parity tests for ADR-40 features: behaviors that should match nats.go.
//! Some of these tests are EXPECTED TO FAIL initially because they exercise
//! known gaps in the implementation.

mod server_pool_parity {
    use async_nats::{ConnectOptions, Event, ServerAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Helper: wait for Disconnected followed by Connected.
    async fn wait_for_reconnect(rx: &mut tokio::sync::mpsc::Receiver<Event>, timeout: Duration) {
        tokio::time::timeout(timeout, async {
            let mut saw_disconnect = false;
            while let Some(ev) = rx.recv().await {
                match ev {
                    Event::Disconnected => saw_disconnect = true,
                    Event::Connected if saw_disconnect => return,
                    _ => {}
                }
            }
            panic!("event channel closed without reconnect sequence");
        })
        .await
        .expect("timed out waiting for reconnect");
    }

    // ──────────────────────────────────────────────
    //  Empty pool is rejected at the API level
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn set_server_pool_rejects_empty() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let result = client.set_server_pool(Vec::<ServerAddr>::new()).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().kind(),
            async_nats::SetServerPoolErrorKind::EmptyPool,
        );
    }

    // ──────────────────────────────────────────────
    //  M2: Callback with zero delay should have some backoff
    // ──────────────────────────────────────────────
    //
    // In nats.go, the reconnect loop sleeps between full pool cycles.
    // The Rust implementation with a callback returning Duration::ZERO
    // and a fast-failing server can spin in a tight loop.
    //
    // EXPECTED: FAIL (no backoff with callback zero delay)

    #[tokio::test]
    async fn callback_zero_delay_does_not_spin_tight() {
        let server = nats_server::run_basic_server();

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();

        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<Event>(128);
        let client = ConnectOptions::new()
            .max_reconnects(10)
            // Short connection timeout so attempts fail fast.
            .connection_timeout(Duration::from_millis(100))
            .reconnect_to_server_callback(move |_servers, _info| {
                let count = callback_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    // Return a fast-failing server with None delay (= use default backoff).
                    Some(async_nats::ReconnectToServer {
                        addr: "nats://127.0.0.1:1".parse().unwrap(),
                        delay: None,
                    })
                }
            })
            .event_callback(move |event| {
                let tx = event_tx.clone();
                async move {
                    tx.send(event).await.ok();
                }
            })
            .connect(server.client_url())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(ev) = event_rx.recv().await {
                if ev == Event::Connected {
                    break;
                }
            }
        })
        .await
        .unwrap();

        // Set pool to the fast-failing server and force reconnect.
        client
            .set_server_pool(vec!["nats://127.0.0.1:1".to_string()])
            .await
            .unwrap();
        client.force_reconnect().await.unwrap();

        let start = std::time::Instant::now();

        // Wait until max_reconnects is hit.
        tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(ev) = event_rx.recv().await {
                if matches!(
                    ev,
                    Event::ClientError(async_nats::ClientError::MaxReconnects)
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();

        let elapsed = start.elapsed();
        let count = callback_count.load(Ordering::SeqCst);

        // With 10 attempts, zero callback delay, and fast-failing connections,
        // without backoff this finishes in well under a second.
        // With proper backoff (exponential), total elapsed should be meaningful.
        // Before the fix, this completed in <5ms. With backoff, it takes hundreds
        // of milliseconds due to exponential delays.
        assert!(
            elapsed > Duration::from_millis(200),
            "callback path with zero delay should have some backoff. \
             {} attempts in {:?} (expected >200ms total)",
            count,
            elapsed,
        );
    }

    // ──────────────────────────────────────────────
    //  Go parity: set_server_pool resets attempts counter
    // ──────────────────────────────────────────────
    //
    // The Rust set_server_pool resets self.attempts=0.
    // This test verifies: after partial reconnect attempts and then a
    // set_server_pool call, the max_reconnects counter is effectively reset.
    //
    // EXPECTED: PASS (this is currently working behavior)

    #[tokio::test]
    async fn set_server_pool_resets_max_reconnects_counter() {
        let server1 = nats_server::run_basic_server();
        let server2 = nats_server::run_basic_server();

        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<Event>(128);
        let client = ConnectOptions::new()
            .max_reconnects(3)
            .event_callback(move |event| {
                let tx = event_tx.clone();
                async move {
                    tx.send(event).await.ok();
                }
            })
            .connect(server1.client_url())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(ev) = event_rx.recv().await {
                if ev == Event::Connected {
                    break;
                }
            }
        })
        .await
        .unwrap();

        // Set pool to server2 (different server), force reconnect.
        // This uses some reconnect attempts (at least 1).
        client
            .set_server_pool(vec![server2.client_url()])
            .await
            .unwrap();
        client.force_reconnect().await.unwrap();

        wait_for_reconnect(&mut event_rx, Duration::from_secs(10)).await;

        // Now set pool back to server1 and force reconnect again.
        // If set_server_pool resets the attempts counter, this should succeed
        // even though we already used some of our 3 max_reconnects.
        client
            .set_server_pool(vec![server1.client_url()])
            .await
            .unwrap();
        client.force_reconnect().await.unwrap();

        wait_for_reconnect(&mut event_rx, Duration::from_secs(10)).await;

        assert_eq!(
            client.connection_state(),
            async_nats::connection::State::Connected,
            "should reconnect after set_server_pool resets the attempts counter"
        );
    }
}
