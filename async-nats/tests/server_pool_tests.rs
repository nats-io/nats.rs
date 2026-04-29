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

//! Integration tests for ADR-40: Custom Server Pool and Reconnect-to-Server Callback.

mod server_pool {
    use async_nats::connection::State;
    use async_nats::{ConnectOptions, Event, ServerAddr};
    use futures_util::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Helper: wait for an event matching the predicate, with timeout.
    async fn wait_for_event(
        rx: &mut tokio::sync::mpsc::Receiver<Event>,
        timeout: Duration,
        mut pred: impl FnMut(&Event) -> bool,
    ) {
        tokio::time::timeout(timeout, async {
            while let Some(ev) = rx.recv().await {
                if pred(&ev) {
                    return;
                }
            }
            panic!("event channel closed without matching event");
        })
        .await
        .expect("timed out waiting for event");
    }

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

    type EventFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + Sync>>;
    type EventCallback = Box<dyn Fn(Event) -> EventFuture + Send + Sync>;

    /// Helper: create event callback and return the channel receiver.
    fn event_channel() -> (EventCallback, tokio::sync::mpsc::Receiver<Event>) {
        let (tx, rx) = tokio::sync::mpsc::channel::<Event>(128);
        let cb: EventCallback = Box::new(move |event: Event| {
            let tx = tx.clone();
            Box::pin(async move {
                tx.send(event).await.ok();
            })
        });
        (cb, rx)
    }

    // ──────────────────────────────────────────────
    //  server_pool() snapshot tests
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn server_pool_returns_initial_servers() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let pool = client.server_pool().await.unwrap();
        assert!(!pool.is_empty(), "pool should contain at least one server");
        let url: ServerAddr = server.client_url().parse().unwrap();
        assert!(
            pool.iter().any(|s| s.addr == url),
            "pool should contain the server we connected to"
        );
    }

    #[tokio::test]
    async fn server_pool_initial_server_shows_did_connect() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let pool = client.server_pool().await.unwrap();
        let url: ServerAddr = server.client_url().parse().unwrap();
        let entry = pool.iter().find(|s| s.addr == url).unwrap();
        assert!(
            entry.did_connect,
            "server we connected to should have did_connect=true"
        );
        assert_eq!(
            entry.failed_attempts, 0,
            "failed_attempts should be 0 after fresh connect"
        );
        assert!(
            !entry.is_discovered,
            "explicitly connected server should not be implicit"
        );
    }

    #[tokio::test]
    async fn server_pool_contains_discovered_servers() {
        let cluster = nats_server::run_cluster("tests/configs/jetstream.conf");
        let client = async_nats::connect(cluster.client_url()).await.unwrap();

        // Poll until the cluster INFO exchange propagates discovered servers.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let pool = client.server_pool().await.unwrap();
                if pool.len() >= 2 {
                    let implicit_count = pool.iter().filter(|s| s.is_discovered).count();
                    assert!(
                        implicit_count >= 1,
                        "cluster pool should have at least 1 implicit server, got {}",
                        implicit_count
                    );
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timed out waiting for discovered servers in pool");
    }

    // ──────────────────────────────────────────────
    //  set_server_pool() tests
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn set_server_pool_replaces_pool() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let fake_addr: ServerAddr = "nats://fake-host-1:4222".parse().unwrap();
        let fake_addr2: ServerAddr = "nats://fake-host-2:4222".parse().unwrap();

        client
            .set_server_pool(vec![
                "nats://fake-host-1:4222".to_string(),
                "nats://fake-host-2:4222".to_string(),
            ])
            .await
            .unwrap();

        let pool = client.server_pool().await.unwrap();
        assert_eq!(
            pool.len(),
            2,
            "pool should have exactly 2 servers after replacement"
        );
        assert!(pool.iter().any(|s| s.addr == fake_addr));
        assert!(pool.iter().any(|s| s.addr == fake_addr2));

        // The original server should be gone.
        let orig: ServerAddr = server.client_url().parse().unwrap();
        assert!(
            !pool.iter().any(|s| s.addr == orig),
            "original server should be removed from pool"
        );
    }

    #[tokio::test]
    async fn set_server_pool_preserves_state_for_overlapping_servers() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let url = server.client_url();

        // Replace pool but include the current server.
        client
            .set_server_pool(vec![url.clone(), "nats://other:4222".to_string()])
            .await
            .unwrap();

        let pool = client.server_pool().await.unwrap();
        let orig_addr: ServerAddr = url.parse().unwrap();
        let entry = pool.iter().find(|s| s.addr == orig_addr).unwrap();
        assert!(
            entry.did_connect,
            "did_connect should be preserved for overlapping server"
        );

        let other_addr: ServerAddr = "nats://other:4222".parse().unwrap();
        let other = pool.iter().find(|s| s.addr == other_addr).unwrap();
        assert!(
            !other.did_connect,
            "new server should have did_connect=false"
        );
    }

    #[tokio::test]
    async fn set_server_pool_marks_all_as_explicit() {
        let cluster = nats_server::run_cluster("tests/configs/jetstream.conf");
        let client = async_nats::connect(cluster.client_url()).await.unwrap();

        // Poll until we have implicit servers.
        let pool_before = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let pool = client.server_pool().await.unwrap();
                if pool.iter().any(|s| s.is_discovered) && pool.len() >= 2 {
                    return pool;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timed out waiting for implicit servers");

        // Set a new pool using actual server addresses from the pool.
        let addrs: Vec<ServerAddr> = pool_before.iter().take(2).map(|s| s.addr.clone()).collect();
        client.set_server_pool(addrs).await.unwrap();

        let pool_after = client.server_pool().await.unwrap();
        assert_eq!(pool_after.len(), 2);
        assert!(
            pool_after.iter().all(|s| !s.is_discovered),
            "all servers in an explicitly set pool should be is_discovered=false"
        );
    }

    #[tokio::test]
    async fn set_server_pool_rejects_empty_vec() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let result = client.set_server_pool(Vec::<ServerAddr>::new()).await;
        assert!(
            result.is_err(),
            "empty vec should be rejected by set_server_pool"
        );
        assert_eq!(
            result.unwrap_err().kind(),
            async_nats::SetServerPoolErrorKind::EmptyPool,
        );
    }

    // ──────────────────────────────────────────────
    //  set_server_pool + force_reconnect integration
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn set_server_pool_then_force_reconnect() {
        let server1 = nats_server::run_basic_server();
        let server2 = nats_server::run_basic_server();

        let (cb, mut event_rx) = event_channel();
        let client = ConnectOptions::new()
            .event_callback(cb)
            .connect(server1.client_url())
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        // Replace pool with server2 only.
        client
            .set_server_pool(vec![server2.client_url()])
            .await
            .unwrap();

        // Force reconnect to switch to the new pool.
        client.force_reconnect().await.unwrap();

        wait_for_reconnect(&mut event_rx, Duration::from_secs(10)).await;

        // Client should now be connected to server2.
        let info = client.server_info();
        assert_eq!(
            info.port,
            server2.client_port(),
            "after set_server_pool + force_reconnect, should be connected to server2"
        );

        // Verify pub/sub still works.
        let mut sub = client.subscribe("test").await.unwrap();
        client.publish("test", "hello".into()).await.unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.payload.as_ref(), b"hello");
    }

    // ──────────────────────────────────────────────
    //  reconnect_to_server_callback tests
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn reconnect_callback_selects_server() {
        // Connect to server1 only, then add server2 to the pool via the callback.
        // Kill server1 → the callback should direct reconnection to server2.
        let server1 = nats_server::run_basic_server();
        let server2 = nats_server::run_basic_server();

        let server2_addr: ServerAddr = server2.client_url().parse().unwrap();
        let callback_addr = server2_addr.clone();

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();

        let (cb, mut event_rx) = event_channel();

        // Connect to server1 initially. Put both servers in the pool
        // via set_server_pool after connecting.
        let client = ConnectOptions::new()
            .reconnect_to_server_callback(move |servers, _info| {
                let addr = callback_addr.clone();
                let count = callback_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    if servers.iter().any(|s| s.addr == addr) {
                        Some(async_nats::ReconnectToServer {
                            addr: addr.clone(),
                            delay: Some(Duration::ZERO),
                        })
                    } else {
                        None
                    }
                }
            })
            .event_callback(cb)
            .connect(server1.client_url())
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        // Ensure both servers are in the pool.
        client
            .set_server_pool(vec![server1.client_url(), server2.client_url()])
            .await
            .unwrap();

        // Kill server1 to trigger reconnection.
        let port1 = server1.client_port();
        drop(server1);

        wait_for_reconnect(&mut event_rx, Duration::from_secs(10)).await;

        assert!(
            callback_count.load(Ordering::SeqCst) >= 1,
            "reconnect callback should have been called at least once"
        );

        let info = client.server_info();
        assert_ne!(
            info.port, port1,
            "should not be connected to the killed server1"
        );
        assert_eq!(
            info.port,
            server2.client_port(),
            "callback should have directed reconnect to server2"
        );
    }

    #[tokio::test]
    async fn reconnect_callback_returning_none_uses_default() {
        let server = nats_server::run_basic_server();

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();

        let (cb, mut event_rx) = event_channel();
        let _client = ConnectOptions::new()
            .reconnect_to_server_callback(move |_servers, _info| {
                let count = callback_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    None
                }
            })
            .event_callback(cb)
            .connect(server.client_url())
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        // Use force_reconnect instead of server.restart() (dynamic ports can't restart).
        _client.force_reconnect().await.unwrap();

        wait_for_reconnect(&mut event_rx, Duration::from_secs(10)).await;

        assert!(callback_count.load(Ordering::SeqCst) >= 1);
        assert_eq!(
            _client.connection_state(),
            State::Connected,
            "should have reconnected using default selection"
        );
    }

    #[tokio::test]
    async fn reconnect_callback_invalid_server_falls_back_to_default() {
        let server = nats_server::run_basic_server();

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();

        let saw_not_in_pool_error = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_not_in_pool_clone = saw_not_in_pool_error.clone();

        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<Event>(128);
        let _client = ConnectOptions::new()
            .reconnect_to_server_callback(move |_servers, _info| {
                let count = callback_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Some(async_nats::ReconnectToServer {
                        addr: "nats://not-in-pool:9999".parse().unwrap(),
                        delay: Some(Duration::ZERO),
                    })
                }
            })
            .event_callback(move |event| {
                let tx = event_tx.clone();
                let flag = saw_not_in_pool_clone.clone();
                async move {
                    if matches!(
                        event,
                        Event::ClientError(async_nats::ClientError::ServerNotInPool)
                    ) {
                        flag.store(true, Ordering::SeqCst);
                    }
                    tx.send(event).await.ok();
                }
            })
            .connect(server.client_url())
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        _client.force_reconnect().await.unwrap();

        wait_for_reconnect(&mut event_rx, Duration::from_secs(10)).await;

        assert!(callback_count.load(Ordering::SeqCst) >= 1);
        assert!(
            saw_not_in_pool_error.load(Ordering::SeqCst),
            "should have emitted a ServerNotInPool ClientError event"
        );
        assert_eq!(_client.connection_state(), State::Connected);
    }

    #[tokio::test]
    async fn reconnect_callback_receives_last_server_info() {
        let server = nats_server::run_basic_server();
        let server_port = server.client_port();

        let received_info_port = Arc::new(AtomicUsize::new(0));
        let received_info_port_clone = received_info_port.clone();

        let (cb, mut event_rx) = event_channel();
        let _client = ConnectOptions::new()
            .reconnect_to_server_callback(move |_servers, info| {
                let port_holder = received_info_port_clone.clone();
                async move {
                    port_holder.store(info.port as usize, Ordering::SeqCst);
                    None
                }
            })
            .event_callback(cb)
            .connect(server.client_url())
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        _client.force_reconnect().await.unwrap();

        wait_for_reconnect(&mut event_rx, Duration::from_secs(10)).await;

        let received_port = received_info_port.load(Ordering::SeqCst);
        assert_eq!(
            received_port, server_port as usize,
            "callback should receive the last known ServerInfo with the correct port"
        );
    }

    #[tokio::test]
    async fn reconnect_callback_with_delay() {
        let server = nats_server::run_basic_server();

        let (cb, mut event_rx) = event_channel();
        let _client = ConnectOptions::new()
            .reconnect_to_server_callback(move |servers, _info| async move {
                servers.first().map(|s| async_nats::ReconnectToServer {
                    addr: s.addr.clone(),
                    delay: Some(Duration::from_secs(2)),
                })
            })
            .event_callback(cb)
            .connect(server.client_url())
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        _client.force_reconnect().await.unwrap();

        let start = std::time::Instant::now();

        wait_for_reconnect(&mut event_rx, Duration::from_secs(15)).await;

        let elapsed = start.elapsed();
        // The callback specifies a 2-second delay.
        // Use generous lower bound to avoid CI flakes.
        assert!(
            elapsed >= Duration::from_secs(1),
            "reconnect should have been delayed by ~2s, but took {:?}",
            elapsed
        );
    }

    // ──────────────────────────────────────────────
    //  server_pool state tracking after reconnect
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn server_pool_updates_did_connect_after_reconnect() {
        let server1 = nats_server::run_basic_server();
        let server2 = nats_server::run_basic_server();

        let s1_addr: ServerAddr = server1.client_url().parse().unwrap();
        let s2_addr: ServerAddr = server2.client_url().parse().unwrap();

        let (cb, mut event_rx) = event_channel();
        let client = ConnectOptions::new()
            .event_callback(cb)
            .connect(vec![s1_addr, s2_addr])
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        // Initially, only the connected server should have did_connect=true.
        let pool = client.server_pool().await.unwrap();
        let connected_count = pool.iter().filter(|s| s.did_connect).count();
        assert_eq!(
            connected_count, 1,
            "initially only one server should have did_connect=true"
        );

        // Set pool to just server2, force reconnect.
        client
            .set_server_pool(vec![server2.client_url()])
            .await
            .unwrap();
        client.force_reconnect().await.unwrap();

        wait_for_reconnect(&mut event_rx, Duration::from_secs(10)).await;

        let pool = client.server_pool().await.unwrap();
        let s2_addr: ServerAddr = server2.client_url().parse().unwrap();
        let s2 = pool.iter().find(|s| s.addr == s2_addr).unwrap();
        assert!(
            s2.did_connect,
            "server2 should have did_connect=true after reconnecting to it"
        );
    }

    // ──────────────────────────────────────────────
    //  max_reconnects interaction
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn set_server_pool_to_unreachable_with_max_reconnects() {
        let server = nats_server::run_basic_server();

        let (cb, mut event_rx) = event_channel();
        let client = ConnectOptions::new()
            .max_reconnects(3)
            .event_callback(cb)
            .connect(server.client_url())
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        // Set pool to only unreachable servers, then force reconnect.
        client
            .set_server_pool(vec!["nats://192.0.2.1:4222".to_string()])
            .await
            .unwrap();
        client.force_reconnect().await.unwrap();

        // Should emit MaxReconnects after exhausting attempts.
        let got_max_reconnects = tokio::time::timeout(Duration::from_secs(30), async {
            while let Some(ev) = event_rx.recv().await {
                if matches!(
                    ev,
                    Event::ClientError(async_nats::ClientError::MaxReconnects)
                ) {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        assert!(
            got_max_reconnects,
            "should emit MaxReconnects after exhausting attempts to unreachable server"
        );
    }

    // ──────────────────────────────────────────────
    //  Multiple set_server_pool calls
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn set_server_pool_can_be_called_multiple_times() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        client
            .set_server_pool(vec!["nats://host-a:4222".to_string()])
            .await
            .unwrap();
        let pool = client.server_pool().await.unwrap();
        assert_eq!(pool.len(), 1);

        client
            .set_server_pool(vec![
                "nats://host-b:4222".to_string(),
                "nats://host-c:4222".to_string(),
            ])
            .await
            .unwrap();
        let pool = client.server_pool().await.unwrap();
        assert_eq!(pool.len(), 2);

        // Go back to the real server.
        client
            .set_server_pool(vec![server.client_url()])
            .await
            .unwrap();
        let pool = client.server_pool().await.unwrap();
        assert_eq!(pool.len(), 1);
    }

    // ──────────────────────────────────────────────
    //  reconnect_to_server_callback called repeatedly
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn reconnect_callback_called_on_every_attempt() {
        // Server1 will die, server2 stays up.
        // The callback will return server1 (dead, in pool) on the first call,
        // which will fail to connect. Then on the next call it returns server2.
        let server1 = nats_server::run_basic_server();
        let server2 = nats_server::run_basic_server();

        let s1_addr: ServerAddr = server1.client_url().parse().unwrap();
        let s2_addr: ServerAddr = server2.client_url().parse().unwrap();
        let s1_clone = s1_addr.clone();
        let s2_clone = s2_addr.clone();

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();

        let (cb, mut event_rx) = event_channel();
        let _client = ConnectOptions::new()
            .reconnect_to_server_callback(move |_servers, _info| {
                let count = callback_count_clone.clone();
                let s1 = s1_clone.clone();
                let s2 = s2_clone.clone();
                async move {
                    let n = count.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        // First call: try the dead server1 (it IS in the pool).
                        Some(async_nats::ReconnectToServer {
                            addr: s1,
                            delay: Some(Duration::ZERO),
                        })
                    } else {
                        // Subsequent calls: select the alive server2.
                        Some(async_nats::ReconnectToServer {
                            addr: s2,
                            delay: Some(Duration::ZERO),
                        })
                    }
                }
            })
            .event_callback(cb)
            .connect(vec![s1_addr, s2_addr])
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        // Kill server1 to trigger reconnect.
        drop(server1);

        wait_for_reconnect(&mut event_rx, Duration::from_secs(15)).await;

        let count = callback_count.load(Ordering::SeqCst);
        assert!(
            count >= 2,
            "callback should be called on every reconnect attempt, got {} calls",
            count
        );
    }

    // ──────────────────────────────────────────────
    //  Callback receives correct pool metadata
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn reconnect_callback_receives_pool_with_metadata() {
        let server = nats_server::run_basic_server();

        let received_pool = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_pool_clone = received_pool.clone();

        let (cb, mut event_rx) = event_channel();
        let _client = ConnectOptions::new()
            .reconnect_to_server_callback(move |servers, _info| {
                let pool = received_pool_clone.clone();
                async move {
                    *pool.lock().await = servers.clone();
                    None
                }
            })
            .event_callback(cb)
            .connect(server.client_url())
            .await
            .unwrap();

        wait_for_event(&mut event_rx, Duration::from_secs(5), |ev| {
            *ev == Event::Connected
        })
        .await;

        _client.force_reconnect().await.unwrap();

        wait_for_reconnect(&mut event_rx, Duration::from_secs(10)).await;

        let pool = received_pool.lock().await;
        assert!(
            !pool.is_empty(),
            "callback should receive a non-empty pool snapshot"
        );

        let url: ServerAddr = server.client_url().parse().unwrap();
        let entry = pool.iter().find(|s| s.addr == url).unwrap();
        assert!(
            entry.did_connect,
            "pool snapshot should show did_connect=true for the previously connected server"
        );
    }
}
