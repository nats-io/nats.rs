// Copyright 2024 The NATS Authors
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

#[cfg(feature = "websockets")]
mod websockets {
    use std::path::PathBuf;

    use futures_util::StreamExt;

    #[tokio::test]
    async fn core() {
        let _server = nats_server::run_server("tests/configs/ws.conf");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("ws://localhost:8444")
            .await
            .unwrap();

        // Simple pub/sub
        let mut sub = client.subscribe("foo").await.unwrap();
        client.publish("foo", "hello".into()).await.unwrap();
        assert_eq!(sub.next().await.unwrap().payload, "hello");

        // Large messages
        let payload = bytes::Bytes::from(vec![22; 1024 * 1024]);

        let mut sub = client.subscribe("foo").await.unwrap().take(10);
        for _ in 0..10 {
            client.publish("foo", payload.clone()).await.unwrap();
        }
        while let Some(msg) = sub.next().await {
            assert_eq!(msg.payload, payload);
        }

        // Request/reply
        let mut requests = client.subscribe("foo").await.unwrap();
        tokio::task::spawn({
            let client = client.clone();
            async move {
                let request = requests.next().await.unwrap();
                client
                    .publish(request.reply.unwrap(), request.payload)
                    .await
                    .unwrap();
            }
        });
        let response = client.request("foo", "hello".into()).await.unwrap();
        assert_eq!(response.payload, "hello");
    }

    #[tokio::test]
    async fn tls() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let _server = nats_server::run_server("tests/configs/ws_tls.conf");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let client = async_nats::ConnectOptions::new()
            .user_and_password("derek".into(), "porkchop".into())
            .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
            .connect("wss://localhost:8445")
            .await
            .unwrap();

        let mut sub = client.subscribe("foo").await.unwrap();
        client.publish("foo", "hello".into()).await.unwrap();
        assert_eq!(sub.next().await.unwrap().payload, "hello");
    }

    /// Starts a `nats-server` instance with an ephemeral WebSocket port and an in-process
    /// TCP proxy that records the HTTP handshake headers for every WebSocket connection.
    async fn spawn_ws_server_with_header_inspector() -> (
        nats_server::Server,
        std::net::SocketAddr,
        tokio::sync::mpsc::UnboundedReceiver<async_nats::http::HeaderMap>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let ws_port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let cfg_path = std::env::temp_dir().join(format!(
            "nats-ws-test-{}-{}.conf",
            std::process::id(),
            ws_port
        ));
        std::fs::write(
            &cfg_path,
            format!("websocket {{\n  port: {ws_port}\n  no_tls: true\n}}\n"),
        )
        .unwrap();

        let server = nats_server::run_server(cfg_path.to_str().unwrap());
        // Wait for the server to start accepting connections.
        let _ = server.client_port();
        let _ = std::fs::remove_file(&cfg_path);

        let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        let (headers_tx, headers_rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = proxy_listener.accept().await {
                let headers_tx = headers_tx.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::with_capacity(1024);
                    let mut temp = [0u8; 512];
                    let header_end = loop {
                        let n = match inbound.read(&mut temp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&temp[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };

                    let req_str = String::from_utf8_lossy(&buf[..header_end]);
                    let mut parsed_headers = async_nats::http::HeaderMap::new();
                    for line in req_str.lines().skip(1) {
                        if let Some((name, value)) = line.split_once(':') {
                            if let (Ok(h_name), Ok(h_val)) = (
                                name.trim().parse::<async_nats::http::HeaderName>(),
                                value.trim().parse::<async_nats::http::HeaderValue>(),
                            ) {
                                parsed_headers.insert(h_name, h_val);
                            }
                        }
                    }
                    let _ = headers_tx.send(parsed_headers);

                    let Ok(mut outbound) =
                        tokio::net::TcpStream::connect(("127.0.0.1", ws_port)).await
                    else {
                        return;
                    };
                    if outbound.write_all(&buf).await.is_err() {
                        return;
                    }
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                });
            }
        });

        (server, proxy_addr, headers_rx)
    }

    #[tokio::test]
    async fn websocket_static_headers() {
        use async_nats::http::{header::AUTHORIZATION, HeaderMap, HeaderValue};

        let (_server, proxy_addr, mut headers_rx) = spawn_ws_server_with_header_inspector().await;

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer static-jwt"));
        headers.insert(
            "x-custom-header",
            HeaderValue::from_static("custom-header-value"),
        );

        let client = async_nats::ConnectOptions::new()
            .websocket_headers(headers)
            .connect(format!("ws://{proxy_addr}"))
            .await
            .unwrap();

        let captured = tokio::time::timeout(std::time::Duration::from_secs(5), headers_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(captured.get(AUTHORIZATION).unwrap(), "Bearer static-jwt");
        assert_eq!(
            captured.get("x-custom-header").unwrap(),
            "custom-header-value"
        );

        let mut sub = client.subscribe("ws.headers").await.unwrap();
        client.publish("ws.headers", "ok".into()).await.unwrap();
        assert_eq!(sub.next().await.unwrap().payload, "ok");
    }

    #[tokio::test]
    async fn websocket_headers_callback_refresh_on_reconnect() {
        use async_nats::http::{HeaderMap, HeaderValue};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let (_server, proxy_addr, mut headers_rx) = spawn_ws_server_with_header_inspector().await;

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let client = async_nats::ConnectOptions::new()
            .event_callback(move |event| {
                let event_tx = event_tx.clone();
                async move {
                    let _ = event_tx.send(event);
                }
            })
            .websocket_headers_callback(move || {
                let attempt = counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    let mut headers = HeaderMap::new();
                    headers.insert(
                        "x-session-token",
                        HeaderValue::from_str(&format!("token-{attempt}")).unwrap(),
                    );
                    Ok(headers)
                }
            })
            .connect(format!("ws://{proxy_addr}"))
            .await
            .unwrap();

        // Verify initial handshake header.
        let first_handshake =
            tokio::time::timeout(std::time::Duration::from_secs(5), headers_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(first_handshake.get("x-session-token").unwrap(), "token-1");

        // Drain initial Connected event.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(event) = event_rx.recv().await {
                if matches!(event, async_nats::Event::Connected) {
                    break;
                }
            }
        })
        .await
        .unwrap();

        // Force a reconnect and verify the callback is invoked again with a fresh header value.
        client.force_reconnect().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut disconnected = false;
            while let Some(event) = event_rx.recv().await {
                match event {
                    async_nats::Event::Disconnected => disconnected = true,
                    async_nats::Event::Connected if disconnected => break,
                    _ => {}
                }
            }
        })
        .await
        .unwrap();

        let second_handshake =
            tokio::time::timeout(std::time::Duration::from_secs(5), headers_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(second_handshake.get("x-session-token").unwrap(), "token-2");
        assert!(counter.load(Ordering::SeqCst) >= 2);

        let mut sub = client.subscribe("ws.reconnect").await.unwrap();
        client
            .publish("ws.reconnect", "after-reconnect".into())
            .await
            .unwrap();
        assert_eq!(sub.next().await.unwrap().payload, "after-reconnect");
    }

    #[tokio::test]
    async fn websocket_headers_callback_error() {
        let (_server, proxy_addr, _headers_rx) = spawn_ws_server_with_header_inspector().await;

        let err = async_nats::ConnectOptions::new()
            .max_reconnects(0)
            .websocket_headers_callback(|| async {
                Err(async_nats::AuthError::new("failed to fetch ws headers"))
            })
            .connect(format!("ws://{proxy_addr}"))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), async_nats::ConnectErrorKind::Authentication);
    }

    #[tokio::test]
    async fn websocket_disallowed_header_error() {
        use async_nats::http::{header::UPGRADE, HeaderMap, HeaderValue};

        let (_server, proxy_addr, _headers_rx) = spawn_ws_server_with_header_inspector().await;

        let mut headers = HeaderMap::new();
        headers.insert(UPGRADE, HeaderValue::from_static("custom"));

        let err = async_nats::ConnectOptions::new()
            .max_reconnects(0)
            .websocket_headers(headers)
            .connect(format!("ws://{proxy_addr}"))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), async_nats::ConnectErrorKind::Io);
    }

    #[tokio::test]
    async fn websocket_tls_headers_callback() {
        use async_nats::http::{HeaderMap, HeaderValue};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let wss_port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let cfg_path = std::env::temp_dir().join(format!(
            "nats-wss-test-{}-{}.conf",
            std::process::id(),
            wss_port
        ));
        std::fs::write(
            &cfg_path,
            format!(
                "websocket {{\n  port: {wss_port}\n  tls {{\n    cert_file: \"./tests/configs/certs/server-cert.pem\"\n    key_file: \"./tests/configs/certs/server-key.pem\"\n    ca_file: \"./tests/configs/certs/rootCA.pem\"\n  }}\n}}\n"
            ),
        )
        .unwrap();

        let server = nats_server::run_server(cfg_path.to_str().unwrap());
        let _ = server.client_port();
        let _ = std::fs::remove_file(&cfg_path);

        let callback_invocations = Arc::new(AtomicUsize::new(0));
        let callback_invocations_clone = callback_invocations.clone();

        let client = async_nats::ConnectOptions::new()
            .add_root_certificates(path.join("tests/configs/certs/rootCA.pem"))
            .websocket_headers_callback(move || {
                callback_invocations_clone.fetch_add(1, Ordering::SeqCst);
                async move {
                    let mut headers = HeaderMap::new();
                    headers.insert("x-wss-header", HeaderValue::from_static("wss-value"));
                    Ok(headers)
                }
            })
            .connect(format!("wss://localhost:{wss_port}"))
            .await
            .unwrap();

        assert_eq!(callback_invocations.load(Ordering::SeqCst), 1);

        let mut sub = client.subscribe("wss.headers").await.unwrap();
        client
            .publish("wss.headers", "hello-wss".into())
            .await
            .unwrap();
        assert_eq!(sub.next().await.unwrap().payload, "hello-wss");
    }
}
