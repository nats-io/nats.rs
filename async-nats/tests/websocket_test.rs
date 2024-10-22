mod websocket {
    use std::path::PathBuf;

    use futures::StreamExt;

    #[tokio::test]
    async fn simple() {
        let _server = nats_server::run_server("tests/configs/ws.conf");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("ws://localhost:8444")
            .await
            .unwrap();

        let mut sub = client.subscribe("foo").await.unwrap();
        client.publish("foo", "hello".into()).await.unwrap();
        assert_eq!(sub.next().await.unwrap().payload, "hello");
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
}
