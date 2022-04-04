#[cfg(test)]
mod client {
    #[tokio::test]
    async fn subscribe() {
        use bytes::Bytes;
        use futures_util::StreamExt;

        let mut client = nats_experimental::connect("localhost:4222").await.unwrap();
        let mut subscriber = client.subscribe("foo".into()).await.unwrap();

        for i in 0..100 {
            let payload: Bytes = i.to_string().into();
            client.publish("foo".into(), payload).await.unwrap();
        }

        // TODO we need flush!

        for i in 0..100 {
            let payload: Bytes = i.to_string().into();
            let message = subscriber.next().await.unwrap();
            assert_eq!(message.payload, payload);
        }
    }
}
