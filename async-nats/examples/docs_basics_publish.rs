#[tokio::main]
async fn main() {
    let nc = async_nats::connect("nats://localhost:4222").await.unwrap();

    // NATS-DOC-START
    // Publish a message to the subject "weather.updates"
    nc.publish("weather.updates", "Temperature: 72°F".into())
        .await
        .unwrap();
    // NATS-DOC-END
}
