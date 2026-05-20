#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let nc = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Publish a message to the subject "weather.updates"
    nc.publish("weather.updates", "Temperature: 72°F".into())
        .await?;
    // NATS-DOC-END

    Ok(())
}
