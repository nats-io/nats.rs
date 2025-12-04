use async_nats;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS demo server
    let client = async_nats::connect("demo.nats.io").await?;

    // Publish a message
    client.publish("hello", "Hello NATS!".into()).await?;
    client.flush().await?;

    println!("Message published to hello");

    Ok(())
}
