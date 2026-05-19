#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // NATS-DOC-START
    // Connect to NATS demo server
    let client = async_nats::connect("demo.nats.io").await?;

    // Publish a message to the subject "hello"
    client.publish("hello", "Hello NATS!".into()).await?;
    client.flush().await?;

    println!("Message published to hello");
    // NATS-DOC-END

    Ok(())
}
