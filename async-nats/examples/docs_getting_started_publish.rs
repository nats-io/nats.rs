use async_nats;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS
    let client = async_nats::connect("localhost:4222").await?;
    println!("Connected to NATS");

    // NATS-DOC-START
    // Publish messages
    client.publish("hello", "Hello NATS!".into()).await?;
    client
        .publish("hello", "Welcome to messaging".into())
        .await?;
    // NATS-DOC-END

    println!("Messages published");

    // Flush to ensure messages are sent
    client.flush().await?;

    Ok(())
}
