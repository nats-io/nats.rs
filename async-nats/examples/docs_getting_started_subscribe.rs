use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS
    let client = async_nats::connect("localhost:4222").await?;
    println!("Connected to NATS");

    // NATS-DOC-START
    // Subscribe to 'hello'
    let mut subscriber = client.subscribe("hello").await?;
    // NATS-DOC-END

    println!("Waiting for messages...");

    // Process messages
    while let Some(msg) = subscriber.next().await {
        println!("Received: {}", String::from_utf8_lossy(&msg.payload));
    }

    Ok(())
}
