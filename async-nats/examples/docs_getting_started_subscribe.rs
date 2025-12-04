use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS demo server
    let client = async_nats::connect("demo.nats.io").await?;

    // Subscribe to 'hello'
    let mut subscriber = client.subscribe("hello").await?;
    println!("Listening for messages on 'hello'...");

    // Process messages
    while let Some(msg) = subscriber.next().await {
        println!("Received: {}", String::from_utf8_lossy(&msg.payload));
    }

    Ok(())
}
