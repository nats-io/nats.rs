use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Subscriber 1
    let mut sub1 = client.subscribe("events.data").await?;
    let client1 = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub1.next().await {
            println!(
                "Subscriber 1 received: {}",
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    // Subscriber 2
    let mut sub2 = client.subscribe("events.data").await?;
    tokio::spawn(async move {
        while let Some(msg) = sub2.next().await {
            println!(
                "Subscriber 2 received: {}",
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    // Publisher
    client
        .publish("events.data", "Hello from NATS!".into())
        .await?;
    // Both subscribers receive the message
    // NATS-DOC-END

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
