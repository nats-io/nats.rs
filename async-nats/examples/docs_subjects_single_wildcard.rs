use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Subscribe with single token wildcard
    let mut sub = client.subscribe("weather.*.east").await?;

    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            println!(
                "Received on {}: {}",
                msg.subject,
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    // Publish to specific subjects
    client
        .publish("weather.us.east", "Temperature: 72F".into())
        .await?;
    client
        .publish("weather.eu.east", "Temperature: 18C".into())
        .await?;
    // NATS-DOC-END

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
