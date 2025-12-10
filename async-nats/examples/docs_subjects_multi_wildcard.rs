use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Subscribe to all weather updates
    let mut sub = client.subscribe("weather.>").await?;

    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            println!(
                "Received on {}: {}",
                msg.subject,
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    // All these match the subscription
    client
        .publish("weather.us", "US weather update".into())
        .await?;
    client
        .publish("weather.us.east", "East coast update".into())
        .await?;
    client
        .publish("weather.eu.north.finland", "Finland weather".into())
        .await?;
    // NATS-DOC-END

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
