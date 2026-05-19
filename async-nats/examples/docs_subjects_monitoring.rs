use futures::StreamExt;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Create a wire tap for monitoring
    let mut sub = client.subscribe(">").await?;

    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            println!(
                "[MONITOR] {}: {}",
                msg.subject,
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    // Make sure the subscription is on the server before publishing
    client.flush().await?;

    // Publish to a few subjects so the monitor has something to print
    client.publish("orders.new", "Order 1".into()).await?;
    client
        .publish("sensor.alarm.smoke", "kitchen".into())
        .await?;
    client
        .publish("billing.invoice.paid", "INV-42".into())
        .await?;
    // NATS-DOC-END

    sleep(Duration::from_millis(100)).await;
    Ok(())
}
