use async_nats;
use futures::StreamExt;

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
    // NATS-DOC-END

    Ok(())
}
