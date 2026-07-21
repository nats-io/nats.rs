use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Subscribe as the warehouse service to orders.created. Each matching
    // message is delivered to this subscription as it is published.
    let mut sub = client.subscribe("orders.created").await?;
    while let Some(msg) = sub.next().await {
        println!(
            "warehouse received: {}",
            String::from_utf8_lossy(&msg.payload)
        );
    }
    // NATS-DOC-END

    Ok(())
}
