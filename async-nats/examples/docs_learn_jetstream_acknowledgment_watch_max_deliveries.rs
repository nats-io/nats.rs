use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS.
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Subscribe to the max-deliveries advisory for the shipping consumer.
    // The server publishes here when a message hits the consumer's
    // MaxDeliver limit, which is how you find poison messages.
    let mut advisories = client
        .subscribe("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ORDERS.shipping")
        .await?;

    while let Some(advisory) = advisories.next().await {
        println!("max deliveries: {}", std::str::from_utf8(&advisory.payload)?);
    }
    // NATS-DOC-END

    Ok(())
}
