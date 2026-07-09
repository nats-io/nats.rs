use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // A shipping-quote provider. Subscribe plainly to shipping.quote (NOT in a
    // queue group, so every provider sees each request) and reply with a price.
    // Run several copies, each quoting a different number.
    let mut sub = client.subscribe("shipping.quote").await?;
    while let Some(msg) = sub.next().await {
        if let Some(reply) = msg.reply {
            client
                .publish(
                    reply,
                    r#"{"carrier":"carrier-a","quote_cents":1500}"#.into(),
                )
                .await?;
        }
    }
    // NATS-DOC-END

    Ok(())
}
