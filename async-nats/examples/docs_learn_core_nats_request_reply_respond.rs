use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // The inventory service: subscribe to orders.inventory.check and answer
    // every request by publishing back to the reply subject it carries.
    let mut sub = client.subscribe("orders.inventory.check").await?;
    while let Some(msg) = sub.next().await {
        if let Some(reply) = msg.reply {
            client
                .publish(reply, r#"{"in_stock":true,"warehouse":"us-east"}"#.into())
                .await?;
        }
    }
    // NATS-DOC-END

    Ok(())
}
