use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Regional analytics: one subscription catches created orders from every
    // region. The single-token wildcard * matches exactly one token, so both
    // orders.us.created and orders.eu.created match, while orders.created and
    // orders.us.west.created do not.
    let mut sub = client.subscribe("orders.*.created").await?;
    while let Some(msg) = sub.next().await {
        println!("analytics: new order on {}", msg.subject);
    }
    // NATS-DOC-END

    Ok(())
}
