use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Audit service: catch every order message at any depth. The multi-token
    // wildcard > matches one or more tokens and must be the last token, so
    // orders.> matches orders.created, orders.us.created, and
    // orders.us.west.created alike.
    let mut sub = client.subscribe("orders.>").await?;
    while let Some(msg) = sub.next().await {
        println!("audit: {}", msg.subject);
    }
    // NATS-DOC-END

    Ok(())
}
