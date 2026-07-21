#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Publish one order to the orders.created subject. Publishing is
    // fire-and-forget: the call hands the message to the server and returns.
    let order = r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}"#;
    client.publish("orders.created", order.into()).await?;
    // NATS-DOC-END

    // Flush so the buffered publish reaches the server before we exit.
    client.flush().await?;
    Ok(())
}
