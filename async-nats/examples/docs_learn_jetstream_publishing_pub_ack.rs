use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    let payload = r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}"#;

    // NATS-DOC-START
    // Publish one order and read the fields the server returns in the ack.
    let ack = js.publish("orders.created", payload.into()).await?.await?;

    println!("Stream:    {}", ack.stream);
    println!("Sequence:  {}", ack.sequence);
    println!("Duplicate: {}", ack.duplicate);
    // NATS-DOC-END

    Ok(())
}
