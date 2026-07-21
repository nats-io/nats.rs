use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    let payload = r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}"#;

    // NATS-DOC-START
    // Publish, then await the ack. The `?` turns a failed store into an error
    // instead of letting the program continue as if the message was saved.
    let ack = js.publish("orders.created", payload.into()).await?.await?;

    // Reaching this line means the server persisted the message.
    println!(
        "Confirmed stored in {} at sequence {}",
        ack.stream, ack.sequence
    );
    // NATS-DOC-END

    Ok(())
}
