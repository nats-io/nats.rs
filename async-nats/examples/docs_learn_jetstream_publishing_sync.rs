use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Publish three orders into the ORDERS stream, reading each ack as it returns.
    let messages = [
        (
            "orders.created",
            r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}"#,
        ),
        (
            "orders.created",
            r#"{"order_id":"ord_2zr9","customer":"globex","total_cents":7800,"ts":"2026-05-22T10:14:25Z"}"#,
        ),
        (
            "orders.shipped",
            r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:31Z"}"#,
        ),
    ];

    for (subject, payload) in messages {
        // The first await sends the message; the second waits for the server ack.
        let ack = js.publish(subject, payload.into()).await?.await?;
        println!("Stored in {} at sequence {}", ack.stream, ack.sequence);
    }
    // NATS-DOC-END

    Ok(())
}
