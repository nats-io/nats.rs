use async_nats::jetstream::{self, message::PublishMessage};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    let payload = r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}"#;

    // NATS-DOC-START
    // Publish the same order twice, tagging both with the same message id.
    // The server stores the first and detects the second as a duplicate.
    let first = js
        .send_publish(
            "orders.created",
            PublishMessage::build()
                .payload(payload.into())
                .message_id("ord_8w2k-created"),
        )
        .await?
        .await?;
    println!(
        "First:  sequence {}, duplicate {}",
        first.sequence, first.duplicate
    );

    let second = js
        .send_publish(
            "orders.created",
            PublishMessage::build()
                .payload(payload.into())
                .message_id("ord_8w2k-created"),
        )
        .await?
        .await?;
    println!(
        "Second: sequence {}, duplicate {}",
        second.sequence, second.duplicate
    );
    // NATS-DOC-END

    Ok(())
}
