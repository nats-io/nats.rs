use async_nats::jetstream;
use async_nats::jetstream::stream;
use std::env;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let client = async_nats::connect(nats_url).await?;
    let js = jetstream::new(client);

    // Create ORDERS with per-message TTLs enabled. The `allow_message_ttl` field
    // maps to the stream's `allow_msg_ttl` setting; without it the server rejects
    // any message that carries a `Nats-TTL` header.
    js.create_stream(stream::Config {
        name: "ORDERS".to_string(),
        subjects: vec!["orders.>".to_string()],
        allow_message_ttl: true,
        ..Default::default()
    })
    .await?;

    // NATS-DOC-START
    // Publish a cancellation that carries its own time-to-live. The `Nats-TTL`
    // header tells the server to delete this one message 60 seconds after it is
    // stored, even if the rest of the stream never expires. "60s" is a duration
    // string; the server also accepts an integer number of seconds (minimum 1s).
    // (A typed shortcut also exists: `PublishMessage::build().ttl(Duration)`.)
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-TTL", "60s");

    let ack = js
        .publish_with_headers("orders.cancelled", headers, "order 4242 cancelled".into())
        .await?
        .await?;

    println!(
        "Stored on stream {} at sequence {}",
        ack.stream, ack.sequence
    );
    // The message is live now and is removed 60 seconds after it was stored.
    // NATS-DOC-END

    Ok(())
}
