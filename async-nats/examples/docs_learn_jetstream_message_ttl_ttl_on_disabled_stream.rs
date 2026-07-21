use async_nats::jetstream;
use async_nats::jetstream::stream;
use std::env;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let client = async_nats::connect(nats_url).await?;
    let js = jetstream::new(client);

    // Create a throwaway stream WITHOUT per-message TTLs enabled. `allow_msg_ttl`
    // stays false here (it is off by default).
    js.create_stream(stream::Config {
        name: "ORDERS_NO_TTL".to_string(),
        subjects: vec!["no-ttl.>".to_string()],
        ..Default::default()
    })
    .await?;

    // NATS-DOC-START
    // Try to publish a message carrying a `Nats-TTL` header to a stream that does
    // not allow per-message TTLs. The server refuses the write with error 10166,
    // "per-message TTL is disabled", so nothing is ever stored.
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-TTL", "60s");

    match js
        .publish_with_headers("no-ttl.event", headers, "should be rejected".into())
        .await?
        .await
    {
        Ok(ack) => println!("Unexpected: message stored at sequence {}", ack.sequence),
        Err(err) => {
            println!("Publish rejected, nothing stored: {err}");
            // The fix is to enable TTLs on the stream: create (or update) it with
            // `allow_message_ttl: true`, as in the publish-with-TTL example.
        }
    }
    // NATS-DOC-END

    Ok(())
}
