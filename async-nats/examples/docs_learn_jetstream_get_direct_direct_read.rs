use async_nats::jetstream;
use std::env;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let client = async_nats::connect(nats_url).await?;
    let js = jetstream::new(client);

    // Get a handle to the ORDERS stream. Direct get needs `allow_direct: true`.
    let stream = js.get_stream("ORDERS").await?;

    // NATS-DOC-START
    // Read the message at stream sequence 1 with the Direct Get API. This can be
    // served by any replica, not just the leader.
    let message = stream.direct_get(1).await?;

    println!("Subject: {}", message.subject);
    println!("Payload: {}", String::from_utf8_lossy(&message.payload));
    // NATS-DOC-END

    Ok(())
}
