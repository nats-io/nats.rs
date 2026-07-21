use async_nats::jetstream;
use std::env;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let client = async_nats::connect(nats_url).await?;
    let js = jetstream::new(client);

    // Get a handle to the ORDERS stream.
    let stream = js.get_stream("ORDERS").await?;

    // NATS-DOC-START
    // Read the message stored at stream sequence 2. This raw-message get always
    // goes to the stream leader.
    let message = stream.get_raw_message(2).await?;

    println!("Subject: {}", message.subject);
    println!("Payload: {}", String::from_utf8_lossy(&message.payload));
    // NATS-DOC-END

    Ok(())
}
