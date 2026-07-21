use async_nats::jetstream;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Cap ORDERS so it can't grow without bound. Fetch the current config, add
    // a seven-day age limit and a 1 GiB byte ceiling, and update the stream in
    // place. Editing limits leaves the messages already stored alone.
    let mut stream = js.get_stream("ORDERS").await?;
    let mut config = stream.info().await?.config.clone();
    config.max_age = Duration::from_secs(7 * 24 * 3600);
    config.max_bytes = 1 << 30; // 1 GiB
    js.update_stream(config).await?;
    println!("ORDERS capped at 7d age and 1 GiB");
    // NATS-DOC-END

    Ok(())
}
