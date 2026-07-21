use async_nats::jetstream;
use std::env;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let client = async_nats::connect(nats_url).await?;
    let js = jetstream::new(client);

    // Fetch the current ORDERS config so we keep every existing setting.
    let mut stream = js.get_stream("ORDERS").await?;
    let mut config = stream.info().await?.config.clone();

    // NATS-DOC-START
    // Turn on direct get for the stream, then push the updated config.
    config.allow_direct = true;
    let info = js.update_stream(&config).await?;

    println!("allow_direct is now: {}", info.config.allow_direct);
    // NATS-DOC-END

    Ok(())
}
