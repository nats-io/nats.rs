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
    // Republish a copy of every stored message onto a second subject so a
    // dashboard can subscribe live without touching the stream. Set
    // `headers_only: true` to republish just the headers (subject, sequence,
    // size) and skip the payload.
    config.republish = Some(async_nats::jetstream::stream::Republish {
        source: "orders.>".to_string(),
        destination: "dash.orders.>".to_string(),
        headers_only: false,
    });
    let info = js.update_stream(&config).await?;

    println!(
        "Republishing to: {}",
        info.config.republish.unwrap().destination
    );
    // NATS-DOC-END

    Ok(())
}
