use async_nats::jetstream::{self, stream::StorageType};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // Start clean so re-runs are deterministic.
    js.delete_stream("ORDERS").await.ok();

    // NATS-DOC-START
    // Create the ORDERS stream, capturing every subject under `orders.`.
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "ORDERS".to_string(),
            subjects: vec!["orders.>".to_string()],
            storage: StorageType::File,
            ..Default::default()
        })
        .await?;

    // Confirm success by printing the name the server assigned.
    println!("Created stream: {}", stream.cached_info().config.name);
    // NATS-DOC-END

    Ok(())
}
