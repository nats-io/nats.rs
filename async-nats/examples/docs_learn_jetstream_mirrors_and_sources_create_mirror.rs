use async_nats::jetstream::{self, stream::Source};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // Start clean so re-runs are deterministic.
    js.delete_stream("ORDERS-ARCHIVE").await.ok();

    // NATS-DOC-START
    // Create ORDERS-ARCHIVE as a read-only mirror of ORDERS. A mirror takes
    // no subjects of its own; it follows the upstream stream.
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "ORDERS-ARCHIVE".to_string(),
            mirror: Some(Source {
                name: "ORDERS".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;

    // Confirm: the new stream mirrors ORDERS.
    let cfg = &stream.cached_info().config;
    println!(
        "Created mirror {} of {}",
        cfg.name,
        cfg.mirror.as_ref().unwrap().name
    );
    // NATS-DOC-END

    Ok(())
}
