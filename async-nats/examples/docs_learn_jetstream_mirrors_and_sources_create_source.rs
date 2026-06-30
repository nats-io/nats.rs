use async_nats::jetstream::{self, stream::Source};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    js.delete_stream("ALL-ORDERS").await.ok();

    // Setup: the three regional streams ALL-ORDERS aggregates, each with its
    // own subjects.
    for (name, subject) in [
        ("ORDERS-US", "us.orders.>"),
        ("ORDERS-EU", "eu.orders.>"),
        ("ORDERS-APAC", "apac.orders.>"),
    ] {
        js.create_stream(jetstream::stream::Config {
            name: name.to_string(),
            subjects: vec![subject.to_string()],
            ..Default::default()
        })
        .await?;
    }

    // NATS-DOC-START
    // Create ALL-ORDERS as an aggregate that sources the three regional streams
    // into one. Unlike a mirror, a stream can list several sources.
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "ALL-ORDERS".to_string(),
            sources: Some(vec![
                Source {
                    name: "ORDERS-US".to_string(),
                    ..Default::default()
                },
                Source {
                    name: "ORDERS-EU".to_string(),
                    ..Default::default()
                },
                Source {
                    name: "ORDERS-APAC".to_string(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        })
        .await?;

    let cfg = &stream.cached_info().config;
    println!(
        "Created {} sourcing {} streams",
        cfg.name,
        cfg.sources.as_ref().map_or(0, |s| s.len())
    );
    // NATS-DOC-END

    Ok(())
}
