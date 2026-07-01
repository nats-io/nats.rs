use async_nats::jetstream;
use async_nats::jetstream::stream;
use std::env;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let client = async_nats::connect(nats_url).await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Create a stream that rewrites each incoming subject as it is stored. Every
    // message published to `ingest.*` is transformed into
    // `orders.<bucket>.<original-token>`, where `partition(3, 1)` hashes the first
    // wildcard token into one of three buckets (0, 1, or 2). This spreads orders
    // across three shard subjects so consumers can filter by bucket.
    let stream = js
        .create_stream(stream::Config {
            name: "ORDERS-SHARDED".to_string(),
            subjects: vec!["ingest.*".to_string()],
            subject_transform: Some(stream::SubjectTransform {
                source: "ingest.*".to_string(),
                destination: "orders.{{partition(3,1)}}.{{wildcard(1)}}".to_string(),
            }),
            ..Default::default()
        })
        .await?;

    println!("Created stream: {}", stream.cached_info().config.name);
    // NATS-DOC-END

    Ok(())
}
