use async_nats::jetstream;
use async_nats::jetstream::stream::{Config, RetentionPolicy};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // FULFILLMENT is the WorkQueue stream of paid orders awaiting shipment.
    js.get_or_create_stream(Config {
        name: "FULFILLMENT".to_string(),
        subjects: vec!["fulfill.>".to_string()],
        retention: RetentionPolicy::WorkQueue,
        ..Default::default()
    })
    .await?;

    // NATS-DOC-START
    // Retention is fixed once a stream exists. Trying to switch FULFILLMENT from
    // WorkQueue to Limits is rejected by the server (err 10052: stream
    // configuration update can not change retention policy to/from workqueue).
    match js
        .update_stream(Config {
            name: "FULFILLMENT".to_string(),
            subjects: vec!["fulfill.>".to_string()],
            retention: RetentionPolicy::Limits,
            ..Default::default()
        })
        .await
    {
        Ok(_) => println!("unexpected: retention switch was accepted"),
        Err(err) => println!("rejected: {err}"),
    }
    // NATS-DOC-END

    Ok(())
}
