use async_nats::jetstream;
use async_nats::jetstream::consumer;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Raise max_ack_pending so a larger pool can hold more orders in progress
    // at once. The cap is shared across the whole "shipping" consumer, not per
    // worker, so size it to at least your worker count. create_consumer updates
    // the existing consumer in place.
    let stream = js.get_stream("ORDERS").await?;
    stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some("shipping".to_string()),
            ack_policy: consumer::AckPolicy::Explicit,
            max_ack_pending: 5000,
            ..Default::default()
        })
        .await?;
    println!("shipping max_ack_pending set to 5000");
    // NATS-DOC-END

    Ok(())
}
