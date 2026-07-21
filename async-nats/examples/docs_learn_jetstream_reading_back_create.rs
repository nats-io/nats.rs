use async_nats::jetstream;
use async_nats::jetstream::consumer::{pull, AckPolicy, DeliverPolicy};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Create a durable pull consumer that starts at the first stored message
    // and acknowledges each message explicitly, so the server tracks progress.
    let stream = js.get_stream("ORDERS").await?;
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("billing".to_string()),
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    println!("Created durable consumer: {}", consumer.cached_info().name);
    // NATS-DOC-END

    Ok(())
}
