use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Add a per-subject ceiling so one noisy subject can't evict another's
    // messages. max_messages_per_subject keeps the most recent N messages for
    // every subject independently, alongside the whole-stream limits.
    let mut stream = js.get_stream("ORDERS").await?;
    let mut config = stream.info().await?.config.clone();
    config.max_messages_per_subject = 100_000;
    js.update_stream(config).await?;
    println!("ORDERS now keeps 100000 messages per subject");
    // NATS-DOC-END

    Ok(())
}
