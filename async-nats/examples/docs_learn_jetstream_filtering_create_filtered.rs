use async_nats::jetstream;
use async_nats::jetstream::consumer::{pull, AckPolicy};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Create a durable pull consumer that only sees orders.shipped messages.
    // The filter subject restricts delivery to one subject in the stream.
    let stream = js.get_stream("ORDERS").await?;
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("analytics".to_string()),
            filter_subject: "orders.shipped".to_string(),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    println!("Created filtered consumer: {}", consumer.cached_info().name);

    // Fetch a small batch with a short expiry. Only orders.shipped come back.
    let mut messages = consumer
        .fetch()
        .max_messages(5)
        .expires(std::time::Duration::from_secs(2))
        .messages()
        .await?;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("got: {}", msg.subject);
        msg.ack().await?;
    }
    // NATS-DOC-END

    Ok(())
}
