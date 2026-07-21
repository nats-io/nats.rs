use async_nats::jetstream;
use async_nats::jetstream::consumer::{pull, AckPolicy};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Create a durable pull consumer whose filter has a typo: "orders.shiped"
    // matches no stored subject, so the consumer is valid but empty.
    let stream = js.get_stream("ORDERS").await?;
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("analytics-typo".to_string()),
            filter_subject: "orders.shiped".to_string(),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    // The fetch waits up to the expiry, then returns with no messages and no
    // error. A wrong filter fails silently: nothing matches, nothing arrives.
    let mut messages = consumer
        .fetch()
        .max_messages(5)
        .expires(std::time::Duration::from_secs(2))
        .messages()
        .await?;
    let mut count = 0;
    while let Some(msg) = messages.next().await {
        msg?;
        count += 1;
    }
    if count == 0 {
        println!("Pull returned nothing: the filter matched no stored subject.");
    }
    // NATS-DOC-END

    Ok(())
}
