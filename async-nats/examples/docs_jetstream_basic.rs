use async_nats::jetstream::{self, consumer::PullConsumer, stream::StorageType};
use futures_util::StreamExt;
use std::str::from_utf8;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let nc = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(nc);

    // NATS-DOC-START
    // Create a stream that captures any subject under `orders.`
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "ORDERS".to_string(),
            subjects: vec!["orders.>".into()],
            storage: StorageType::File,
            ..Default::default()
        })
        .await?;

    // Publish a few orders
    js.publish("orders.new", "Order #1001".into()).await?;
    js.publish("orders.new", "Order #1002".into()).await?;
    js.publish("orders.shipped", "Order #1001 shipped".into())
        .await?;

    // Create a durable pull consumer that delivers from the beginning
    let consumer: PullConsumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("order-processor".to_string()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    // Fetch a batch and acknowledge each message
    let mut messages = consumer.fetch().max_messages(3).messages().await?;
    while let Some(message) = messages.next().await {
        let message = message?;
        println!(
            "Received on {}: {}",
            message.subject,
            from_utf8(&message.payload)?
        );
        message.ack().await?;
    }
    // NATS-DOC-END

    Ok(())
}
