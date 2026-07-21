use async_nats::jetstream;
use async_nats::jetstream::consumer::{pull, AckPolicy};
use async_nats::jetstream::stream::{Config, RetentionPolicy};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and open a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // FULFILLMENT is the queue of paid orders waiting to ship, separate from the
    // ORDERS record. WorkQueue retention means each message is delivered to one
    // consumer, and the first ack removes it for everyone, draining the stream.
    let mut stream = js
        .create_stream(Config {
            name: "FULFILLMENT".to_string(),
            subjects: vec!["fulfill.>".to_string()],
            retention: RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await?;
    println!("retention is {:?}", stream.cached_info().config.retention);

    // Queue one paid order for a shipper to pick up.
    js.publish(
        "fulfill.us",
        r#"{"order_id":"ord_8w2k","customer":"acme-co"}"#.into(),
    )
    .await?
    .await?;

    // Shipping workers drain the queue through a durable pull consumer that
    // acknowledges each task explicitly.
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("shippers".to_string()),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    // Take one task and ack it once the order is shipped.
    let mut messages = consumer.fetch().max_messages(1).messages().await?;
    while let Some(message) = messages.next().await {
        let message = message?;
        println!("shipping {}", std::str::from_utf8(&message.payload)?);
        message.ack().await?;
    }

    // The ack removed the task from the WorkQueue stream, so the count is now 0.
    // A Limits stream like ORDERS would still hold the message after ack.
    let count = stream.info().await?.state.messages;
    println!("messages left in FULFILLMENT: {count}");
    // NATS-DOC-END

    Ok(())
}
