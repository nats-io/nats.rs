use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;
use futures::StreamExt;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Bind to the durable "shipping" consumer.
    let stream = js.get_stream("ORDERS").await?;
    let consumer: PullConsumer = stream.get_consumer("shipping").await?;

    // Fetch a batch of up to 10 orders, waiting up to 2 seconds for them. The
    // call returns when the batch is full or the wait elapses, whichever comes
    // first. Process and ack each, then fetch again to keep going.
    let mut messages = consumer
        .fetch()
        .max_messages(10)
        .expires(Duration::from_secs(2))
        .messages()
        .await?;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("shipping {}", std::str::from_utf8(&msg.payload)?);
        msg.ack().await?;
    }
    // NATS-DOC-END

    Ok(())
}
