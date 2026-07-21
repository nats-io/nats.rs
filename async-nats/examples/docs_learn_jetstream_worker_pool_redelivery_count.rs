use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Bind to the durable "shipping" consumer.
    let stream = js.get_stream("ORDERS").await?;
    let consumer: PullConsumer = stream.get_consumer("shipping").await?;

    // Each message carries how many times it has been delivered. A count above
    // one means a redelivery: the server handed this order out before, but a
    // worker crashed or ran past AckWait before acking. Key your side effects
    // by order_id so handling the same order twice is harmless.
    let mut messages = consumer.fetch().max_messages(10).messages().await?;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let delivered = msg.info()?.delivered;
        let payload = std::str::from_utf8(&msg.payload)?;
        if delivered > 1 {
            println!("redelivery #{delivered} of {payload}");
        } else {
            println!("first delivery of {payload}");
        }
        msg.ack().await?;
    }
    // NATS-DOC-END

    Ok(())
}
