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

    // A fetch on a drained consumer ends empty once the wait elapses, not with
    // an error. Treat "nothing right now" as normal: if no orders came back,
    // wait and fetch again instead of failing.
    let mut messages = consumer
        .fetch()
        .max_messages(10)
        .expires(Duration::from_secs(2))
        .messages()
        .await?;
    let mut count = 0;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("shipping {}", std::str::from_utf8(&msg.payload)?);
        msg.ack().await?;
        count += 1;
    }
    if count == 0 {
        println!("no orders waiting, will retry");
    }
    // NATS-DOC-END

    Ok(())
}
