use async_nats::jetstream;
use async_nats::jetstream::consumer;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Ask for an ordered consumer over the stream. There's no name to manage and
    // no ack to send: the library runs the consumer for you and recreates it if
    // it ever misses a message, so you read every order in stream order.
    let stream = js.get_stream("ORDERS").await?;
    let consumer = stream
        .create_consumer(consumer::pull::OrderedConfig {
            deliver_policy: consumer::DeliverPolicy::All,
            ..Default::default()
        })
        .await?;

    // Read the whole log once, in order, stopping when caught up (pending 0).
    let mut messages = consumer.messages().await?;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("order {}", std::str::from_utf8(&msg.payload)?);
        if msg.info()?.pending == 0 {
            break;
        }
    }
    // NATS-DOC-END

    Ok(())
}
