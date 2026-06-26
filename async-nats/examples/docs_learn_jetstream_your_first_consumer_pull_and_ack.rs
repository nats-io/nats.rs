use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Bind to the existing durable consumer.
    let stream = js.get_stream("ORDERS").await?;
    let consumer: PullConsumer = stream.get_consumer("shipping").await?;

    // Fetch a single message, process it, then acknowledge it.
    let mut messages = consumer.fetch().max_messages(1).messages().await?;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!(
            "{}: {}",
            msg.subject,
            std::str::from_utf8(&msg.payload)?
        );
        // Acknowledge so the server records this message as handled.
        msg.ack().await?;
    }
    // NATS-DOC-END

    Ok(())
}
