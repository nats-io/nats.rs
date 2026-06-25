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
    let mut consumer: PullConsumer = stream.get_consumer("orders-reader").await?;

    // Ask the consumer how many messages are still waiting, then fetch exactly
    // that many. This reads everything in order without assuming a count.
    let pending = consumer.info().await?.num_pending;
    if pending == 0 {
        println!("nothing to read");
        return Ok(());
    }

    let mut messages = consumer
        .fetch()
        .max_messages(pending as usize)
        .messages()
        .await?;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        let info = msg.info()?;
        println!(
            "stream {} consumer {}: {}",
            info.stream_sequence,
            info.consumer_sequence,
            std::str::from_utf8(&msg.payload)?
        );
        // Acknowledge so the server records this message as read.
        msg.ack().await?;
    }
    // NATS-DOC-END

    Ok(())
}
