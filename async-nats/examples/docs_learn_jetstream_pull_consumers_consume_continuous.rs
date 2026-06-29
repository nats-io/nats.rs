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

    // messages() sets up a continuous flow: the library keeps pull requests
    // open and yields each order as soon as it lands in the stream. It runs
    // until you stop it, no fetch loop to write by hand.
    let mut messages = consumer.messages().await?;
    while let Some(msg) = messages.next().await {
        let msg = msg?;
        println!("shipping {}", std::str::from_utf8(&msg.payload)?);
        msg.ack().await?;
    }
    // NATS-DOC-END

    Ok(())
}
