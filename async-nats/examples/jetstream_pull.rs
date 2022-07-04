use std::str::from_utf8;

use async_nats::jetstream::consumer::PullConsumer;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client);

    let consumer: PullConsumer = jetstream
        .create_stream("max")
        .await?
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some("max".to_string()),
            ..Default::default()
        })
        .await?;

    for _ in 0..10 {
        jetstream.publish("max".to_string(), "max".into()).await?;
    }

    let mut messages = consumer.stream().await?.take(10);

    while let Some(message) = messages.next().await {
        let message = message?;
        println!("got message: {:?}", message);
        println!("paylaod: {:?}", from_utf8(&message.payload));
    }

    Ok(())
}
