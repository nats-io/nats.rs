use std::{env, str::from_utf8};

use async_nats::jetstream::{self, consumer::PullConsumer};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Use the NATS_URL env variable if defined, otherwise fallback to the default.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // Create an unauthenticated connection to NATS.
    let client = async_nats::connect(nats_url).await?;

    // Access the JetStream Context for managing streams and consumers as well as for publishing and subscription convenience methods.
    let jetstream = jetstream::new(client);

    let stream_name = String::from("EVENTS");

    // Create a stream and a consumer.
    // We can chain the methods.
    // First we create a stream and bind to it.
    let consumer: PullConsumer = jetstream
        .create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec!["events.>".into()],
            ..Default::default()
        })
        .await?
        // Then, on that `Stream` use method to create Consumer and bind to it too.
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("consumer".into()),
            ..Default::default()
        })
        .await?;

    // Publish a few messages for the example.
    for i in 0..10 {
        jetstream
            .publish(format!("events.{i}"), "data".into())
            // The first `await` sends the publish
            .await?
            // The second `await` awaits a publish acknowledgement.
            // This can be skipped (for the cost of processing guarantee)
            // or deferred to not block another `publish`
            .await?;
    }

    // Attach to the messages iterator for the Consumer.
    // The iterator does its best to optimize retrieval of messages from the server.
    let mut messages = consumer.messages().await?.take(10);

    // Iterate over messages.
    while let Some(message) = messages.next().await {
        let message = message?;
        println!(
            "got message on subject {} with payload {:?}",
            message.subject,
            from_utf8(&message.payload)?
        );

        // acknowledge the message
        message.ack().await?;
    }

    Ok(())
}
