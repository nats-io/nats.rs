use async_nats::jetstream::{self, consumer::PushConsumer};
use futures_util::StreamExt;
use std::{env, str::from_utf8, time::Duration};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Use the NATS_URL env variable if defined, otherwise fallback to the default.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // Create an unauthenticated connection to NATS.
    let client = async_nats::connect(nats_url).await?;

    let inbox = client.new_inbox();

    // Access the JetStream Context for managing streams and consumers as well as for publishing and subscription convenience methods.
    let jetstream = jetstream::new(client);

    let stream_name = String::from("EVENTS");

    // Create a stream and a consumer.
    // We can chain the methods.
    // First we create a stream and bind to it.
    let consumer: PushConsumer = jetstream
        .create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec!["events.>".to_string()],
            ..Default::default()
        })
        .await?
        // Then, on that `Stream` use method to create Consumer and bind to it too.
        .create_consumer(jetstream::consumer::push::Config {
            deliver_subject: inbox.clone(),
            inactive_threshold: Duration::from_secs(60),
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
