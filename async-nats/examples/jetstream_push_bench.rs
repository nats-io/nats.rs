use std::{env, str::from_utf8};

use async_nats::jetstream::{
    self,
    consumer::{OrderedPullConsumer, OrderedPushConsumer, PullConsumer},
    context::PublishAckFuture,
};
use futures::StreamExt;
use tokio::time::Instant;

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
    let consumer: OrderedPushConsumer = jetstream
        .create_stream(jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec!["events".into()],
            ..Default::default()
        })
        .await?
        // Then, on that `Stream` use method to create Consumer and bind to it too.
        .create_consumer(jetstream::consumer::push::OrderedConfig {
            deliver_subject: "delivery".into(),
            ..Default::default()
        })
        .await?;
    let stream = jetstream.get_stream(stream_name).await?;
    if stream.cached_info().state.messages >= 50_000_000 {
        println!("stream already has 50M messages");
    } else {
        println!("publishing messages");
        // Publish a few messages for the example.

        let (tx, mut rx) = tokio::sync::mpsc::channel(10000);
        tokio::spawn(async move {
            for _ in 0..50_000_000 {
                let ack = jetstream
                    .publish(format!("events"), "foo".into())
                    // The first `await` sends the publish
                    .await
                    .unwrap();
                tx.send(ack).await.unwrap();
            }
        });
        while let Some(ack) = rx.recv().await {
            ack.await.unwrap();
        }
    }

    println!("pulling messages");
    let now = Instant::now();
    // Attach to the messages iterator for the Consumer.
    // The iterator does its best to optimize retrieval of messages from the server.
    let mut messages = consumer.messages().await?.take(50_000_000);

    // Iterate over messages.
    while let Some(message) = messages.next().await {
        message.unwrap();
        // let message = message.unwrap();
        // let sequence = message.info().unwrap().stream_sequence;
        // let i = i + 1;
        // assert_eq!(i as u64, sequence);
    }
    println!("pulled 50M messages in {:?}", now.elapsed());
    let msgs_per_sec = 50_000_000.0 / now.elapsed().as_secs_f64();
    println!("msgs/sec: {}", msgs_per_sec);
    Ok(())
}
