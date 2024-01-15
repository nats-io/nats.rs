use std::{env, str::from_utf8};

use async_nats::jetstream::{
    self,
    consumer::{OrderedPullConsumer, OrderedPushConsumer, PullConsumer},
    context::PublishAckFuture,
};
use futures::StreamExt;
use tokio::time::Instant;
use async_nats::jetstream::stream::DiscardPolicy;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {

    // Prepare the stream:
    // nats bench benchsubject --js --purge --pub 1 --msgs 10000000 --maxbytes 10000000000
    // nats bench benchsubject --js --sub 1 --msgs 10000000 --maxbytes 10000000000
    let msgs = 10_000_000;

    // Use the NATS_URL env variable if defined, otherwise fallback to the default.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // Create an unauthenticated connection to NATS.
    let client = async_nats::connect(nats_url).await?;

    // Access the JetStream Context for managing streams and consumers as well as for publishing and subscription convenience methods.
    let jetstream = jetstream::new(client);

    let stream_name = String::from("benchstream");

    // Create a stream and a consumer.
    // We can chain the methods.
    // First we create a stream and bind to it.
    let consumer: OrderedPullConsumer = jetstream
        .create_stream(jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec!["benchsubject".into()],
            max_bytes: 10000000000,
            max_messages: -1,
            max_messages_per_subject: -1,
            max_consumers: -1,
            num_replicas: 1,
            discard: DiscardPolicy::New,
            ..Default::default()
        })
        .await?
        // Then, on that `Stream` use method to create Consumer and bind to it too.
        .create_consumer(jetstream::consumer::pull::OrderedConfig {
            ..Default::default()
        })
        .await?;

    println!("pulling messages");
    let now = Instant::now();
    // Attach to the messages iterator for the Consumer.
    // The iterator does its best to optimize retrieval of messages from the server.
    let mut messages = consumer.messages().await?.take(msgs as usize);

    // Iterate over messages.
    while let Some(message) = messages.next().await {
        message.unwrap();
        // let message = message.unwrap();
        // let sequence = message.info().unwrap().stream_sequence;
        // let i = i + 1;
        // assert_eq!(i as u64, sequence);
    }
    println!("pulled {:?} messages in {:?}", msgs, now.elapsed());
    let msgs_per_sec = msgs as f64 / now.elapsed().as_secs_f64();
    println!("msgs/sec: {}", msgs_per_sec);
    Ok(())
}
