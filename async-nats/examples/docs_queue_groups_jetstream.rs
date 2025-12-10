use async_nats::jetstream;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = jetstream::new(client);

    // NATS-DOC-START
    // Create work queue stream
    jetstream
        .create_stream(jetstream::stream::Config {
            name: "WORK_QUEUE".to_string(),
            subjects: vec!["work.tasks".to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            storage: jetstream::stream::StorageType::File,
            num_replicas: 3,
            ..Default::default()
        })
        .await?;

    // Create consumer for workers
    let stream = jetstream.get_stream("WORK_QUEUE").await?;
    stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("WORKERS".to_string()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            max_deliver: 3,
            ..Default::default()
        })
        .await?;

    // Worker processing loop
    let consumer: jetstream::consumer::PullConsumer = stream.get_consumer("WORKERS").await?;

    loop {
        let mut messages = consumer.fetch().max_messages(1).messages().await?;

        while let Some(Ok(msg)) = messages.next().await {
            println!("Processing task: {}", String::from_utf8_lossy(&msg.payload));
            process_task(&msg.payload);
            msg.ack().await?;
        }
    }
    // NATS-DOC-END
}

fn process_task(_data: &[u8]) {
    // Process task implementation
}
