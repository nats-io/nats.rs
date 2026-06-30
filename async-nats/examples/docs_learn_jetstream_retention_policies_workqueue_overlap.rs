use async_nats::jetstream;
use async_nats::jetstream::consumer::{pull, AckPolicy};
use async_nats::jetstream::stream::{Config, RetentionPolicy};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // FULFILLMENT is the WorkQueue stream of paid orders awaiting shipment.
    let stream = js
        .get_or_create_stream(Config {
            name: "FULFILLMENT".to_string(),
            subjects: vec!["fulfill.>".to_string()],
            retention: RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await?;

    // NATS-DOC-START
    // One unfiltered consumer claims every order on the queue. That is allowed.
    stream
        .create_consumer(pull::Config {
            durable_name: Some("shippers".to_string()),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    // A WorkQueue stream gives each message to exactly one consumer, so two
    // unfiltered consumers would both claim the same orders. The server rejects
    // the second one (err 10099: multiple non-filtered consumers not allowed on
    // workqueue stream).
    match stream
        .create_consumer(pull::Config {
            durable_name: Some("eu-shippers".to_string()),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await
    {
        Ok(_) => println!("unexpected: second unfiltered consumer was accepted"),
        Err(err) => println!("rejected: {err}"),
    }

    // Split the work by subject instead. Drop the unfiltered consumer, then add
    // one consumer per region. Their filters do not overlap, so both succeed.
    stream.delete_consumer("shippers").await?;

    stream
        .create_consumer(pull::Config {
            durable_name: Some("us-shippers".to_string()),
            filter_subject: "fulfill.us".to_string(),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;
    stream
        .create_consumer(pull::Config {
            durable_name: Some("eu-shippers".to_string()),
            filter_subject: "fulfill.eu".to_string(),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;
    println!("us-shippers and eu-shippers created");
    // NATS-DOC-END

    Ok(())
}
