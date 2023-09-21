use async_nats::jetstream::{self, stream};
use futures::{StreamExt, TryStreamExt};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Use the env variable if running in the container, otherwise use the default.
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // Create an unauthenticated connection to NATS.
    let client = async_nats::connect(nats_url).await?;

    // Access `jetstream::context` to use the JS APIs.
    let jetstream = jetstream::new(client);

    // ### Creating the stream
    // Define the stream configuration, specifying `WorkQueuePolicy` for
    // retention, and create the stream.
    let mut stream = jetstream
        .create_stream(jetstream::stream::Config {
            name: "EVENTS".to_string(),
            retention: stream::RetentionPolicy::WorkQueue,
            subjects: vec!["events.>".to_string()],
            ..Default::default()
        })
        .await?;
    println!("created the stream");

    // ### Queue messages
    // Publish a few messages.
    jetstream
        .publish("events.us.page_loaded".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.eu.mouse_clicked".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.us.input_focused".into(), "".into())
        .await?
        .await?;
    println!("published 3 messages");

    // Checking the stream info, we see three messages have been queued.
    println!(
        "Stream info without any consumers: {:#?}",
        stream.info().await?
    );

    // ### Adding a consumer
    // Now let's add a consumer and publish a few more messages.
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("processor-1".to_string()),
            ..Default::default()
        })
        .await?;

    // Fetch and ack the queued messages.
    let mut messages = consumer.fetch().max_messages(3).messages().await?;
    while let Some(message) = messages.next().await {
        message?.ack().await?;
    }

    // Checking the stream info again, we will notice no messages
    // are available.
    println!("Stream info with one consumer: {:#?}", stream.info().await?);

    // ### Exclusive non-filtered consumer
    // As noted in the description above, work-queue streams can only have
    // at most one consumer with interest on a subject at any given time.
    // Since the pull consumer above is not filtered, if we try to create
    // another one, it will fail.
    let err = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("processor-2".to_string()),
            ..Default::default()
        })
        .await
        .expect_err("fail to create overlapping consumer");
    println!("Create an overlapping consumer: {}", err);

    // However if we delete the first one, we can then add the new one.
    stream.delete_consumer("processor-1").await?;
    let result = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("processor-2".to_string()),
            ..Default::default()
        })
        .await;
    println!("created the new consumer? {}", result.is_ok());
    stream.delete_consumer("processor-2").await?;

    // ### Multiple filtered consumers
    // To create multiple consumers, a subject filter needs to be applied.
    // For this example, we could scope each consumer to the geo that the
    // event was published from, in this case `us` or `eu`.
    let us_consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("processor-us".to_string()),
            filter_subject: "events.us.>".to_string(),
            ..Default::default()
        })
        .await?;

    let eu_consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("processor-eu".to_string()),
            filter_subject: "events.eu.>".to_string(),
            ..Default::default()
        })
        .await?;

    jetstream
        .publish("events.eu.mouse_clicked".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.us.page_loaded".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.us.input_focused".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.eu.page_loaded".into(), "".into())
        .await?
        .await?;
    println!("published 4 messages");

    let mut us_messages = us_consumer.fetch().max_messages(2).messages().await?;
    let mut eu_messages = eu_consumer.fetch().max_messages(2).messages().await?;

    while let Some(message) = us_messages.try_next().await? {
        println!("us consumer got: {}", message.subject);
        message.ack().await?;
    }
    while let Some(message) = eu_messages.try_next().await? {
        println!("eu consumer got: {}", message.subject);
        message.ack().await?;
    }

    Ok(())
}
