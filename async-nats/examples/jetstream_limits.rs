use std::{future::IntoFuture, time::Duration};

use async_nats::jetstream::{self, stream::StorageType};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Use the env variable if running in the container, otherwise use the default.
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // Create an unauthenticated connection to NATS.
    let client = async_nats::connect(nats_url).await?;

    // Access `jetstream::context` to use the JS APIs.
    let jetstream = jetstream::new(client);

    // We will declare the initial stream configuration by specifying
    // the name and subjects. Stream names are commonly uppercased to
    // visually differentiate them from subjects, but this is not required.
    // A stream can bind one or more subjects which almost always include
    // wildcards. In addition, no two streams can have overlapping subjects
    // otherwise the primary messages would be persisted twice. There
    // are option to replicate messages in various ways, but that will
    // be explained in later examples.
    let mut stream_config = jetstream::stream::Config {
        name: "EVENTS".to_string(),
        subjects: vec!["events.>".to_string()],
        ..Default::default()
    };

    // JetStream provides both file and in-memory storage options. For
    // durability of the stream data, file storage must be chosen to
    // survive crashes and restarts. This is the default for the stream,
    // but we can still set it explicitly.
    stream_config.storage = StorageType::File;

    // Finally, let's add/create the stream with the default (no) limits.
    // We're cloning the config as we will use it later in the example.
    let mut stream = jetstream.create_stream(stream_config.clone()).await?;
    println!("created the stream");

    // Let's publish a few messages which are received by the stream since
    // they match the subject bound to the stream. The `jetstream.publish()` sends a `request`
    // to the stream and returns ack future that may be awaited at any time.
    // Let's first publish few messages immediately awaiting the ack.

    // Send the publish request.
    jetstream
        .publish("events.page_loaded".into(), "".into())
        .await?
        // And wait for acknowledgement.
        .await?;
    jetstream
        .publish("events.input_blurred".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.mouse_clicked".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.page_loaded".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.mouse_clicked".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.input_focused".into(), "".into())
        .await?
        .await?;
    println!("published 6 messages");

    // Acked can also be processed later.
    // There are many possible patterns in Rust to handle that
    // and the best approach should be matched to given needs.
    // Below example is one of the most straightforward ones.
    // It publishes messages and gathers all acks in a Vec.
    // Then, it awaits all ack futures to complete.

    // Create a vector of ack futures.
    let mut acks = Vec::new();

    // publish the messages and push it's Ack future into to Vec.
    acks.push(
        jetstream
            .publish("events.input_changed".into(), "".into())
            .await?
            .into_future(),
    );
    acks.push(
        jetstream
            .publish("events.input_blurred".into(), "".into())
            .await?
            .into_future(),
    );
    acks.push(
        jetstream
            .publish("events.key_pressed".into(), "".into())
            .await?
            .into_future(),
    );
    acks.push(
        jetstream
            .publish("events.input_focused".into(), "".into())
            .await?
            .into_future(),
    );
    acks.push(
        jetstream
            .publish("events.input_changed".into(), "".into())
            .await?
            .into_future(),
    );
    acks.push(
        jetstream
            .publish("events.input_blurred".into(), "".into())
            .await?
            .into_future(),
    );

    // Await all acks to complete. This methods will fail if any ack will fail.
    match futures::future::try_join_all(acks).await {
        Ok(_acks) => println!("published 6 messages"),
        Err(err) => panic!("failed to ack all messages: {}", err),
    }

    // Checking out the stream info, we can see how many messages we
    // have.
    println!("{:#?}", stream.info().await?);

    // Stream configuration can be dynamically changed. For example,
    // we can set the max messages limit to 10 and it will truncate the
    // two initial events in the stream.
    stream_config.max_messages = 10;
    jetstream.update_stream(stream_config.clone()).await?;
    println!("set max messages to 10");

    // Checking out the info, we see there are now 10 messages and the
    // first sequence and timestamp are based on the third message.
    println!("{:#?}", stream.info().await?);

    // Limits can be combined and whichever one is reached, it will
    // be applied to truncate the stream. For example, let's set a
    // maximum number of bytes for the stream.
    stream_config.max_bytes = 300;
    jetstream.update_stream(stream_config.clone()).await?;
    println!("set max bytes to 300");

    // Inspecting the stream info we now see more messages have been
    // truncated to ensure the size is not exceeded.
    println!("{:#?}", stream.info().await?);

    // Finally, for the last primary limit, we can set the max age.
    stream_config.max_age = Duration::from_secs(1);
    jetstream.update_stream(stream_config.clone()).await?;
    println!("set max age to one second");

    // Looking at the stream info, we still see all the messages...
    println!("{:#?}", stream.info().await?);

    // until a second passes.
    println!("sleeping one second...");
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("{:#?}", stream.info().await?);

    Ok(())
}
