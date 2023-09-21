use async_nats::jetstream::{self, stream};
use futures::TryStreamExt;

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
    // Define the stream configuration, specifying `InterestPolicy` for retention, and
    // create the stream.
    let mut stream = jetstream
        .create_stream(jetstream::stream::Config {
            name: "EVENTS".to_string(),
            retention: stream::RetentionPolicy::Interest,
            subjects: vec!["events.>".to_string()],
            ..Default::default()
        })
        .await?;
    println!("created the stream");

    // To demonstrate the base case behavior of the stream without any consumers, we
    // will publish a few messages to the stream.
    jetstream
        .publish("events.page_loaded".into(), "".into())
        // First, await message to be published.
        .await?
        // Second, await the acknowledgement.
        .await?;
    jetstream
        .publish("events.mouse_clicke".into(), "".into())
        .await?
        .await?;
    let ack = jetstream
        .publish("events.input_focused".into(), "".into())
        .await?
        .await?;

    // We confirm that all three messages were published and the last message sequence
    // is 3.
    println!("last message sequence: {}", ack.sequence);

    // Checking out the stream info, notice how zero messages are present in
    // the stream, but the `last_seq` is 3 which matches the last ack'ed
    // publish sequence above. Also notice that the `first_seq` is one greater
    // which behaves as a sentinel value indicating the stream is empty. This
    // sequence has not been assigned to a message yet, but can be interpreted
    // as _no messages available_ in this context.
    println!("# Stream info without any consumers");
    println!("{:#?}", stream.info().await?);

    // ### Adding a consumer
    // Now let's add a consumer and publish a few more messages. Note that we are _only_ creating the
    // consumer and have not yet bound a subscription to actually receive messages. This
    // is only to point out that a subscription is not _required_ to show _interest_, but
    // it is the presence of a consumer which the stream cares about to determine retention
    // of messages.
    let first_consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("processor-1".to_string()),
            filter_subject: "events.>".to_string(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    jetstream
        .publish("events.mouse_clicked".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.input_focused".into(), "".into())
        .await?
        .await?;

    // If we inspect the stream info again, we will notice a few differences.
    // It shows two messages (which we expect) and the first and last sequences
    // corresponding to the two messages we just published. We also see that
    // the `consumer_count` is now one.
    println!("\n# Stream info with one consumer");
    println!("{:#?}", stream.info().await?);

    // Now that the consumer is there and showing _interest_ in the messages, we know they
    // will remain until we process the messages. Let's fetch two messages and ack them.
    let mut messages = first_consumer.fetch().max_messages(2).messages().await?;
    while let Some(message) = messages.try_next().await? {
        message.double_ack().await?;
    }

    // What do we expect in the stream? No messages and the `first_seq` has been set to
    // the _next_ sequence number like in the base case.
    // ☝️ As a quick aside on ack, We are using `double_ack` here for this
    // example to ensure the stream state has been synced up for this subsequent
    // retrieval.
    println!("\n# Stream info with one consumer and acked messages");
    println!("{:#?}", stream.info().await?);

    // ### Two or more consumers
    // Since each consumer represents a separate _view_ over a stream, we would expect
    // that if messages were processed by one consumer, but not the other, the messages
    // would be retained. This is indeed the case.
    let second_consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("processor-2".to_string()),
            filter_subject: "events.>".to_string(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    jetstream
        .publish("events.input_focused".into(), "".into())
        .await?
        .await?;
    jetstream
        .publish("events.mouse_clicked".into(), "".into())
        .await?
        .await?;

    // Here we bind a subscription for `processor-2`, followed by a fetch and ack. There are
    // two observations to make here. First the fetched messages are the latest two messages
    // that were published just above and not any prior messages since these were already
    // deleted from the stream. This should be apparent now, but this reinforces that a _late_
    // consumer cannot retroactively show interest.
    // The second point is that the stream info shows that the latest two messages are still
    // present in the stream. This is also expected since the first consumer had not yet
    // processed them.
    let messages = second_consumer
        .fetch()
        .max_messages(2)
        .messages()
        .await?
        .try_collect::<Vec<jetstream::Message>>()
        .await?;

    let message1 = messages.get(0).unwrap();
    let message2 = messages.get(1).unwrap();

    println!(
        "msg seqs {} and {}",
        message1.info().unwrap().stream_sequence,
        message2.info().unwrap().stream_sequence
    );

    message1.ack().await?;
    message2.double_ack().await?;

    println!("Stream info with two consumers, but only one set of acked messages");
    println!("{:#?}", stream.info().await?);

    first_consumer
        .fetch()
        .max_messages(2)
        .messages()
        .await?
        .try_for_each(|message| async move { message.double_ack().await })
        .await?;

    println!("Stream info with two consumers having both acked");
    println!("{:#?}", stream.info().await?);

    // A final callout is that _interest_ respects the `FilterSubject` on a consumer.
    // For example, if a consumer defines a filter only for `events.mouse_clicked` events
    // then it won't be considered _interested_ in events such as `events.input_focused`.
    stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("processor-3".to_string()),
            filter_subject: "events.mouse_clicked".to_string(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    jetstream
        .publish("events.mouse_clicked".into(), "".into())
        .await?
        .await?;

    first_consumer
        // Fetch 1 message.
        .fetch()
        .max_messages(1)
        .messages()
        .await?
        // Retrieve it from iterator.
        .try_next()
        .await?
        .expect("should have a message")
        // Terminating message also works.
        .ack_with(jetstream::AckKind::Term)
        .await?;

    second_consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await?
        .try_next()
        .await?
        .expect("should have message")
        .double_ack()
        .await?;

    println!(
        "Stream info with three consumers with interest from two\n{:#?}",
        stream.info().await?
    );

    Ok(())
}
