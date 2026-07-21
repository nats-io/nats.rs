use async_nats::jetstream;
use async_nats::jetstream::stream::DiscardPolicy;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Switch ORDERS to Discard New. Discard New never drops messages already
    // stored, so capping it at one message leaves the existing orders in place
    // and puts the stream instantly over its limit. The next publish is
    // rejected rather than evicting an older order.
    let mut stream = js.get_stream("ORDERS").await?;
    let mut config = stream.info().await?.config.clone();
    config.discard = DiscardPolicy::New;
    config.max_messages = 1;
    js.update_stream(config.clone()).await?;

    // This publish hits the full stream; the ack returns an error instead of
    // the publish succeeding silently. Handle it in the publisher.
    let ack = js
        .publish("orders.created", r#"{"order_id":"ord_8w2k"}"#.into())
        .await?;
    if let Err(err) = ack.await {
        println!("publish rejected: {err}");
    }

    // Put ORDERS back: Discard Old, no message cap (age and byte limits stay).
    config.discard = DiscardPolicy::Old;
    config.max_messages = -1;
    js.update_stream(config).await?;
    // NATS-DOC-END

    Ok(())
}
