use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Async publish: the first await only sends the message and returns a
    // PublishAckFuture. Collect the futures without awaiting them, so the round
    // trips overlap. Then await each future to read its ack -- a future that
    // resolves to an error is a failed publish you must re-publish.
    let orders = [
        r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200}"#,
        r#"{"order_id":"ord_2zr9","customer":"globex","total_cents":7800}"#,
        r#"{"order_id":"ord_5t1m","customer":"initech","total_cents":1500}"#,
        r#"{"order_id":"ord_9p3x","customer":"hooli","total_cents":9900}"#,
    ];

    let mut acks = Vec::with_capacity(orders.len());
    for order in orders {
        let future = js.publish("orders.created", order.into()).await?;
        acks.push(future);
    }

    // Now await each future to confirm the order was stored.
    for (i, future) in acks.into_iter().enumerate() {
        match future.await {
            Ok(ack) => println!("order {} stored at sequence {}", i + 1, ack.sequence),
            Err(err) => println!("order {} failed, re-publish it: {}", i + 1, err),
        }
    }
    // NATS-DOC-END

    Ok(())
}
