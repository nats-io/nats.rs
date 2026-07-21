use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Join the "packers" queue group on orders.created. Every subscriber that
    // names the same group shares the load: each order is delivered to exactly
    // one member. Run this in several processes to watch the load balance.
    let mut sub = client
        .queue_subscribe("orders.created", "packers".to_string())
        .await?;
    while let Some(msg) = sub.next().await {
        println!("packer handling: {}", String::from_utf8_lossy(&msg.payload));
    }
    // NATS-DOC-END

    Ok(())
}
