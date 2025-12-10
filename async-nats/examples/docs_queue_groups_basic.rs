use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Create three workers in the same queue group
    let mut worker_a = client
        .queue_subscribe("orders.new", "workers".to_string())
        .await?;

    let mut worker_b = client
        .queue_subscribe("orders.new", "workers".to_string())
        .await?;

    let mut worker_c = client
        .queue_subscribe("orders.new", "workers".to_string())
        .await?;

    // Spawn tasks to process messages
    tokio::spawn(async move {
        while let Some(msg) = worker_a.next().await {
            println!(
                "Worker A processed: {}",
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    tokio::spawn(async move {
        while let Some(msg) = worker_b.next().await {
            println!(
                "Worker B processed: {}",
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    tokio::spawn(async move {
        while let Some(msg) = worker_c.next().await {
            println!(
                "Worker C processed: {}",
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    // Publish messages - automatically load balanced
    for i in 1..=10 {
        client
            .publish("orders.new", format!("Order {}", i).into())
            .await?;
    }
    // NATS-DOC-END

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
