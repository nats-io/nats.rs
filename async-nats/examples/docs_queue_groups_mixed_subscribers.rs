use async_nats;
use futures::StreamExt;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("localhost:4222").await?;

    // NATS-DOC-START
    // Audit logger - receives all messages
    let mut audit_sub = client.subscribe("orders.>").await?;
    tokio::spawn(async move {
        while let Some(msg) = audit_sub.next().await {
            println!(
                "[AUDIT] {}: {}",
                msg.subject,
                String::from_utf8_lossy(&msg.payload)
            );
        }
    });

    // Metrics collector - receives all messages
    let mut metrics_sub = client.subscribe("orders.>").await?;
    tokio::spawn(async move {
        while let Some(msg) = metrics_sub.next().await {
            update_metrics(&msg.subject);
        }
    });

    // Workers in queue group - load balanced
    let mut worker_a = client.queue_subscribe("orders.new", "workers").await?;
    tokio::spawn(async move {
        while let Some(msg) = worker_a.next().await {
            println!(
                "[WORKER A] Processing: {}",
                String::from_utf8_lossy(&msg.payload)
            );
            process_order(&msg.payload);
        }
    });

    let mut worker_b = client.queue_subscribe("orders.new", "workers").await?;
    tokio::spawn(async move {
        while let Some(msg) = worker_b.next().await {
            println!(
                "[WORKER B] Processing: {}",
                String::from_utf8_lossy(&msg.payload)
            );
            process_order(&msg.payload);
        }
    });

    // Publish order
    client.publish("orders.new", "Order 123".into()).await?;
    // Audit and metrics see it, one worker processes it
    // NATS-DOC-END

    sleep(Duration::from_millis(100)).await;
    Ok(())
}

fn update_metrics(_subject: &str) {
    // Update metrics implementation
}

fn process_order(_data: &[u8]) {
    // Process order implementation
}
