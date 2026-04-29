use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Subscribe with single token wildcard
    let mut sub1 = client.subscribe("orders.*.shipped").await?;
    tokio::spawn(async move {
        while let Some(msg) = sub1.next().await {
            println!(
                "[orders.*.shipped] {}: ({})",
                String::from_utf8_lossy(&msg.payload),
                msg.subject
            );
        }
    });

    let mut sub2 = client.subscribe("orders.*.placed").await?;
    tokio::spawn(async move {
        while let Some(msg) = sub2.next().await {
            println!(
                "[orders.*.placed]  {}: ({})",
                String::from_utf8_lossy(&msg.payload),
                msg.subject
            );
        }
    });

    let mut sub3 = client.subscribe("orders.retail.*").await?;
    tokio::spawn(async move {
        while let Some(msg) = sub3.next().await {
            println!(
                "[orders.retail.*]  {}: ({})",
                String::from_utf8_lossy(&msg.payload),
                msg.subject
            );
        }
    });

    // Publish to specific subjects
    client
        .publish("orders.wholesale.placed", "Order W73737".into())
        .await?;
    client
        .publish("orders.retail.placed", "Order R65432".into())
        .await?;
    client
        .publish("orders.wholesale.shipped", "Order W73001".into())
        .await?;
    client
        .publish("orders.retail.shipped", "Order R65321".into())
        .await?;
    // NATS-DOC-END

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
