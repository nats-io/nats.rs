use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;
    // NATS-DOC-START
    // Subscribe to all weather updates
    let mut sub1 = client.subscribe("sensor.alarm.*").await?;
    tokio::spawn(async move {
        while let Some(msg) = sub1.next().await {
            println!(
                "[sensor.alarm.*]       {:15} ({})",
                String::from_utf8_lossy(&msg.payload),
                msg.subject
            );
        }
    });

    let mut sub2 = client.subscribe("sensor.*.*.critical").await?;
    tokio::spawn(async move {
        while let Some(msg) = sub2.next().await {
            println!(
                "[sensor.*.*.critical]  {:15} ({})",
                String::from_utf8_lossy(&msg.payload),
                msg.subject
            );
        }
    });

    let mut sub3 = client.subscribe("sensor.>").await?;
    tokio::spawn(async move {
        while let Some(msg) = sub3.next().await {
            println!(
                "[sensor.>]             {:15} ({})",
                String::from_utf8_lossy(&msg.payload),
                msg.subject
            );
        }
    });

    // Publish to specific subjects
    client
        .publish("sensor.alarm.smoke", "kitchen,14:22".into())
        .await?;
    client
        .publish("sensor.alarm.smoke.critical", "kitchen,14:23".into())
        .await?;
    client
        .publish("sensor.alarm.water", "basement,16:42".into())
        .await?;
    client
        .publish("sensor.alarm.water.critical", "basement,16:43".into())
        .await?;
    // NATS-DOC-END

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
