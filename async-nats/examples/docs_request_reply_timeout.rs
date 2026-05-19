use futures::StreamExt;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // Slow responder that takes longer than our timeout
    let mut sub = client.subscribe("service").await?;
    let service_client = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            sleep(Duration::from_secs(5)).await;
            if let Some(reply) = msg.reply {
                service_client.publish(reply, "slow".into()).await.ok();
            }
        }
    });

    // NATS-DOC-START
    // Request with custom timeout
    match tokio::time::timeout(
        Duration::from_secs(2),
        client.request("service", "data".into()),
    )
    .await
    {
        Ok(Ok(response)) => {
            println!("Response: {}", String::from_utf8_lossy(&response.payload));
        }
        Ok(Err(e)) => {
            eprintln!("Request failed: {}", e);
        }
        Err(_) => {
            println!("Request timed out");
        }
    }
    // NATS-DOC-END

    Ok(())
}
