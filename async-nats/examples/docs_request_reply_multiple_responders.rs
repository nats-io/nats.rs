use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Multiple responders - only first response is returned
    let mut sub_a = client.subscribe("calc.add").await?;
    let client_a = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub_a.next().await {
            let response = format!("calculated result from A");
            if let Some(reply) = msg.reply {
                client_a.publish(reply, response.into()).await.ok();
            }
        }
    });

    let mut sub_b = client.subscribe("calc.add").await?;
    let client_b = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub_b.next().await {
            let response = format!("calculated result from B");
            if let Some(reply) = msg.reply {
                client_b.publish(reply, response.into()).await.ok();
            }
        }
    });

    // Gets one response
    match client.request("calc.add", "data".into()).await {
        Ok(response) => {
            println!(
                "Got response: {}",
                String::from_utf8_lossy(&response.payload)
            );
        }
        Err(e) => {
            eprintln!("Request failed: {}", e);
        }
    }
    // NATS-DOC-END

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
