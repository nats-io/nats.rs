use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Set up a service
    let mut sub = client.subscribe("time").await?;
    let service_client = client.clone();

    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            let time = chrono::Utc::now().to_rfc3339();
            if let Some(reply) = msg.reply {
                service_client.publish(reply, time.into()).await.ok();
            }
        }
    });

    // Make a request
    match client.request("time", "".into()).await {
        Ok(response) => {
            println!("Response: {}", String::from_utf8_lossy(&response.payload));
        }
        Err(e) => {
            eprintln!("Request failed: {}", e);
        }
    }
    // NATS-DOC-END

    Ok(())
}
