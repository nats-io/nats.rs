use async_nats;
use futures::StreamExt;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("localhost:4222").await?;

    // NATS-DOC-START
    // Calculator service
    let mut sub = client.subscribe("calc.add").await?;
    let service_client = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            if let Some(reply) = msg.reply {
                let input = String::from_utf8_lossy(&msg.payload);
                let parts: Vec<&str> = input.split_whitespace().collect();

                if parts.len() == 2 {
                    if let (Ok(a), Ok(b)) = (parts[0].parse::<i32>(), parts[1].parse::<i32>()) {
                        let result = (a + b).to_string();
                        service_client.publish(reply, result.into()).await.ok();
                    }
                    else {
                        service_client.publish(reply, "error: invalid input".into()).await.ok();
                    }
                } else {
                    service_client.publish(reply, "error: invalid input".into()).await.ok();
                }
            }
        }
    });

    // Make calculations
    sleep(Duration::from_millis(100)).await;

    let resp = client.request("calc.add", "5 3".into()).await?;
    println!("5 + 3 = {}", String::from_utf8_lossy(&resp.payload));

    let resp = client.request("calc.add", "10 7".into()).await?;
    println!("10 + 7 = {}", String::from_utf8_lossy(&resp.payload));

    let resp = client.request("calc.add", "10 x".into()).await?;
    println!("10 + x = {}", String::from_utf8_lossy(&resp.payload));
    // NATS-DOC-END

    sleep(Duration::from_millis(100)).await;
    Ok(())
}
