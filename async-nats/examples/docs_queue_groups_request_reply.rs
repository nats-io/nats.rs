use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Start multiple service instances with queue group for load balancing
    for i in 1..=3 {
        let client = client.clone();
        let instance_id = format!("instance-{}", i);
        let mut sub = client
            .queue_subscribe("api.calculate", "api-workers".to_string())
            .await?;

        tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                let response = format!("handled by {}", instance_id);

                if let Some(reply) = msg.reply {
                    client.publish(reply, response.into()).await.ok();
                }

                println!("Instance {} processed request", instance_id);
            }
        });
    }

    // Make requests - automatically load balanced
    for i in 0..10 {
        let response = client
            .request("api.calculate", format!("request {}", i).into())
            .await?;
        println!("Response: {}", String::from_utf8_lossy(&response.payload));
    }
    // NATS-DOC-END

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
