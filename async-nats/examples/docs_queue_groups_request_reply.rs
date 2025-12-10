use async_nats;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Service instance with queue group for load balancing
    let create_service_instance = |client: async_nats::Client, instance_id: String| {
        tokio::spawn(async move {
            let mut sub = client
                .queue_subscribe("api.calculate", "api-workers".to_string())
                .await
                .unwrap();

            while let Some(msg) = sub.next().await {
                // Parse request (simplified)
                let response = format!("{{\"result\": 42, \"processedBy\": \"{}\"}}", instance_id);

                if let Some(reply) = msg.reply {
                    client.publish(reply, response.into()).await.ok();
                }

                println!("Instance {} processed request", instance_id);
            }
        });
    };

    // Start multiple service instances
    for i in 1..=3 {
        create_service_instance(client.clone(), format!("instance-{}", i));
    }

    // Make requests - automatically load balanced
    for i in 0..10 {
        match client
            .request(
                "api.calculate",
                format!("{{\"a\": {}, \"b\": {}}}", i, i * 2).into(),
            )
            .await
        {
            Ok(response) => {
                println!("Response: {}", String::from_utf8_lossy(&response.payload));
            }
            Err(e) => {
                eprintln!("Request failed: {}", e);
            }
        }
    }
    // NATS-DOC-END

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
