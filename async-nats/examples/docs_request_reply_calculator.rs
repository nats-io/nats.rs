use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Calculator service - replies with a fixed result for any request
    let mut sub = client.subscribe("calc.add").await?;
    let service_client = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            if let Some(reply) = msg.reply {
                service_client.publish(reply, "42".into()).await.ok();
            }
        }
    });

    // Make calculations
    let resp = client.request("calc.add", "5 3".into()).await?;
    println!("5 + 3 = {}", String::from_utf8_lossy(&resp.payload));

    let resp = client.request("calc.add", "10 7".into()).await?;
    println!("10 + 7 = {}", String::from_utf8_lossy(&resp.payload));
    // NATS-DOC-END

    Ok(())
}
