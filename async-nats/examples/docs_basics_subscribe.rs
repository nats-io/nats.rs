use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let nc = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    //  Subscribe to the "weather.updates" subject
    let mut sub = nc.subscribe("weather.updates").await?;

    while let Some(msg) = sub.next().await {
        println!("Received: {}", String::from_utf8_lossy(&msg.payload));
    }
    // NATS-DOC-END

    Ok(())
}
