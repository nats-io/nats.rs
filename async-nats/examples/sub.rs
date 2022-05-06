use futures_util::StreamExt;
use std::error::Error;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    let now = Instant::now();
    let mut subscriber = client.subscribe("foo".into()).await.unwrap();

    println!("Awaiting messages");
    while let Some(message) = subscriber.next().await {
        println!("Received message {:?}", message);
    }

    println!("subscriber received in {:?}", now.elapsed());

    Ok(())
}
