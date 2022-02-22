use bytes::Bytes;
use futures_util::StreamExt;
use std::error::Error;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = nats_experimental::connect("localhost:4222").await?;

    let now = Instant::now();
    let subject = String::from("foo");
    let dat = Bytes::from("bar");
    for _ in 0..10_000_000 {
        client.publish(subject.clone(), dat.clone()).await?;
    }

    println!("published in {:?}", now.elapsed());

    Ok(())
}
