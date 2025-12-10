use async_nats;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

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
