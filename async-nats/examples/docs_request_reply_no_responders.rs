use async_nats;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("localhost:4222").await?;

    // NATS-DOC-START
    match client.request("no.such.service".into(), "test".into()).await {
        Err(async_nats::RequestError::NoResponders) => {
            println!("No services available to handle request");
        }
        Err(e) => println!("Request error: {}", e),
        Ok(msg) => println!("Response: {}", String::from_utf8_lossy(&msg.payload)),
    }
    // NATS-DOC-END

    Ok(())
}
