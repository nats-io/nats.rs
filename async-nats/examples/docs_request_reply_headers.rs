use async_nats::HeaderMap;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("localhost:4222").await?;

    // NATS-DOC-START
    // Create message with headers
    let mut headers = HeaderMap::new();
    headers.insert("X-Request-ID", "123");
    headers.insert("X-Priority", "high");

    let response = client
        .request_with_headers("service".into(), headers, "data".into())
        .await?;

    println!("Response: {}", String::from_utf8_lossy(&response.payload));
    if let Some(response_id) = response.headers.as_ref().and_then(|h| h.get("X-Response-ID")) {
        println!("Response ID: {}", response_id);
    }
    // NATS-DOC-END

    Ok(())
}
