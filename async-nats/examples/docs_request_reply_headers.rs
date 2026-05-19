use async_nats::HeaderMap;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // Service that reads the request id from the headers and echoes it back
    let mut sub = client.subscribe("service").await?;
    let service_client = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            let Some(reply) = msg.reply else { continue };
            let mut headers = HeaderMap::new();
            if let Some(request_id) = msg.headers.as_ref().and_then(|h| h.get("X-Request-ID")) {
                headers.insert("X-Response-ID", request_id.as_str());
            }
            service_client
                .publish_with_headers(reply, headers, msg.payload)
                .await
                .ok();
        }
    });

    // NATS-DOC-START
    // Create message with headers
    let mut headers = HeaderMap::new();
    headers.insert("X-Request-ID", "123");
    headers.insert("X-Priority", "high");

    let response = client
        .request_with_headers("service", headers, "data".into())
        .await?;

    println!("Response: {}", String::from_utf8_lossy(&response.payload));
    if let Some(response_id) = response
        .headers
        .as_ref()
        .and_then(|h| h.get("X-Response-ID"))
    {
        println!("Response ID: {}", response_id);
    }
    // NATS-DOC-END

    Ok(())
}
