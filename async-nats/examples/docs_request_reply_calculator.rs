use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("localhost:4222").await?;

    // NATS-DOC-START
    // Calculator service
    let mut sub = client.subscribe("calc.add").await?;
    let service_client = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            let reply = match msg.reply {
                Some(reply) => reply,
                None => continue,
            };
            let input = String::from_utf8_lossy(&msg.payload);
            let parts: Vec<&str> = input.split_whitespace().collect();
            let result = match parts.as_slice() {
                [a, b] => match (a.parse::<i32>(), b.parse::<i32>()) {
                    (Ok(a), Ok(b)) => (a + b).to_string(),
                    _ => "error: operands must be integers".to_string(),
                },
                _ => "error: expected two operands".to_string(),
            };
            service_client.publish(reply, result.into()).await.ok();
        }
    });

    // Make sure the subscription is on the server before we send requests
    client.flush().await?;

    // Make calculations
    let resp = client.request("calc.add", "5 3".into()).await?;
    println!("5 + 3 = {}", String::from_utf8_lossy(&resp.payload));

    let resp = client.request("calc.add", "10 7".into()).await?;
    println!("10 + 7 = {}", String::from_utf8_lossy(&resp.payload));

    let resp = client.request("calc.add", "10 x".into()).await?;
    println!("10 + x = {}", String::from_utf8_lossy(&resp.payload));
    // NATS-DOC-END

    Ok(())
}
