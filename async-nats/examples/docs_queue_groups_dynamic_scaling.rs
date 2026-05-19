use futures::StreamExt;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Worker that can be dynamically added/removed
    struct Worker {
        handle: tokio::task::JoinHandle<()>,
    }

    let new_worker = |client: async_nats::Client, id: i32| async move {
        let mut sub = client
            .queue_subscribe("tasks", "workers".to_string())
            .await?;

        let handle = tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                println!(
                    "Worker {} processing: {}",
                    id,
                    String::from_utf8_lossy(&msg.payload)
                );
            }
        });

        Ok::<Worker, async_nats::Error>(Worker { handle })
    };

    let mut workers: Vec<Worker> = Vec::new();

    // Scale up
    for i in 1..=5 {
        workers.push(new_worker(client.clone(), i).await?);
    }

    // Scale down: drop one worker
    if let Some(worker) = workers.pop() {
        worker.handle.abort();
    }
    // NATS-DOC-END

    sleep(Duration::from_millis(100)).await;
    Ok(())
}
