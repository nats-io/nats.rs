use async_nats;
use futures::StreamExt;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("localhost:4222").await?;

    // NATS-DOC-START
    // Worker that can be dynamically added/removed
    struct Worker {
        id: String,
        handle: tokio::task::JoinHandle<()>,
    }

    let new_worker = |client: async_nats::Client, id: String| async move {
        let mut sub = client.queue_subscribe("tasks", "workers").await?;

        let handle = tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                println!("Worker {} processing: {}", id, String::from_utf8_lossy(&msg.payload));
                // Simulate work
                sleep(Duration::from_millis(100)).await;
            }
        });

        Ok::<Worker, async_nats::Error>(Worker { id, handle })
    };

    // Dynamic scaling
    let mut workers: Vec<Worker> = Vec::new();

    // Scale up
    for i in 1..=5 {
        let worker = new_worker(client.clone(), i.to_string()).await?;
        workers.push(worker);
    }

    // Scale down
    if let Some(worker) = workers.pop() {
        worker.handle.abort();
    }
    // NATS-DOC-END

    sleep(Duration::from_millis(500)).await;
    Ok(())
}
