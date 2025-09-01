use futures_util::stream::StreamExt;
use tokio::time::Instant;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // spawn first task that clones `Client` which allows to using it in more than one task.
    tokio::task::spawn({
        let client = client.clone();
        async move {
            let mut subscriber = client.subscribe("foo").await?;

            println!("Awaiting messages on foo");
            while let Some(message) = subscriber.next().await {
                println!("Received message {message:?}");
            }
            // unfortunately we have to annotate return type in async blocks in Rust (for now)
            Ok::<(), async_nats::Error>(())
        }
    });

    // spawn a second task, clone client again, subscribe to another subject.
    tokio::task::spawn({
        let client = client.clone();
        async move {
            let mut subscriber = client.subscribe("bar").await?;

            println!("Awaiting messages on bar");
            while let Some(message) = subscriber.next().await {
                println!("Received message {message:?}");
            }

            Ok::<(), async_nats::Error>(())
        }
    });

    // spawn a task publishing to foo
    let foo_pub_handle = tokio::task::spawn({
        let client = client.clone();

        async move {
            let now = Instant::now();
            for _ in 0..10_000 {
                client.publish("foo", "data".into()).await?;
            }
            Ok::<std::time::Duration, async_nats::Error>(now.elapsed())
        }
    });

    // spawn a task publishing to bar
    let bar_pub_handle = tokio::task::spawn({
        let client = client.clone();
        async move {
            let now = Instant::now();
            for _ in 0..10_000 {
                client.publish("bar", "data".into()).await?;
            }
            Ok::<std::time::Duration, async_nats::Error>(now.elapsed())
        }
    });

    // run both publishing tasks in parallel and gather the results.
    match futures_util::try_join!(foo_pub_handle, bar_pub_handle) {
        Ok((foo_duration, bar_duration)) => println!(
            "finished publishing foo in {:?} and bar in {:?}",
            foo_duration?, bar_duration?
        ),
        Err(err) => println!("error while in task: {:?} ", err.to_string()),
    }

    Ok(())
}
