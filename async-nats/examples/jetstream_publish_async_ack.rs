use async_nats::jetstream::{self, stream};
use clap::{Parser, ArgAction};
use futures::future::join_all;
use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of messages to publish
    #[arg(short, long, default_value_t = 10000)]
    count: usize,

    /// Size of each message in bytes
    #[arg(long, default_value_t = 32)]
    size: usize,

    /// Subject to publish to
    #[arg(short, long, default_value = "bench.test")]
    subjects: Vec<String>,

    /// Stream name
    #[arg(long, default_value = "BENCH_STREAM")]
    stream: String,

    /// NATS server URL
    #[arg(short, long, default_value = "nats://localhost:4222")]
    url: String,

    /// Max outstanding acks
    #[arg(long, default_value_t = 10000)]
    outstanding_acks: usize,

    /// Whether to create the stream or assert its existence
    #[arg(long, default_value_t = true, action = ArgAction::Set, value_parser = clap::value_parser!(bool))]
    create_stream: bool,
}

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let args = Args::parse();

    let semaphore = Arc::new(Semaphore::new(args.outstanding_acks));

    println!("Connecting to {}...", args.url);
    let client = async_nats::connect(&args.url).await?;
    let jetstream = jetstream::new(client);

    // Create or get the stream
    if args.create_stream {
        println!("Creating stream '{}'", args.stream);
        jetstream
            .get_or_create_stream(stream::Config {
                name: args.stream.clone(),
                subjects: args.subjects.clone(),
                ..Default::default()
            })
            .await?;
    } else {
        println!("Ensuring stream '{}' exists", args.stream);
    }
    println!(
        "Publishing {} messages of {} bytes each to stream '{}'",
        args.count, args.size, args.stream
    );

    // Prepare the message payload
    let payload = vec![b'X'; args.size];
    let subjects = args.subjects;
    let subjects_len = subjects.len();

    // Start timing
    let start = Instant::now();
    let publish_start = start;

    // Publish all messages without awaiting acks
    let mut ack_futures = Vec::with_capacity(args.count);
    for i in 0..args.count {
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let ack_future = jetstream
            .publish(subjects[i % subjects_len].clone(), payload.clone().into())
            .await?;

        // Spawn a task to release the permit when ack completes
        let ack_with_permit = tokio::spawn(async move {
            let result = ack_future.into_future().await;
            drop(permit); // Release the semaphore permit
            result
        });

        ack_futures.push(ack_with_permit);
    }

    let publish_duration = publish_start.elapsed();
    println!(
        "\rAll {} messages published in {:?}",
        args.count, publish_duration
    );

    let publish_rate = args.count as f64 / publish_duration.as_secs_f64();
    let publish_throughput =
        (args.count * args.size) as f64 / publish_duration.as_secs_f64() / 1024.0 / 1024.0;

    println!(
        "Publish rate: {:.0} msgs/sec, {:.2} MB/sec",
        publish_rate, publish_throughput
    );

    println!("\nAwaiting acknowledgments...");
    let ack_start = Instant::now();

    let results = join_all(ack_futures).await;

    let ack_duration = ack_start.elapsed();
    let total_duration = start.elapsed();

    // Check for any errors
    let mut success_count = 0;
    let mut error_count = 0;
    for result in results {
        match result {
            Ok(Ok(_)) => success_count += 1,
            Ok(Err(e)) => {
                error_count += 1;
                if error_count <= 5 {
                    eprintln!("Ack error: {}", e);
                }
            }
            Err(e) => {
                error_count += 1;
                if error_count <= 5 {
                    eprintln!("Task join error: {}", e);
                }
            }
        }
    }

    // Print results
    println!("\n=== Results ===");
    println!("Messages published: {}", args.count);
    println!("Messages acknowledged: {}", success_count);
    if error_count > 0 {
        println!("Errors: {}", error_count);
    }
    println!("\nTiming:");
    println!("  Publish time:     {:?}", publish_duration);
    println!("  Ack wait time:    {:?}", ack_duration);
    println!("  Total time:       {:?}", total_duration);

    let total_rate = args.count as f64 / total_duration.as_secs_f64();
    let total_throughput =
        (args.count * args.size) as f64 / total_duration.as_secs_f64() / 1024.0 / 1024.0;

    println!("\nEnd-to-end Performance:");
    println!("  Message rate:     {:.0} msgs/sec", total_rate);
    println!("  Throughput:       {:.2} MB/sec", total_throughput);
    println!(
        "  Avg latency:      {:.2} ms/msg",
        total_duration.as_millis() as f64 / args.count as f64
    );

    Ok(())
}
