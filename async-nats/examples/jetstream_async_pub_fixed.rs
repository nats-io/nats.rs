#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use async_nats::jetstream::{self, stream};
use clap::{ArgAction, Parser};
use futures::StreamExt;
use std::time::Instant;

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
    #[arg(short, long, default_value = "bench", value_delimiter = ',')]
    subjects: Vec<String>,

    /// Stream name
    #[arg(long, default_value = "BENCH")]
    stream: String,

    /// NATS server URL
    #[arg(short, long, default_value = "nats://localhost:4222")]
    url: String,

    /// Optional username for authentication
    #[arg(long)]
    username: Option<String>,

    /// Optional password for authentication
    #[arg(long)]
    password: Option<String>,

    /// Max outstanding acks
    #[arg(long, default_value_t = 10000)]
    outstanding_acks: usize,

    /// Enable backpressure mode (wait when max inflight is reached instead of erroring)
    #[arg(long, default_value_t = false)]
    backpressure: bool,

    /// Whether to create the stream or assert its existence
    #[arg(long, default_value_t = true, action = ArgAction::Set, value_parser = clap::value_parser!(bool))]
    create_stream: bool,

    /// Number of concurrent clients to use
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Delete streams
    #[arg(long, default_value_t = false, action = ArgAction::Set, value_parser = clap::value_parser!(bool))]
    delete_streams: bool,
}

async fn run() -> Result<(), async_nats::Error> {
    let args = Args::parse();

    println!("Connecting to {}...", args.url);

    if args.delete_streams {
        // Delete existing streams if requested
        let mut options = async_nats::ConnectOptions::new();

        if let Some(username) = &args.username {
            options =
                options.user_and_password(username.to_string(), args.password.clone().unwrap());
        }

        let client = options.connect(args.url.as_str()).await?;
        let jetstream = jetstream::new(client);
        for subject in &args.subjects {
            println!("Deleting stream '{}'", subject);
            match jetstream.delete_stream(subject).await {
                Ok(_) => println!("Stream '{}' deleted successfully", subject),
                Err(e) => eprintln!("Failed to delete stream '{}': {}", subject, e),
            }
        }
    }
    // Create the stream first using a single connection
    if args.create_stream {
        for subject in &args.subjects {
            let mut setup_client = async_nats::ConnectOptions::new()
                .client_capacity(args.outstanding_acks * 2)
                .subscription_capacity(args.outstanding_acks * 2);

            if let Some(username) = &args.username {
                setup_client = setup_client
                    .user_and_password(username.to_string(), args.password.clone().unwrap());
            }
            let client = setup_client.connect(args.url.as_str()).await?;
            let setup_jetstream = jetstream::new(client);
            println!("Creating stream '{}'", subject);
            setup_jetstream
                .get_or_create_stream(stream::Config {
                    name: subject.clone(),
                    subjects: vec![subject.clone()],
                    ..Default::default()
                })
                .await?;
        }
    } else {
        println!("Ensuring stream '{}' exists", args.stream);
    }

    println!(
        "Publishing {} messages of {} bytes each to stream '{}' using {} client(s)",
        args.count, args.size, args.stream, args.clients
    );
    if args.backpressure {
        println!(
            "Backpressure mode enabled: will wait when max inflight ({}) is reached",
            args.outstanding_acks
        );
    } else {
        println!(
            "Backpressure mode disabled: will error when max inflight ({}) is reached",
            args.outstanding_acks
        );
    }

    // Prepare the message payload
    let payload = vec![b'X'; args.size];
    let subjects = args.subjects;
    let subjects_len = subjects.len();

    // Channel for collecting ack futures
    let (tx, rx) = tokio::sync::mpsc::channel(args.count);

    // Start the ack processor BEFORE publishing
    let (results_tx, mut results_rx) = tokio::sync::mpsc::channel(args.count);

    let results_tx_clone = results_tx.clone();
    let ack_processor = tokio::spawn(async move {
        tokio_stream::wrappers::ReceiverStream::new(rx)
            .for_each_concurrent(200, |ack_future| {
                let tx = results_tx_clone.clone();
                async move {
                    let result = ack_future.await;
                    tx.send(result).await.unwrap();
                }
            })
            .await;
    });

    // Calculate messages per client
    let base_count = args.count / args.clients;
    let remainder = args.count % args.clients;

    // Create all client connections sequentially (with natural spacing)
    println!(
        "Creating {} client connections sequentially...",
        args.clients
    );
    let mut jetstream_contexts = Vec::new();
    for client_id in 0..args.clients {
        let mut options = async_nats::ConnectOptions::new()
            .client_capacity(args.outstanding_acks * 2)
            .subscription_capacity(args.outstanding_acks * 2);

        if let Some(username) = &args.username {
            options = options.user_and_password(
                username.to_string(),
                args.password.clone().unwrap_or_default(),
            )
        };

        let client = options.connect(&args.url).await?;

        let jetstream = jetstream::context::ContextBuilder::new()
            .max_ack_inflight(args.outstanding_acks)
            .backpressure_on_inflight(args.backpressure)
            .ack_timeout(tokio::time::Duration::from_secs(5))
            .build(client.clone());

        jetstream_contexts.push((client, jetstream));
        println!("Client {} connected", client_id);
    }

    println!("All clients connected. Starting coordinated publishing...");

    // Create a barrier to synchronize all tasks
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(args.clients));

    // Spawn multiple client tasks with pre-established connections
    let mut tasks = Vec::new();
    for (client_id, (client, jetstream)) in jetstream_contexts.into_iter().enumerate() {
        // Distribute messages evenly, with remainder going to first clients
        let messages_for_client = if client_id < remainder {
            base_count + 1
        } else {
            base_count
        };

        let start_idx = if client_id < remainder {
            client_id * (base_count + 1)
        } else {
            remainder * (base_count + 1) + (client_id - remainder) * base_count
        };

        let tx = tx.clone();
        let payload = payload.clone();
        let subjects = subjects.clone();
        let barrier_clone = barrier.clone();

        let task = tokio::spawn(async move {
            // Wait at the barrier until all tasks are ready
            barrier_clone.wait().await;

            // Publish this client's share of messages
            for i in 0..messages_for_client {
                let msg_idx = start_idx + i;
                let ack_future = jetstream
                    .publish(
                        subjects[msg_idx % subjects_len].clone(),
                        payload.clone().into(),
                    )
                    .await
                    .unwrap();

                tx.send(ack_future).await.unwrap();

                // if (i + 1) % 1000 == 0 {
                //     println!("Client {} published {} messages", client_id, i + 1);
                // }
            }

            println!(
                "Client {} completed publishing {} messages",
                client_id, messages_for_client
            );

            // Return the client to keep it alive
            client
        });

        tasks.push(task);
    }

    // Start timing immediately after spawning all tasks (they'll be released from barrier)
    let start = Instant::now();
    let publish_start = Instant::now();

    // Collect clients to keep them alive until acks complete
    let mut clients = Vec::new();
    for task in tasks {
        let client = task.await.unwrap();
        clients.push(client);
    }

    drop(tx); // Signal we're done publishing

    let publish_duration = publish_start.elapsed();
    println!(
        "\nAll {} messages published in {:?}",
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

    // Wait for all acks to be processed
    ack_processor.await.unwrap();
    drop(results_tx);

    // Collect results
    let mut success_count = 0;
    let mut error_count = 0;
    while let Some(result) = results_rx.recv().await {
        match result {
            Ok(_) => success_count += 1,
            Err(e) => {
                error_count += 1;
                if error_count <= 5 {
                    eprintln!("Ack error: {}", e);
                }
            }
        }
    }

    let ack_duration = ack_start.elapsed();
    let total_duration = start.elapsed();

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

    // Keep clients alive until the end
    drop(clients);

    Ok(())
}

fn main() -> Result<(), async_nats::Error> {
    // console_subscriber::init();
    let num_threads = num_cpus::get();
    println!("Using {} threads for Tokio runtime", num_threads);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("jetstream_async_pub_fixed")
        .worker_threads(num_threads)
        .build()
        .expect("Failed to create Tokio runtime");

    rt.block_on(run())
}
