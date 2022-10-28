use std::path::PathBuf;
use std::str;

use futures::StreamExt;

use clap::{ArgGroup, Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, group(
    ArgGroup::new("auth")
        .required(false)
        .args(["creds", "auth_token"]),
))]
struct Cli {
    /// The NATS server URL(s)
    #[arg(short, long, default_value = "demo.nats.io")]
    server: Option<async_nats::ServerAddr>,

    /// Credentials file
    #[arg(short, long)]
    creds: Option<String>,

    /// Authentication token
    #[arg(short, long)]
    auth_token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    /// Publishes a message to a given subject
    Pub { subject: String, msg: String },
    /// Subscribes to a given subject
    Sub {
        subject: String,
        #[arg(short, long)]
        no_watch: bool,
    },
    /// Sends a request and waits on reply
    Request { subject: String, msg: String },
    /// Listens for requests and sends the reply
    Reply {
        subject: String,
        resp: String,
        #[arg(short, long)]
        reply_limit: Option<i8>,
    },
    /// Sets a key/value pair
    Set { key: String, value: String },
    /// Reads a key's value
    Get { key: String },
    /// Deletes a key's value
    Delete { key: String },
}

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let args = Cli::parse();

    let options = (match (args.creds, args.auth_token) {
        (Some(creds), _) => {
            async_nats::ConnectOptions::with_credentials_file(PathBuf::from(creds)).await?
        }
        (_, Some(token)) => async_nats::ConnectOptions::with_token(token),
        _ => async_nats::ConnectOptions::new(),
    })
    .name("nats-clap rust example");

    let client = async_nats::connect_with_options(&args.server.clone().unwrap(), options).await?;

    match args.command {
        Commands::Pub { subject, msg } => {
            client.publish(subject.clone(), msg.clone().into()).await?;
            client.flush().await?;

            println!("Published to '{}': '{}'", subject.clone(), msg.clone());
        }
        Commands::Sub { subject, no_watch } => {
            let mut subscription = client.subscribe(subject.into()).await.unwrap();

            println!("Awaiting messages");

            if no_watch {
                let message = subscription.next().await;

                println!("Received: '{}'", str::from_utf8(&message.unwrap().payload)?);
            } else {
                while let Some(message) = subscription.next().await {
                    println!("Received: '{}'", str::from_utf8(&message.payload)?);
                }
            }
        }
        Commands::Request { subject, msg } => {
            println!("Waiting on response for '{}'", subject);

            let resp = client.request(subject.into(), msg.into()).await?;

            println!("Response is: '{}'", str::from_utf8(&resp.payload)?);
        }
        Commands::Reply {
            subject,
            resp,
            reply_limit,
        } => {
            let mut limit = match reply_limit {
                None => -1,
                Some(l) => l,
            };

            let mut subscription = client.subscribe(subject.clone().into()).await?;

            println!("Listening for requests on '{}'", subject.clone());

            while let Some(message) = subscription.next().await {
                println!("Received a request '{}'", str::from_utf8(&message.payload)?);

                if let Some(reply) = message.reply {
                    client.publish(reply, resp.clone().into()).await?;
                    client.flush().await?;

                    println!("Published a response");
                }

                if limit > 0 {
                    limit = limit - 1;

                    if limit == 0 {
                        println!("Response limit reached");
                        break;
                    }
                }
            }
        }
        Commands::Set { key, value } => {
            println!("setting key '{}' to '{}'", key.clone(), value.clone());

            let jetstream = async_nats::jetstream::new(client);

            let kv = jetstream.get_key_value("kv").await?;

            kv.put(key, value.into()).await?;

            println!("set key");
        }
        Commands::Get { key } => {
            println!("reading key '{}'", key.clone());

            let jetstream = async_nats::jetstream::new(client);

            let kv = jetstream.get_key_value("kv").await?;

            let entry = kv.entry(key.clone()).await?;

            if let Some(entry) = entry {
                println!("'{}': '{}'", key.clone(), str::from_utf8(&entry.value)?);
            } else {
                println!("key '{}' not found", key);
            }
        }
        Commands::Delete { key } => {
            let jetstream = async_nats::jetstream::new(client);

            let kv = jetstream.get_key_value("kv").await?;

            kv.purge(key.clone()).await?;

            println!("'{}' value deleted", key);
        }
    }

    Ok(())
}
