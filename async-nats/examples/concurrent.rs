// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures_util::stream::StreamExt;
use rand::Rng;
use std::{env, str::from_utf8, time::Duration};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Use the NATS_URL env variable if defined, otherwise fallback
    // to the default.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());

    let client = async_nats::connect(nats_url).await?;

    // `Subscriber` implements Rust iterator, so we can leverage
    // combinators like `take()` to limit the messages intended
    // to be consumed for this interaction.
    let subscription = client.subscribe("greet.*").await?.take(50);

    // Publish set of messages, each with order identifier.
    for i in 0..50 {
        client
            .publish("greet.joe", format!("hello {i}").into())
            .await?;
    }

    // Flush the internal buffer and ensure that all messages are sent.
    client.flush().await?;

    // Iterate over messages concurrently.
    // for_each_concurrent allows us to not wait for time-consuming operation and receive next
    // message immediately.
    subscription
        .for_each_concurrent(25, |message| async move {
            let num = rand::thread_rng().gen_range(0..500);
            tokio::time::sleep(Duration::from_millis(num)).await;
            println!(
                "received message: {:?}",
                from_utf8(&message.payload).unwrap()
            )
        })
        .await;

    Ok(())
}
