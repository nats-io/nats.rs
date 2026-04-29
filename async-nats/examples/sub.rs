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
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    let now = Instant::now();
    let mut subscriber = client.subscribe("foo").await.unwrap();

    println!("Awaiting messages");
    while let Some(message) = subscriber.next().await {
        println!("Received message {message:?}");
    }

    println!("subscriber received in {:?}", now.elapsed());

    Ok(())
}
