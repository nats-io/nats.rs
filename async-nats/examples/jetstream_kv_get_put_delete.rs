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

use std::env;
use std::str;

use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Use the NATS_URL env variable if defined, otherwise fallback to the default.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // Create an unauthenticated connection to NATS.
    let client = async_nats::connect(nats_url).await?;

    // Access the JetStream Context for managing streams and consumers as well as for publishing and subscription convenience methods.
    let jetstream = jetstream::new(client);

    // Create a new key value store.
    jetstream.create_key_value(async_nats::jetstream::kv::Config {
        bucket: "kv_example".to_string(),
         ..Default::default()
    }).await?;

    // Get the new key value store.
    let kv = jetstream.get_key_value("kv_example").await?;

    println!("Setting key value 'key1': 'hello key/value example'");

    // Put a key value pair into the store.
    kv.put("key1", "hello key/value example".into()).await?;

    // Get the value back out of the store.
    let entry = kv.get("key1").await?.unwrap();

    println!("Got key value '{}': '{}'", "key1", str::from_utf8(&entry)?);

    // Delete the key value pair from the store.
    jetstream.delete_key_value("kv_example").await?;

    Ok(())
}
