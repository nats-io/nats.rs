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

use std::{env, str::from_utf8};

use async_nats::jetstream;
use futures_util::{StreamExt, TryStreamExt};

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let client = async_nats::connect(nats_url).await?;

    let jetstream = jetstream::new(client);

    // ### Bucket basics
    // A key-value (KV) bucket is created by specifying a bucket name.
    let kv = jetstream
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "profiles".to_string(),
            ..Default::default()
        })
        .await?;

    // As one would expect, the `async_nats::jetstream::kv` module provides the
    // standard `put` and `get` methods. However, unlike most KV
    // stores, a revision number of the entry is tracked.
    kv.put("sue.color", "blue".into()).await?;
    let entry = kv.entry("sue.color").await?;
    if let Some(entry) = entry {
        println!(
            "{} @ {} -> {}",
            entry.key,
            entry.revision,
            from_utf8(&entry.value)?
        );
    }

    kv.put("sue.color", "green".into()).await?;
    let entry = kv.entry("sue.color").await?;
    if let Some(entry) = entry {
        println!(
            "{} @ {} -> {}",
            entry.key,
            entry.revision,
            from_utf8(&entry.value)?
        );
    }

    // A revision number is useful when you need to enforce [optimistic
    // concurrency control][occ] on a specific key-value entry. In short,
    // if there are multiple actors attempting to put a new value for a
    // key concurrently, we want to prevent the "last writer wins" behavior
    // which is non-deterministic. To guard against this, we can use the
    // `kv.Update` method and specify the expected revision. Only if this
    // matches on the server, will the value be updated.
    // [occ]: https://en.wikipedia.org/wiki/Optimistic_concurrency_control
    kv.update("sue.color", "red".into(), 1)
        .await
        .expect_err("expected error");

    kv.update("sue.color", "red".into(), 2).await?;
    let entry = kv.entry("sue.color").await?;
    if let Some(entry) = entry {
        println!(
            "{} @ {} -> {}",
            entry.key,
            entry.revision,
            from_utf8(&entry.value)?
        );
    }

    // You can also iterate over all available keys in a bucket.
    let keys = kv.keys().await?.try_collect::<Vec<String>>().await?;

    println!("All keys: {keys:?}");

    // ### Stream abstraction
    // Before moving on, it is important to understand that a KV bucket is
    // light abstraction over a standard stream. This is by design since it
    // enables some powerful features which we will observe in a minute.
    //
    // **How exactly is a KV bucket modeled as a stream?**
    // When one is created, internally, a stream is created using the `KV_`
    // prefix as convention. Appropriate stream configuration are used that
    // are optimized for the KV access patterns, so you can ignore the
    // details.
    let name = jetstream.stream_names().next().await.unwrap().unwrap();
    println!("KV stream name: {name}");

    // Since it is a normal stream, we can create a consumer and
    // fetch messages.
    // If we look at the subject, we will notice that first token is a
    // special reserved prefix, the second token is the bucket name, and
    // remaining suffix is the actually key. The bucket name is inherently
    // a namespace for all keys and thus there is no concern for conflict
    // across buckets. This is different from what we need to do for a stream
    // which is to bind a set of _public_ subjects to a stream.
    let consumer = jetstream
        .get_stream("KV_profiles")
        .await?
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            ..Default::default()
        })
        .await?;
    let mut messages = consumer.messages().await?;
    let message = messages.next().await.unwrap()?;
    let metadata = message.info()?;
    println!(
        "{} @ {} -> {}",
        message.subject,
        metadata.stream_sequence,
        from_utf8(&message.payload)?
    );

    // Let's put a new value for this key and see what we get from the subscription.
    kv.put("sue.color", "yellow".into()).await?;
    let message = messages.next().await.unwrap()?;
    let metadata = message.info()?;
    println!(
        "{} @ {} -> {}",
        message.subject,
        metadata.stream_sequence,
        from_utf8(&message.payload)?
    );

    // Unsurprisingly, we get the new updated value as a message.
    // Since it's KV interface, we should be able to delete a key as well.
    // Does this result in a new message?
    kv.delete("sue.color").await?;
    let message = messages.next().await.unwrap()?;
    let metadata = message.info()?;
    println!(
        "{} @ {} -> {}",
        message.subject,
        metadata.stream_sequence,
        from_utf8(&message.payload)?
    );

    // 🤔 That is useful to get a message that something happened to that key,
    // and that this is considered a new revision.
    // However, how do we know if the new value was set to be `nil` or the key
    // was deleted?
    // To differentiate, delete-based messages contain a header. Notice the `KV-Operation: DEL`
    // header.
    println!("headers: {:?}", message.headers.as_ref().unwrap());

    // ### Watching for changes
    // Although one could subscribe to the stream directly, it is more convenient
    // to use a `KeyWatcher` which provides a deliberate API and types for tracking
    // changes over time. Notice that we can use a wildcard which we will come back to..
    let mut watch = kv.watch("sue.*").await?;

    // Even though we deleted the key, of course we can put a new value.
    kv.put("sue.color", "purple".into()).await?;

    // If we receive from the *watch* iterator, the value is a `Entry`
    // which exposes more KV-specific information than the raw stream message
    // shown above (so this API is recommended).
    // Since we initialized this watcher prior to setting the new color, the
    // first entry will contain the delete operation.
    let entry = watch.next().await.unwrap()?;
    println!(
        "{} @ {} -> {} (op: {:?})",
        entry.key,
        entry.revision,
        from_utf8(&entry.value)?,
        entry.operation
    );

    // To finish this short intro, since we know that keys are subjects under the covers, if we
    // put another key, we can observe the change through the watcher. One other detail to call out
    // is notice the revision for this *new* key is not `1`. It relies on the underlying stream's
    // message sequence number to indicate the _revision_. The guarantee being that it is always
    // monotonically increasing, but numbers will be shared across keys (like subjects) rather
    // than sequence numbers relative to each key.
    kv.put("sue.food", "pizza".into()).await?;

    let entry = watch.next().await.unwrap()?;
    println!(
        "{} @ {} -> {} (op: {:?})",
        entry.key,
        entry.revision,
        from_utf8(&entry.value)?,
        entry.operation
    );

    Ok(())
}
