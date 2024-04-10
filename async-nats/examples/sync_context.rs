// Copyright 2024 The NATS Authors
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

use futures::StreamExt;

fn main() -> Result<(), async_nats::Error> {
    // Spawn a new runtime that will run on the current thread.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    // We are using `block_on` to run the async code in a sync context.
    let client = rt.block_on(async_nats::connect("nats://localhost:4222"))?;

    // Subscribe a client to the "foo" subject.
    let subscription = rt.block_on(client.subscribe("foo"))?;

    // Limit the number of messages to 10. This does not have to be done inside the
    // async context.
    let mut subscription = subscription.take(10);

    // Publish some messages
    for _ in 0..10 {
        rt.block_on(client.publish("foo", "Hello, sync code!".into()))?;
    }

    // To receive message in a loop, you can use the same pattern as you would in an async context.
    while let Some(message) = rt.block_on(subscription.next()) {
        println!("Received message {:?}", message);
    }

    // You need to drop subscripions in async context, as they do spawn tasks to clean themselves up.
    rt.block_on(async {
        drop(subscription);
        drop(client);
    });

    Ok(())
}
