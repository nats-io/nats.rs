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

use tokio::io::AsyncReadExt;

use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Use the NATS_URL env variable if defined, otherwise fallback to the default.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // Create an unauthenticated connection to NATS.
    let client = async_nats::connect(nats_url).await?;

    // Access the JetStream Context for managing streams and consumers as well as for publishing and subscription convenience methods.
    let jetstream = jetstream::new(client);

    // Create a new object bucket
    let bucket = jetstream
        .create_object_store(async_nats::jetstream::object_store::Config {
            bucket: "example_bucket".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Create something to store in the bucket. In this case the nats logo.
    let nats_logo = r#"                                                       
 /////////////////// ((((((((((((((((((( ################### *******************
  ///     /////   /// (((((((    (((((((( ###             ### *****         *****
  ///  */   ///   /// (((((   (((  (((((( ########  ######### ****         ******
  ///  *////      /// ((((           (((( ########  ######### **** ********  ****
  /////////////////// ((((((((((((((((((( ################### *******************
  /////////////////// ((((((((((((((((((( ################### *******************
             //
"#;

    // Post the bytes of the nats logo to the bucket.
    bucket.put("LOGO", &mut nats_logo.as_bytes()).await.unwrap();

    // Get object back from the bucket
    let mut object_from_bucket = bucket.get("LOGO").await.unwrap();

    // Create a vectior to hold the bytes of the object.
    let mut object_value = Vec::new();
    loop {
        // Create a buffer to hold the bytes of the object.
        let mut buffer = [0; 1024];
        // Read the bytes of the object into the buffer.
        if let Ok(n) = object_from_bucket.read(&mut buffer).await {
            // If the number of bytes read is 0, we are done reading the object and can break out of the loop.
            if n == 0 {
                break;
            }

            // Extend the vector with the bytes read from the buffer.
            object_value.extend_from_slice(&buffer[..n]);
        }
    }

    // Print the stored object value to the console.
    println!("{}", str::from_utf8(&object_value).unwrap());

    // Delete the object store.
    jetstream
        .delete_object_store("example_bucket")
        .await
        .unwrap();

    Ok(())
}
