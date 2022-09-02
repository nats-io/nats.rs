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

mod object_store {

    use rand::RngCore;
    use tokio::io::AsyncReadExt;
    use tracing::Level;

    use super::*;

    #[tokio::test]

    async fn get() {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
        let server = nats_server::run_server("tests/configs/jetstream.conf");
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let bucket = jetstream
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "bucket".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut rng = rand::thread_rng();
        let mut bytes = vec![0; 1024 * 1024 + 22];
        rng.try_fill_bytes(&mut bytes).unwrap();

        bucket.put("FOO", &mut bytes.as_slice()).await.unwrap();

        let info = bucket.info("FOO").await.unwrap();

        println!("info: {:?}", info);

        let mut object = bucket.get("FOO").await.unwrap();

        let mut result = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            if let Ok(n) = object.read(&mut buffer).await {
                if n == 0 {
                    println!("finished");
                    break;
                }

                result.extend_from_slice(&buffer[..n]);
            }
        }
        assert_eq!(result, bytes);
    }
}
