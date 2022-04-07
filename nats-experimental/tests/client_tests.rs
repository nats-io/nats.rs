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

mod nats_server;

mod client {
    use super::nats_server;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn basic_pub_sub() {
        let s = nats_server::run_basic_server();
        let mut con = nats_experimental::connect(s.client_url()).await.unwrap();

        let mut sub = con.subscribe("foo".into()).await.unwrap();

        for _ in 0..10 {
            con.publish("foo".to_string(), bytes::Bytes::from("data"))
                .await
                .unwrap();
        }
        con.flush().await.unwrap();

        let mut i = 0;
        while tokio::time::timeout(tokio::time::Duration::from_millis(100), sub.next())
            .await
            .unwrap()
            .is_some()
        {
            i += 1;
            if i >= 10 {
                break;
            }
        }
        assert_eq!(i, 10);
    }
}
