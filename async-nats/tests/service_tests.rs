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

#[cfg(feature = "service")]
mod service {
    use std::{collections::HashMap, str::from_utf8};

    use async_nats::service::{self, Info, ServiceExt, StatsResponse};
    use futures::StreamExt;
    use tracing::debug;

    #[tokio::test]
    async fn service_config_validations() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        // not semver compatible version string.
        let err_kind = client
            .add_service(async_nats::service::Config {
                name: "serviceA".to_string(),
                description: None,
                version: "1.0.0.1".to_string(),
                schema: None,
                stats_handler: None,
                metadata: None,
            })
            .await
            .unwrap_err()
            .downcast::<std::io::Error>()
            .unwrap()
            .kind();
        assert_eq!(std::io::ErrorKind::InvalidInput, err_kind);

        // not semver compatible version string.
        let err_kind = client
            .add_service(async_nats::service::Config {
                name: "serviceB".to_string(),
                description: None,
                version: "beta-1.0.0".to_string(),
                schema: None,
                stats_handler: None,
                metadata: None,
            })
            .await
            .unwrap_err()
            .downcast::<std::io::Error>()
            .unwrap()
            .kind();
        assert_eq!(std::io::ErrorKind::InvalidInput, err_kind);

        // bad service name name.
        let err_kind = client
            .add_service(async_nats::service::Config {
                name: "service.B".to_string(),
                description: None,
                version: "1.0.0".to_string(),
                schema: None,
                stats_handler: None,
                metadata: None,
            })
            .await
            .unwrap_err()
            .downcast::<std::io::Error>()
            .unwrap()
            .kind();
        assert_eq!(std::io::ErrorKind::InvalidInput, err_kind);

        // bad service name name.
        let err_kind = client
            .add_service(async_nats::service::Config {
                name: "service B".to_string(),
                description: None,
                version: "1.0.0".to_string(),
                schema: None,
                stats_handler: None,
                metadata: None,
            })
            .await
            .unwrap_err()
            .downcast::<std::io::Error>()
            .unwrap()
            .kind();
        assert_eq!(std::io::ErrorKind::InvalidInput, err_kind);
    }

    #[tokio::test]
    async fn metadata() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let metadata = HashMap::from([
            ("key".to_string(), "value".to_string()),
            ("other".to_string(), "value".to_string()),
        ]);
        client
            .add_service(async_nats::service::Config {
                name: "serviceA".to_string(),
                description: None,
                version: "1.0.0".to_string(),
                schema: None,
                stats_handler: None,
                metadata: Some(metadata.clone()),
            })
            .await
            .unwrap();

        let reply = client.new_inbox();
        let mut responses = client.subscribe(reply.clone()).await.unwrap();
        client
            .publish_with_reply("$SRV.INFO".to_string(), reply, "".into())
            .await
            .unwrap();
        let response = responses
            .next()
            .await
            .map(|message| {
                serde_json::from_slice::<service::Info>(&message.payload)
                    .unwrap()
                    .metadata
            })
            .unwrap();
        assert_eq!(metadata, response);
    }

    #[tokio::test]
    async fn ping() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        client
            .add_service(async_nats::service::Config {
                name: "serviceA".to_string(),
                description: None,
                version: "1.0.0".to_string(),
                schema: None,
                stats_handler: None,
                metadata: None,
            })
            .await
            .unwrap();

        client
            .add_service(async_nats::service::Config {
                name: "serviceB".to_string(),
                description: None,
                version: "2.0.0".to_string(),
                schema: None,
                stats_handler: None,
                metadata: None,
            })
            .await
            .unwrap();

        let reply = client.new_inbox();
        let mut responses = client.subscribe(reply.clone()).await.unwrap();
        client
            .publish_with_reply("$SRV.PING".to_string(), reply, "".into())
            .await
            .unwrap();
        responses.next().await.unwrap();
        responses.next().await.unwrap();
    }

    #[tokio::test]
    async fn groups() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let service = client
            .add_service(async_nats::service::Config {
                name: "serviceA".to_string(),
                version: "1.0.0".to_string(),
                schema: None,
                description: None,
                stats_handler: None,
                metadata: None,
            })
            .await
            .unwrap();

        let mut products = service.endpoint("products").await.unwrap();
        let reply = client.new_inbox();
        let mut responses = client.subscribe(reply.clone()).await.unwrap();
        client
            .publish_with_reply("products".to_string(), reply.clone(), "data".into())
            .await
            .unwrap();
        let request = products.next().await.unwrap();
        request.respond(Ok("response".into())).await.unwrap();
        responses.next().await.unwrap();

        let v2 = service.group("v2");
        let mut v2product = v2.endpoint("products").await.unwrap();
        client
            .publish_with_reply("v2.products".to_string(), reply.clone(), "data".into())
            .await
            .unwrap();
        let request = v2product.next().await.unwrap();
        request.respond(Ok("v2".into())).await.unwrap();
        let message = responses.next().await.unwrap();
        assert_eq!(from_utf8(&message.payload).unwrap(), "v2".to_string());
    }

    #[tokio::test]
    async fn requests() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let service = client
            .add_service(async_nats::service::Config {
                name: "serviceA".to_string(),
                version: "1.0.0".to_string(),
                schema: None,
                description: None,
                stats_handler: None,
                metadata: None,
            })
            .await
            .unwrap();

        let mut endpoint = service.endpoint("products").await.unwrap().take(3);
        let reply = client.new_inbox();
        let mut response = client.subscribe(reply.clone()).await.unwrap();
        client
            .publish_with_reply("products".to_string(), reply.clone(), "data".into())
            .await
            .unwrap();
        client
            .publish_with_reply("products".to_string(), reply.clone(), "data".into())
            .await
            .unwrap();
        client
            .publish_with_reply("products".to_string(), reply.clone(), "data".into())
            .await
            .unwrap();
        client
            .publish_with_reply("products".to_string(), reply.clone(), "data".into())
            .await
            .unwrap();
        client.flush().await.unwrap();

        // respond with 3 Oks.
        while let Some(request) = endpoint.next().await {
            request.respond(Ok("data".into())).await.unwrap();
        }
        let mut endpoint = endpoint.into_inner();
        // 4 respond is an error.
        if let Some(request) = endpoint.next().await {
            debug!("respond with error");
            request
                .respond(Err(async_nats::service::error::Error {
                    code: 503,
                    status: "error".to_string(),
                }))
                .await
                .unwrap();
        }

        let info = client
            .request("$SRV.INFO.serviceA".into(), "".into())
            .await
            .map(|message| serde_json::from_slice::<Info>(&message.payload))
            .unwrap()
            .unwrap();
        assert_eq!(info.version, "1.0.0".to_string());
        assert_eq!(info.name, "serviceA".to_string());

        let stats = client
            .request("$SRV.STATS".into(), "".into())
            .await
            .map(|message| serde_json::from_slice::<StatsResponse>(&message.payload))
            .unwrap()
            .unwrap();
        let requests = stats
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == "products")
            .unwrap();
        assert_eq!(requests.requests, 4);
        assert_eq!(requests.errors, 1);

        // stopping the service.

        assert!(service
            .stats()
            .await
            .endpoints
            .get("products")
            .unwrap()
            .last_error
            .is_some());

        service.stop().await.unwrap();

        assert!(response.next().await.is_some());
        assert!(response.next().await.is_some());
        assert!(response.next().await.is_some());
        let error_response = response.next().await.unwrap();
        assert_eq!(
            error_response
                .headers
                .clone()
                .unwrap()
                .get(async_nats::service::NATS_SERVICE_ERROR_CODE)
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            503
        );
        assert_eq!(
            *error_response
                .headers
                .unwrap()
                .get(async_nats::service::NATS_SERVICE_ERROR)
                .unwrap()
                .iter()
                .next()
                .unwrap(),
            "error".to_string()
        );

        // service should not respond anymore, as its stopped.
        client
            .request("$SRV.PING".to_string(), "".into())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    #[cfg(not(target_os = "windows"))]
    async fn cross_clients_tests() {
        use std::process::Command;

        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();

        let service = client
            .service_builder()
            .stats_handler(|endpoint, _| format!("custom data for {endpoint}"))
            .description("a cross service")
            .start("cross", "1.0.0")
            .await
            .unwrap();

        let mut endpoint = service.endpoint("cross").await.unwrap();
        tokio::task::spawn(async move {
            while let Some(request) = endpoint.next().await {
                if request.message.payload.is_empty()
                    || from_utf8(&request.message.payload).unwrap() == "error"
                {
                    request
                        .respond(Err(async_nats::service::error::Error {
                            code: 503,
                            status: "empty payload".to_string(),
                        }))
                        .await
                        .unwrap();
                } else {
                    let echo = request.message.payload.clone();
                    request.respond(Ok(echo)).await.unwrap();
                }
            }
        });

        Command::new("deno").args(["run", "-A", "--unstable", "https://raw.githubusercontent.com/nats-io/nats.deno/main/tests/helpers/service-check.ts", "--server", &server.client_url(), "--name", "cross"]).output().unwrap();
    }
}
