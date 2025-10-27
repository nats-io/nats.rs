use std::time::Duration;

use bytes::Bytes;
use criterion::{criterion_group, Criterion};
use futures_util::stream::StreamExt;

static MSG: &[u8] = &[22; 32768];

pub fn publish(c: &mut Criterion) {
    let messages_per_iter = 500_000;
    let server = nats_server::run_basic_server();
    let mut throughput_group = c.benchmark_group("nats::publish_throughput");
    throughput_group.sample_size(10);
    throughput_group.warm_up_time(std::time::Duration::from_secs(1));

    for &size in [32, 1024, 8192].iter() {
        throughput_group.throughput(criterion::Throughput::Bytes(
            size as u64 * messages_per_iter,
        ));
        throughput_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc =
                    rt.block_on(async { async_nats::connect(server.client_url()).await.unwrap() });

                b.to_async(rt).iter_with_large_drop(move || {
                    let nc = nc.clone();
                    async move {
                        publish_messages(nc, Bytes::from_static(&MSG[..size]), messages_per_iter)
                            .await
                    }
                });
            },
        );
    }
    throughput_group.finish();

    let mut messages_group = c.benchmark_group("nats::publish_amount");
    messages_group.sample_size(10);
    messages_group.warm_up_time(std::time::Duration::from_secs(1));

    for &size in [32, 1024, 8192].iter() {
        messages_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        messages_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc = rt.block_on(async {
                    let nc = async_nats::connect(server.client_url()).await.unwrap();
                    nc.publish("data", "data".into()).await.unwrap();
                    nc
                });

                b.to_async(rt).iter_with_large_drop(move || {
                    let nc = nc.clone();
                    async move {
                        publish_messages(nc, Bytes::from_static(&MSG[..size]), messages_per_iter)
                            .await
                    }
                });
            },
        );
    }
    messages_group.finish();
}

pub fn subscribe(c: &mut Criterion) {
    let server = nats_server::run_basic_server();
    let messages_per_iter = 500_000;

    let mut subscribe_amount_group = c.benchmark_group("nats::subscribe_amount");
    subscribe_amount_group.sample_size(10);

    for &size in [32, 1024, 8192].iter() {
        let url = server.client_url();
        subscribe_amount_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        subscribe_amount_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            move |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let url = url.clone();
                let nc = rt.block_on(async move {
                    let nc = async_nats::ConnectOptions::new()
                        .connect(url.clone())
                        .await
                        .unwrap();
                    let (started, ready) = tokio::sync::oneshot::channel();
                    tokio::task::spawn({
                        async move {
                            let client = async_nats::ConnectOptions::new()
                                .connect(url)
                                .await
                                .unwrap();

                            started.send(()).unwrap();
                            loop {
                                client
                                    .publish("bench", Bytes::from_static(&MSG[..size]))
                                    .await
                                    .unwrap()
                            }
                        }
                    });
                    nc.publish("data", "data".into()).await.unwrap();
                    ready.await.unwrap();
                    nc
                });

                b.to_async(rt).iter_with_large_drop(move || {
                    let nc = nc.clone();
                    async move { subscribe_messages(nc, messages_per_iter).await }
                });
            },
        );
    }
    subscribe_amount_group.finish();
}

pub fn request(c: &mut Criterion) {
    let server = nats_server::run_basic_server();
    let messages_per_iter = 10_000;

    let mut subscribe_amount_group = c.benchmark_group("nats::request_amount");
    subscribe_amount_group.sample_size(10);

    for &size in [32, 1024, 8192].iter() {
        let url = server.client_url();
        subscribe_amount_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        subscribe_amount_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            move |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let url = url.clone();
                let nc = rt.block_on(async move {
                    let nc = async_nats::ConnectOptions::new()
                        .connect(url.clone())
                        .await
                        .unwrap();
                    let (started, ready) = tokio::sync::oneshot::channel();
                    tokio::task::spawn({
                        async move {
                            let client = async_nats::ConnectOptions::new()
                                .connect(url)
                                .await
                                .unwrap();

                            let mut subscription = client.subscribe("bench").await.unwrap();
                            tokio::time::sleep(Duration::from_secs(3)).await;
                            started.send(()).unwrap();

                            while let Some(request) = subscription.next().await {
                                client
                                    .publish(request.reply.unwrap(), "".into())
                                    .await
                                    .unwrap();
                            }
                        }
                    });
                    ready.await.unwrap();
                    nc
                });
                b.to_async(rt).iter_with_large_drop(move || {
                    let nc = nc.clone();
                    async move {
                        requests(nc, Bytes::from_static(&MSG[..size]), messages_per_iter).await
                    }
                });
            },
        );
    }
    subscribe_amount_group.finish();
}

async fn requests(nc: async_nats::Client, msg: Bytes, amount: u64) {
    for _i in 0..amount {
        nc.request("bench", msg.clone()).await.unwrap();
    }
}

async fn publish_messages(nc: async_nats::Client, msg: Bytes, amount: u64) {
    for _i in 0..amount {
        nc.publish("bench", msg.clone()).await.unwrap();
    }
}

async fn subscribe_messages(nc: async_nats::Client, amount: u64) {
    let mut sub = nc.subscribe("bench").await.unwrap();
    for _ in 0..amount {
        sub.next().await.unwrap();
    }
}

#[cfg(target_os = "linux")]
pub fn publish_uring(c: &mut Criterion) {
    let messages_per_iter = 500_000;
    let server = nats_server::run_basic_server();
    let mut throughput_group = c.benchmark_group("nats::publish_throughput_uring");
    throughput_group.sample_size(10);
    throughput_group.warm_up_time(std::time::Duration::from_secs(1));

    for &size in [32, 1024, 8192].iter() {
        throughput_group.throughput(criterion::Throughput::Bytes(
            size as u64 * messages_per_iter,
        ));
        throughput_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            |b, _| {
                let url = server.client_url();
                b.iter(|| {
                    tokio_uring::start(async {
                        let nc = async_nats::connect(&url).await.unwrap();
                        publish_messages(nc, Bytes::from_static(&MSG[..size]), messages_per_iter)
                            .await
                    })
                });
            },
        );
    }
    throughput_group.finish();

    let mut messages_group = c.benchmark_group("nats::publish_amount_uring");
    messages_group.sample_size(10);
    messages_group.warm_up_time(std::time::Duration::from_secs(1));

    for &size in [32, 1024, 8192].iter() {
        messages_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        messages_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            |b, _| {
                let url = server.client_url();
                b.iter(|| {
                    tokio_uring::start(async {
                        let nc = async_nats::connect(&url).await.unwrap();
                        nc.publish("data", "data".into()).await.unwrap();
                        publish_messages(nc, Bytes::from_static(&MSG[..size]), messages_per_iter)
                            .await
                    })
                });
            },
        );
    }
    messages_group.finish();
}

#[cfg(target_os = "linux")]
pub fn subscribe_uring(c: &mut Criterion) {
    let server = nats_server::run_basic_server();
    let messages_per_iter = 500_000;

    let mut subscribe_amount_group = c.benchmark_group("nats::subscribe_amount_uring");
    subscribe_amount_group.sample_size(10);

    for &size in [32, 1024, 8192].iter() {
        let url = server.client_url();
        subscribe_amount_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        subscribe_amount_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            move |b, _| {
                let url = url.clone();
                b.iter(|| {
                    tokio_uring::start(async {
                        let nc = async_nats::ConnectOptions::new()
                            .connect(&url)
                            .await
                            .unwrap();
                        let (started, ready) = tokio::sync::oneshot::channel();
                        tokio::task::spawn({
                            let url = url.clone();
                            async move {
                                let client = async_nats::ConnectOptions::new()
                                    .connect(&url)
                                    .await
                                    .unwrap();

                                started.send(()).unwrap();
                                loop {
                                    client
                                        .publish("bench", Bytes::from_static(&MSG[..size]))
                                        .await
                                        .unwrap()
                                }
                            }
                        });
                        nc.publish("data", "data".into()).await.unwrap();
                        ready.await.unwrap();
                        subscribe_messages(nc, messages_per_iter).await
                    })
                });
            },
        );
    }
    subscribe_amount_group.finish();
}

#[cfg(target_os = "linux")]
pub fn request_uring(c: &mut Criterion) {
    let server = nats_server::run_basic_server();
    let messages_per_iter = 10_000;

    let mut subscribe_amount_group = c.benchmark_group("nats::request_amount_uring");
    subscribe_amount_group.sample_size(10);

    for &size in [32, 1024, 8192].iter() {
        let url = server.client_url();
        subscribe_amount_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        subscribe_amount_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            move |b, _| {
                let url = url.clone();
                b.iter(|| {
                    tokio_uring::start(async {
                        let nc = async_nats::ConnectOptions::new()
                            .connect(&url)
                            .await
                            .unwrap();
                        let (started, ready) = tokio::sync::oneshot::channel();
                        tokio::task::spawn({
                            let url = url.clone();
                            async move {
                                let client = async_nats::ConnectOptions::new()
                                    .connect(&url)
                                    .await
                                    .unwrap();

                                let mut subscription = client.subscribe("bench").await.unwrap();
                                tokio::time::sleep(Duration::from_secs(3)).await;
                                started.send(()).unwrap();

                                while let Some(request) = subscription.next().await {
                                    client
                                        .publish(request.reply.unwrap(), "".into())
                                        .await
                                        .unwrap();
                                }
                            }
                        });
                        ready.await.unwrap();
                        requests(nc, Bytes::from_static(&MSG[..size]), messages_per_iter).await
                    })
                });
            },
        );
    }
    subscribe_amount_group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    core_nats_uring,
    publish_uring,
    subscribe_uring,
    request_uring
);

criterion_group!(core_nats, publish, subscribe, request);
