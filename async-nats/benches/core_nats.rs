use criterion::{criterion_group, criterion_main, Criterion};
use futures::stream::StreamExt;

pub fn publish(c: &mut Criterion) {
    let server = nats_server::run_basic_server();
    let mut throughput_group = c.benchmark_group("async-nats: publish throughput");
    throughput_group.sample_size(10);
    throughput_group.warm_up_time(std::time::Duration::from_secs(1));

    let bmsg: Vec<u8> = (0..32768).map(|_| 22).collect();
    for size in [32, 128, 256, 1024, 4096, 8192].iter() {
        throughput_group.throughput(criterion::Throughput::Bytes(*size as u64 * 1000));
        throughput_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc =
                    rt.block_on(async { async_nats::connect(server.client_url()).await.unwrap() });
                let msg = &bmsg[0..*size];

                b.to_async(rt).iter(move || {
                    let nc = nc.clone();
                    async move { publish_1000_messages(nc, msg).await }
                });
            },
        );
    }
    throughput_group.finish();

    let mut messages_group = c.benchmark_group("async-nats: publish messages amount");
    messages_group.sample_size(10);
    messages_group.warm_up_time(std::time::Duration::from_secs(1));

    let bmsg: Vec<u8> = (0..32768).map(|_| 22).collect();
    for size in [32, 128, 256, 1024, 4096, 8192].iter() {
        messages_group.throughput(criterion::Throughput::Elements(1000));
        messages_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc = rt.block_on(async {
                    let mut nc = async_nats::connect(server.client_url()).await.unwrap();
                    nc.publish("data".to_string(), "data".into()).await.unwrap();
                    nc.flush().await.unwrap();
                    nc
                });
                let msg = &bmsg[0..*size];

                b.to_async(rt).iter(move || {
                    let nc = nc.clone();
                    async move { publish_1000_messages(nc, msg).await }
                });
            },
        );
    }
    messages_group.finish();
}

pub fn subscribe(c: &mut Criterion) {
    let server = nats_server::run_basic_server();

    let mut subscribe_amount_group = c.benchmark_group("subscribe amount");
    subscribe_amount_group.sample_size(10);
    subscribe_amount_group.warm_up_time(std::time::Duration::from_secs(1));

    for size in [32, 128, 256, 1024, 4096, 8192].iter() {
        subscribe_amount_group.throughput(criterion::Throughput::Elements(1000));
        subscribe_amount_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc = rt.block_on(async {
                    let mut nc = async_nats::connect(server.client_url()).await.unwrap();

                    tokio::task::spawn({
                        let mut nc = nc.clone();
                        async move {
                            let bmsg: Vec<u8> = (0..32768).map(|_| 22).collect();
                            let msg = &bmsg[0..*size].to_vec();

                            loop {
                                nc.publish("bench".to_string(), msg.clone().into())
                                    .await
                                    .unwrap();
                            }
                        }
                    });
                    nc.publish("data".to_string(), "data".into()).await.unwrap();
                    nc.flush().await.unwrap();
                    nc
                });

                b.to_async(rt).iter(move || {
                    let nc = nc.clone();
                    async move { subscribe_1000_messages(nc).await }
                });
            },
        );
    }
    subscribe_amount_group.finish();
}
async fn publish_1000_messages(mut nc: async_nats::Client, msg: &'_ [u8]) {
    let msg = msg.to_vec();
    for _i in 0..1000 {
        nc.publish("bench".into(), msg.clone().into())
            .await
            .unwrap();
    }
    nc.flush().await.unwrap();
}

async fn subscribe_1000_messages(mut nc: async_nats::Client) {
    let mut sub = nc.subscribe("bench".into()).await.unwrap();
    for _ in 0..1000 {
        sub.next().await.unwrap();
    }
}

criterion_group!(benches, publish, subscribe);
criterion_main!(benches);
