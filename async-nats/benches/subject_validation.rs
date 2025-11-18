use async_nats::subject::ValidatedSubject;
use bytes::Bytes;
use criterion::{criterion_group, Criterion};

static MSG: &[u8] = &[22; 32768];

// Most efficient: compile-time validated subject (zero runtime validation overhead)
static VALIDATED_SUBJECT: ValidatedSubject = ValidatedSubject::from_static("bench");

pub fn publish_with_validation(c: &mut Criterion) {
    let messages_per_iter = 500_000;
    let server = nats_server::run_basic_server();

    // Baseline: standard publish pattern (validates on every publish)
    let mut baseline_group = c.benchmark_group("validation::baseline");
    baseline_group.sample_size(10);
    baseline_group.warm_up_time(std::time::Duration::from_secs(1));

    for &size in [32, 1024, 8192].iter() {
        baseline_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        baseline_group.bench_with_input(
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
                        publish_baseline(nc, Bytes::from_static(&MSG[..size]), messages_per_iter)
                            .await
                    }
                });
            },
        );
    }
    baseline_group.finish();

    // Optimized: pre-validated static subject (zero runtime validation overhead)
    let mut optimized_group = c.benchmark_group("validation::optimized");
    optimized_group.sample_size(10);
    optimized_group.warm_up_time(std::time::Duration::from_secs(1));

    for &size in [32, 1024, 8192].iter() {
        optimized_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        optimized_group.bench_with_input(
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
                        publish_optimized(nc, Bytes::from_static(&MSG[..size]), messages_per_iter)
                            .await
                    }
                });
            },
        );
    }
    optimized_group.finish();
}

// Baseline: standard publish - validates on every call
async fn publish_baseline(nc: async_nats::Client, msg: Bytes, amount: u64) {
    for _i in 0..amount {
        nc.publish("bench", msg.clone()).await.unwrap();
    }
}

// Optimized: uses static ValidatedSubject - zero validation overhead
async fn publish_optimized(nc: async_nats::Client, msg: Bytes, amount: u64) {
    for _i in 0..amount {
        nc.publish(&VALIDATED_SUBJECT, msg.clone()).await.unwrap();
    }
}

criterion_group!(benches, publish_with_validation);
