use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};

static PAYLOAD: &[u8] = &[22; 32];

// Pre-defined subjects as static strs to avoid cloning overhead
static SUBJECT_5: &str = "bench";
static SUBJECT_16: &str = "bench.data.testx";
static SUBJECT_32: &str = "events.data.bench.test.messages";
static SUBJECT_128: &str = "segment.segment.segment.segment.segment.segment.segment.segment.segment.segment.segment.segment.segment.segment.segment.segment.x";

async fn publish_with_static_str(
    nc: async_nats::Client,
    subject: &'static str,
    payload: Bytes,
    count: u64,
) {
    for _ in 0..count {
        nc.publish(subject, payload.clone()).await.unwrap();
    }
}

pub fn publish_validation_comparison(c: &mut Criterion) {
    let messages_per_iter = 500_000;
    let server = nats_server::run_basic_server();

    let mut validation_group = c.benchmark_group("nats::publish_validation_comparison");
    validation_group.sample_size(10);
    validation_group.warm_up_time(std::time::Duration::from_secs(1));

    // Test different subject lengths: 5, 16, 32, 128 characters
    for (subject_len, subject_str) in [
        (5, SUBJECT_5),
        (16, SUBJECT_16),
        (32, SUBJECT_32),
        (128, SUBJECT_128),
    ] {
        validation_group.throughput(criterion::Throughput::Elements(messages_per_iter));

        // Benchmark 1: With runtime validation (default)
        validation_group.bench_with_input(
            criterion::BenchmarkId::new("with_validation", subject_len),
            &subject_len,
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
                        publish_with_static_str(
                            nc,
                            subject_str,
                            Bytes::from_static(PAYLOAD),
                            messages_per_iter,
                        )
                        .await
                    }
                });
            },
        );

        // Benchmark 2: With validation disabled
        validation_group.bench_with_input(
            criterion::BenchmarkId::new("skip_validation", subject_len),
            &subject_len,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc = rt.block_on(async {
                    let nc = async_nats::ConnectOptions::new()
                        .skip_subject_validation(true)
                        .connect(server.client_url())
                        .await
                        .unwrap();
                    nc.publish("data", "data".into()).await.unwrap();
                    nc
                });

                b.to_async(rt).iter_with_large_drop(move || {
                    let nc = nc.clone();
                    async move {
                        publish_with_static_str(
                            nc,
                            subject_str,
                            Bytes::from_static(PAYLOAD),
                            messages_per_iter,
                        )
                        .await
                    }
                });
            },
        );
    }

    validation_group.finish();
}

criterion_group!(subject_validation, publish_validation_comparison);
criterion_main!(subject_validation);
