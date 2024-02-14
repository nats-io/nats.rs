use async_nats::jetstream::{context::PublishAckFuture, stream};
use bytes::Bytes;
use criterion::{criterion_group, Criterion};

static MSG: &[u8] = &[22; 32768];

pub fn jetstream_publish_sync(c: &mut Criterion) {
    let messages_per_iter = 50_000;
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let mut throughput_group = c.benchmark_group("jetstream::sync_publish_throughput");
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
                let context = rt.block_on(async {
                    let context = async_nats::jetstream::new(
                        async_nats::connect(server.client_url()).await.unwrap(),
                    );

                    let stream = context
                        .create_stream(stream::Config {
                            name: "bench".to_owned(),
                            subjects: vec!["bench".to_string()],
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                    stream.purge().await.unwrap();
                    context
                });

                b.to_async(rt).iter_with_large_drop(move || {
                    let nc = context.clone();
                    async move {
                        publish_sync_batch(nc, Bytes::from_static(&MSG[..size]), messages_per_iter)
                            .await
                    }
                });
            },
        );
    }
    throughput_group.finish();

    let mut messages_group = c.benchmark_group("jetstream sync publish messages amount");
    messages_group.sample_size(10);
    messages_group.warm_up_time(std::time::Duration::from_secs(1));

    for &size in [32, 1024, 8192].iter() {
        messages_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        messages_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let context = rt.block_on(async {
                    let context = async_nats::jetstream::new(
                        async_nats::connect(server.client_url()).await.unwrap(),
                    );

                    let stream = context
                        .create_stream(stream::Config {
                            name: "bench".to_owned(),
                            subjects: vec!["bench".to_string()],
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                    stream.purge().await.unwrap();
                    context
                });

                b.to_async(rt).iter_with_large_drop(move || {
                    let context = context.clone();
                    async move {
                        publish_sync_batch(
                            context,
                            Bytes::from_static(&MSG[..size]),
                            messages_per_iter,
                        )
                        .await
                    }
                });
            },
        );
    }
    messages_group.finish();
}

pub fn jetstream_publish_async(c: &mut Criterion) {
    let messages_per_iter = 50_000;
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let mut throughput_group = c.benchmark_group("jetstream async publish throughput");
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
                let context = rt.block_on(async {
                    let context = async_nats::jetstream::new(
                        async_nats::connect(server.client_url()).await.unwrap(),
                    );

                    let stream = context
                        .create_stream(stream::Config {
                            name: "bench".to_owned(),
                            subjects: vec!["bench".to_string()],
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                    stream.purge().await.unwrap();
                    context
                });

                b.to_async(rt).iter_with_large_drop(move || {
                    let nc = context.clone();
                    async move {
                        publish_async_batch(nc, Bytes::from_static(&MSG[..size]), messages_per_iter)
                            .await
                    }
                });
            },
        );
    }
    throughput_group.finish();

    let mut messages_group = c.benchmark_group("jetstream::async_publish_messages_amount");

    messages_group.sample_size(10);
    messages_group.warm_up_time(std::time::Duration::from_secs(1));

    for &size in [32, 1024, 8192].iter() {
        messages_group.throughput(criterion::Throughput::Elements(messages_per_iter));
        messages_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            &size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let context = rt.block_on(async {
                    let context = async_nats::jetstream::new(
                        async_nats::connect(server.client_url()).await.unwrap(),
                    );

                    let stream = context
                        .create_stream(stream::Config {
                            name: "bench".to_owned(),
                            subjects: vec!["bench".to_string()],
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                    stream.purge().await.unwrap();
                    context
                });

                b.to_async(rt).iter_with_large_drop(move || {
                    let context = context.clone();
                    async move {
                        publish_async_batch(
                            context,
                            Bytes::from_static(&MSG[..size]),
                            messages_per_iter,
                        )
                        .await
                    }
                });
            },
        );
    }
    messages_group.finish();
}
async fn publish_sync_batch(context: async_nats::jetstream::Context, msg: Bytes, amount: u64) {
    for _i in 0..amount {
        context
            .publish("bench", msg.clone())
            .await
            .unwrap()
            .await
            .unwrap();
    }
}

async fn publish_async_batch(context: async_nats::jetstream::Context, msg: Bytes, amount: u64) {
    // This acts as a semaphore that does not allow for more than 10 publish acks awaiting.
    let (tx, mut rx) = tokio::sync::mpsc::channel(amount as usize);

    let handle = tokio::task::spawn(async move {
        for _ in 0..amount {
            let ack: PublishAckFuture = rx.recv().await.unwrap();
            ack.await.unwrap();
        }
    });
    for _ in 0..amount {
        let ack = context.publish("bench", msg.clone()).await.unwrap();
        tx.send(ack).await.unwrap();
    }
    handle.await.unwrap();
}

criterion_group!(jetstream, jetstream_publish_sync, jetstream_publish_async);
