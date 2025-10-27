use criterion::criterion_main;

// Import the benchmark groups from both files
mod core_nats;
mod jetstream;

#[cfg(target_os = "linux")]
criterion_main!(
    core_nats::core_nats,
    core_nats::core_nats_uring,
    jetstream::jetstream
);

#[cfg(not(target_os = "linux"))]
criterion_main!(core_nats::core_nats, jetstream::jetstream);
