use criterion::criterion_main;

// Import the benchmark groups from both files
mod core_nats;
mod jetstream;

criterion_main!(core_nats::core_nats, jetstream::jetstream);
