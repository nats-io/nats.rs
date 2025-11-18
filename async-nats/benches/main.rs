use criterion::criterion_main;

// Import the benchmark groups from both files
mod core_nats;
mod jetstream;
mod subject_validation;

criterion_main!(
    core_nats::core_nats,
    jetstream::jetstream,
    subject_validation::benches
);
