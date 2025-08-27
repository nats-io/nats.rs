use criterion::criterion_main;

// Import the benchmark groups from both files
mod core_nats;
mod jetstream;
mod subject_validation;
mod publish_validation;
mod validated_subject;

criterion_main!(core_nats::core_nats, jetstream::jetstream, subject_validation::benches, publish_validation::benches, validated_subject::benches);
