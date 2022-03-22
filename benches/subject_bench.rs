use criterion::{criterion_group, criterion_main, Criterion};
use nats::Subject;

const WIRETAP: &str = ">";
const MULTIPLE_SINGLE_WILDCARDS: &str = "fAN.*.sdb.*";
const SHORT_SUBJECT: &str = "$SYS.REQ.SERVER.PING";
const SUBJECT_MULTI_WILDCARD: &str = "id.00112233445566778899aabbccddeeff.token1.token2.token3.token4.>";
const LONG_SUBJECT: &str = "id.00112233445566778899aabbccddeeff.info.token1.*.tokenabzdd.!§$toke.*.kjadslfha.iiiiiäöü.--__--.*.%%&jjkkll.>";

fn validate(c: &mut Criterion) {
    let mut group = c.benchmark_group("validate");
    group.bench_function("wiretap", |b| b.iter(|| Subject::new(WIRETAP).is_ok()));
    group.bench_function("short mixed", |b| b.iter(|| Subject::new(MULTIPLE_SINGLE_WILDCARDS).is_ok()));
    group.bench_function("short", |b| b.iter(|| Subject::new(SHORT_SUBJECT).is_ok()));
    group.bench_function("mw", |b| b.iter(|| Subject::new(SUBJECT_MULTI_WILDCARD).is_ok()));
    group.bench_function("long", |b| b.iter(|| Subject::new(LONG_SUBJECT).is_ok()));
}

criterion_group!(
    benches,
    validate
);
criterion_main!(benches);
