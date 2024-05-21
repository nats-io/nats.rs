FROM rust:1.78
WORKDIR /usr/src/nats.rs/async-nats
ARG PROFILE=test
COPY . /usr/src/nats.rs
RUN cargo test --features compatibility_tests --no-run
ENV NATS_URL=localhost:4222
CMD cargo test --features compatibility_tests compatibility -- --nocapture
