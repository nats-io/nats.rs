FROM rust:1.71.1
WORKDIR /usr/src/nats.rs/async-nats
ARG PROFILE=test
COPY . /usr/src/nats.rs
RUN cargo test $PROFILE --no-run
ENV NATS_URL=localhost:4222
CMD ["cargo", "test", "--$PROFILE", "compatibility", "--", "--nocapture"]