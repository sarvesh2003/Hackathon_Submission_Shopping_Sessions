# syntax=docker/dockerfile:1
# Shopping Sessions: build one Rust binary and run it with no arguments.
# BIN picks the binary (reference = simple oracle, shopping = fast version).
# TARGET_CPU=x86-64-v3 (AVX2) runs on both the laptop and the scorer.

FROM rust:1.99-slim-bookworm@sha256:f2c004e3d2d8b547e8cf7ffbc29205385f4e71d23e53e4e383065db5e9f822cd AS build
ARG BIN=shopping
ARG TARGET_CPU=x86-64-v3
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    RUSTFLAGS="-C target-cpu=${TARGET_CPU}" cargo build --release --locked --bin "${BIN}" \
    && mkdir -p /out && cp "target/release/${BIN}" /out/run

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
ARG BIN=shopping
COPY --from=build /out/run /app/run
COPY scripts/diag.sh /app/diag.sh
ENV BIN=${BIN}
USER 1000:1000
CMD ["/app/diag.sh"]
