FROM rust:1.89-slim-bookworm@sha256:d7fc7de78bb8c1469933aeecbf801314d30d7d6e9f0578bba4cfa285bfa37fe6 AS build
WORKDIR /build
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+aes"
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --locked --release -j 1 && cp target/release/shopping-sessions /shopping-sessions

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8 MEM_GB=24
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
