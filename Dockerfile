# ---- Builder ----
FROM rust:1.82-bookworm AS builder

# protoc é necessário para o build.rs do contracts-rs (prost-build).
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cargo build --release --bin server

# ---- Runtime ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/server /usr/local/bin/server

ENV RUST_LOG=info
EXPOSE 8080

CMD ["server"]
