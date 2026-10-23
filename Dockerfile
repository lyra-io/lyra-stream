# syntax=docker/dockerfile:1
FROM rust:1.92-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler pkg-config libssl-dev clang cmake git && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p lyra-stream-server && install -m 0755 target/release/lyra-stream /usr/local/bin/lyra-stream

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/lyra-stream /usr/local/bin/lyra-stream
EXPOSE 7070
ENTRYPOINT ["/usr/local/bin/lyra-stream"]
