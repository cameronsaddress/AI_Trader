FROM rustlang/rust:nightly-slim AS base
WORKDIR /usr/src/app
# Install system dependencies required for compiling network/ssl crates
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Development Stage (for cargo run)
FROM base AS dev
RUN groupadd -g 1000 app && useradd -m -u 1000 -g app app
COPY --chown=app:app . .
USER app
CMD ["cargo", "run"]

# Builder Stage (for production)
FROM base AS builder
COPY . .
RUN cargo install --path .

# Production Stage
FROM debian:bookworm-slim AS production
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/arb-scanner /usr/local/bin/arb-scanner
RUN groupadd -g 10001 app && useradd -m -u 10001 -g app app
USER app
CMD ["arb-scanner"]
