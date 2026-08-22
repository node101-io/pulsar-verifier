# syntax=docker/dockerfile:1

FROM rust:1.88-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
RUN cargo build --locked --release --bin pulsar-verifier

FROM debian:bookworm-slim AS barretenberg
ARG TARGETARCH
ARG BB_VERSION=5.2.0
ARG BB_SHA256=17ab8476961728cdc5c69b6c4ff427c9092cef11d1e0b0166929a0417dfa7cfb
RUN test "$TARGETARCH" = "amd64"
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN curl --fail --location --retry 3 \
      "https://github.com/AztecProtocol/aztec-packages/releases/download/v${BB_VERSION}/barretenberg-amd64-linux.tar.gz" \
      --output /tmp/barretenberg.tar.gz \
    && echo "${BB_SHA256}  /tmp/barretenberg.tar.gz" | sha256sum --check --strict \
    && tar -xzf /tmp/barretenberg.tar.gz -C /usr/local/bin \
    && test "$(/usr/local/bin/bb --version)" = "$BB_VERSION"

# Provision only the CRS segment required by the pinned compatibility fixture.
COPY tests/fixtures/noir/bb-5.2.0 /tmp/noir-fixture
RUN mkdir -p /opt/pulsar-verifier \
    && HOME=/opt/pulsar-verifier /usr/local/bin/bb verify \
      -p /tmp/noir-fixture/proof \
      -k /tmp/noir-fixture/vk \
      -i /tmp/noir-fixture/public_inputs \
    && test -s /opt/pulsar-verifier/.bb-crs/bn254_g1_compressed.dat \
    && rm -rf /tmp/noir-fixture /tmp/barretenberg.tar.gz

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/pulsar-verifier /usr/local/bin/pulsar-verifier
COPY --from=barretenberg /usr/local/bin/bb /usr/local/bin/bb
COPY --from=barretenberg /opt/pulsar-verifier/.bb-crs /opt/pulsar-verifier/crs
COPY --chmod=0755 docker/entrypoint.sh /usr/local/bin/pulsar-verifier-entrypoint

ENTRYPOINT ["/usr/local/bin/pulsar-verifier-entrypoint"]
CMD ["run", "--config", "/etc/pulsar-verifier/config.toml"]
