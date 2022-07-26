# FROM rust:1.62-bullseye as builder
# WORKDIR /tmp/acars_router
# # hadolint ignore=DL3008,DL3003,SC1091
# RUN set -x && \
#     apt-get update && \
#     apt-get install -y --no-install-recommends libzmq3-dev
# COPY . .

# RUN cargo build --release

FROM ghcr.io/sdr-enthusiasts/docker-baseimage:base

ENV AR_LISTEN_UDP_ACARS=5550 \
    AR_LISTEN_TCP_ACARS=5550 \
    AR_LISTEN_UDP_VDLM2=5555 \
    AR_LISTEN_TCP_VDLM2=5555 \
    AR_SERVE_TCP_ACARS=15550 \
    AR_SERVE_TCP_VDLM2=15555 \
    AR_SERVE_ZMQ_ACARS=45550 \
    AR_SERVE_ZMQ_VDLM2=45555

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
COPY ./rootfs /
# COPY --from=builder /tmp/acars_router/target/release/acars_router /opt/acars_router

COPY ./target/armv7-unknown-linux-gnueabihf/release/acars_router /opt/acars_router.armv7
COPY ./target/aarch64-unknown-linux-gnu/release/acars_router /opt/acars_router.aarch64
COPY ./target/release/acars_router /opt/acars_router.x86_64

# hadolint ignore=DL3008,DL3003,SC1091
RUN set -x && \
    KEPT_PACKAGES=() && \
    KEPT_PACKAGES+=(libzmq5) && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    "${KEPT_PACKAGES[@]}" \
    "${TEMP_PACKAGES[@]}"\
    && \
    # Simple date/time versioning
    date +%Y%m%d.%H%M > /IMAGE_VERSION && \
    apt-get autoremove -y && \
    rm -rf /src/* /tmp/* /var/lib/apt/lists/*
