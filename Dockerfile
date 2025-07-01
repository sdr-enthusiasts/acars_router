FROM rust:1.88.0 AS builder
WORKDIR /tmp/acars_router
# hadolint ignore=DL3008,DL3003,SC1091
RUN set -x && \
    apt-get update && \
    apt-get install -y --no-install-recommends libzmq3-dev
COPY . .

RUN cargo build --release

FROM ghcr.io/sdr-enthusiasts/docker-baseimage:base

ENV AR_LISTEN_UDP_ACARS=5550 \
    AR_LISTEN_TCP_ACARS=5550 \
    AR_LISTEN_UDP_VDLM2=5555 \
    AR_LISTEN_TCP_VDLM2=5555 \
    AR_LISTEN_UDP_HFDL=5556 \
    AR_LISTEN_TCP_HFDL=5556 \
    AR_LISTEN_UDP_IMSL=5557 \
    AR_LISTEN_TCP_IMSL=5557 \
    AR_LISTEN_UDP_IRDM=5558 \
    AR_LISTEN_TCP_IRDM=5558 \
    AR_LISTEN_ZMQ_ACARS=35550 \
    AR_LISTEN_ZMQ_VDLM2=35555 \
    AR_LISTEN_ZMQ_HFDL=35556 \
    AR_LISTEN_ZMQ_IMSL=35557 \
    AR_LISTEN_ZMQ_IRDM=35558 \
    AR_SERVE_TCP_ACARS=15550 \
    AR_SERVE_TCP_VDLM2=15555 \
    AR_SERVE_TCP_HFDL=15556 \
    AR_SERVE_TCP_IMSL=15557 \
    AR_SERVE_TCP_IRDM=15558 \
    AR_SERVE_ZMQ_ACARS=45550 \
    AR_SERVE_ZMQ_VDLM2=45555 \
    AR_SERVE_ZMQ_HFDL=45556 \
    AR_SERVE_ZMQ_IMSL=45557 \
    AR_SERVE_ZMQ_IRDM=45558 \
    AR_ADD_PROXY_ID=true \
    AR_DISABLE_ACARS=false \
    AR_DISABLE_VDLM2=false \
    AR_DISABLE_HFDL=false \
    AR_DISABLE_IMSL=false \
    AR_DISABLE_IRDM=false

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
COPY rootfs /
COPY --from=builder /tmp/acars_router/target/release/acars_router /opt/acars_router
# hadolint ignore=DL3008,DL3003,SC1091
RUN set -x && \
    KEPT_PACKAGES=() && \
    TEMP_PACKAGES=() && \
    KEPT_PACKAGES+=(libzmq5) && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    "${KEPT_PACKAGES[@]}" \
    "${TEMP_PACKAGES[@]}"\
    && \
    # ensure binaries are executable
    chmod -v a+x \
    /opt/acars_router \
    && \
    # clean up
    apt-get remove -y "${TEMP_PACKAGES[@]}" && \
    apt-get autoremove -y && \
    rm -rf /src/* /tmp/* /var/lib/apt/lists/*
