FROM ghcr.io/sdr-enthusiasts/docker-baseimage:base

ENV AR_LISTEN_UDP_ACARS=5550 \
    AR_LISTEN_TCP_ACARS=5550 \
    AR_LISTEN_UDP_VDLM2=5555 \
    AR_LISTEN_TCP_VDLM2=5555 \
    AR_SERVE_TCP_ACARS=15550 \
    AR_SERVE_TCP_VDLM2=15555 \
    AR_SERVE_ZMQ_ACARS=45550 \
    AR_SERVE_ZMQ_VDLM2=45555 \
    CARGO_HOME=/usr/local/rust \
    RUSTUP_HOME=/usr/local/rust \
    PATH=/opt/acars_router:/usr/local/rust:/usr/local/rust/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY rootfs /
COPY src/ /tmp/acars_router/src
COPY Cargo.* /tmp/acars_router/

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# hadolint ignore=DL3008,DL3003
RUN set -x && \
    TEMP_PACKAGES=(build-essential) && \
    TEMP_PACKAGES=(gcc) && \
    KEPT_PACKAGES=() && \
    TEMP_PACKAGES+=(build-essential) && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    "${KEPT_PACKAGES[@]}" \
    "${TEMP_PACKAGES[@]}" \
    && \
    # install rust
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    # build acars_router
    cd /tmp/acars_router && \
    cargo build --release && \
    # copy binary
    cp target/release/acars_router /opt/acars_router && \
    # Clean up
    #rustup self uninstall -y && \
    rm -rf /usr/local/rust && \
    apt-get remove -y "${TEMP_PACKAGES[@]}" && \
    apt-get autoremove -y && \
    rm -rf /src/* /tmp/* /var/lib/apt/lists/* && \
    # Simple date/time versioning
    date +%Y%m%d.%H%M > /IMAGE_VERSION
