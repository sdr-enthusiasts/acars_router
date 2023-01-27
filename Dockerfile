FROM ghcr.io/sdr-enthusiasts/docker-baseimage:base

ENV AR_LISTEN_UDP_ACARS=5550 \
    AR_LISTEN_TCP_ACARS=5550 \
    AR_LISTEN_UDP_VDLM2=5555 \
    AR_LISTEN_TCP_VDLM2=5555 \
    AR_SERVE_TCP_ACARS=15550 \
    AR_SERVE_TCP_VDLM2=15555 \
    AR_SERVE_ZMQ_ACARS=45550 \
    AR_SERVE_ZMQ_VDLM2=45555 \
    AR_ADD_PROXY_ID=true

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
COPY ./rootfs /
COPY ./bin/acars_router.armv7/acars_router /opt/acars_router.armv7
COPY ./bin/acars_router.arm64/acars_router /opt/acars_router.arm64
COPY ./bin/acars_router.amd64/acars_router /opt/acars_router.amd64

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
    /opt/acars_router.armv7 \
    /opt/acars_router.arm64 \
    /opt/acars_router.amd64 \
    && \
    # remove foreign architecture binaries
    /rename_current_arch_binary.sh && \
    rm -fv \
    /opt/acars_router.* \
    && \
    # clean up
    apt-get remove -y "${TEMP_PACKAGES[@]}" && \
    apt-get autoremove -y && \
    rm -rf /src/* /tmp/* /var/lib/apt/lists/* && \
    # test
    /opt/acars_router --version
