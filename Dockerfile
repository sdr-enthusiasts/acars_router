FROM python:3-slim

ENV AR_LISTEN_UDP_ACARS=5550 \
    AR_LISTEN_TCP_ACARS=5550 \
    AR_LISTEN_UDP_VDLM2=5555 \
    AR_LISTEN_TCP_VDLM2=5555 \
    AR_SERVE_TCP_ACARS=15550 \
    AR_SERVE_TCP_VDLM2=15555

COPY acars_router/requirements.txt /opt/acars_router/requirements.txt

RUN set -x && \
    python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir --requirement /opt/acars_router/requirements.txt

COPY acars_router/ /opt/acars_router

ENTRYPOINT [ "python3" ]

CMD [ "/opt/acars_router/acars_router.py" ]
