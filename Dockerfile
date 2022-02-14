FROM python:3-slim

# COPY acars_router/requirements.txt /opt/acars_router/requirements.txt

# RUN set -x && \
#     python3 -m pip install --requirement /opt/acars_router/requirements.txt

COPY acars_router/ /opt/acars_router

ENTRYPOINT [ "python3" ]

CMD [ "/opt/acars_router/acars_router.py" ]
