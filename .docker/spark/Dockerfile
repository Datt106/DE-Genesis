FROM python:3.11-slim-bullseye AS python-runtime

FROM apache/spark:3.5.1

USER root
COPY --from=python-runtime /usr/local /usr/local

ENV PATH="/usr/local/bin:${PATH}" \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_PYTHON=/usr/local/bin/python3.11

RUN /usr/local/bin/pip install --no-cache-dir "psycopg2-binary==2.9.9"

USER spark
