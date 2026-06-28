FROM apache/airflow:2.9.3-python3.11

ARG SPARK_VERSION=3.5.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark==4.8.0" \
    "apache-airflow-providers-postgres==5.11.2" \
    "pandas==2.1.4" \
    "psycopg2-binary==2.9.9" \
    "pyspark==${SPARK_VERSION}" \
    "requests==2.32.3" \
    "SQLAlchemy==1.4.52"

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
