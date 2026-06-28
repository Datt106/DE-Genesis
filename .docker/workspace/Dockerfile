FROM python:3.11-bookworm

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        default-mysql-client \
        git \
        iputils-ping \
        jq \
        less \
        netcat-openbsd \
        openjdk-17-jdk \
        postgresql-client \
        procps \
        unzip \
        vim \
        wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN python -c "import pathlib, pyspark; pathlib.Path('/opt/spark').symlink_to(pathlib.Path(pyspark.__file__).parent, target_is_directory=True)"

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

CMD ["sleep", "infinity"]
