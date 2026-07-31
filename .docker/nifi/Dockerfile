FROM apache/nifi:1.27.0

USER root
ARG POSTGRES_JDBC_VERSION=42.7.3
RUN curl -fsSL \
    "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o /opt/nifi/nifi-current/lib/postgresql.jar \
    && chown nifi:nifi /opt/nifi/nifi-current/lib/postgresql.jar
USER nifi
