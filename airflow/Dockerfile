FROM apache/airflow:2.10.5-python3.11

ENV AIRFLOW_HOME=/opt/airflow
ENV APP_HOME=/opt/app
ENV PYTHONPATH=/opt/app/src
ENV PIP_NO_CACHE_DIR=1

USER root

# Do not run apt-get: the Airflow base image pins many apt sources; `apt-get update` often fails
# during docker build with "invalid signature" (host/GPG/disk). Install JRE from Temurin instead.
RUN set -eux; \
    ARCH="$(uname -m)"; \
    case "$ARCH" in \
        x86_64) JRE_ARCH=x64 ;; \
        aarch64) JRE_ARCH=aarch64 ;; \
        *) echo "unsupported arch: $ARCH" >&2; exit 1 ;; \
    esac; \
    mkdir -p /opt/java /opt/app/jars /usr/lib/jvm; \
    curl -fsSL "https://api.adoptium.net/v3/binary/latest/17/ga/linux/${JRE_ARCH}/jre/hotspot/normal/eclipse?project=jdk" \
        -o /tmp/temurin-jre.tar.gz; \
    tar -xzf /tmp/temurin-jre.tar.gz -C /opt/java; \
    rm -f /tmp/temurin-jre.tar.gz; \
    JAVA_DIR="$(find /opt/java -maxdepth 1 -mindepth 1 -type d | head -n1)"; \
    ln -sfn "$JAVA_DIR" /usr/lib/jvm/java-17-current; \
    curl -fsSL -o /opt/app/jars/gcs-connector-hadoop3-2.2.22.jar \
        "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22.jar"; \
    chown -R airflow:root /opt/app /opt/airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-current
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /opt/app

COPY --chown=airflow:root pyproject.toml uv.lock* README.md ./
COPY --chown=airflow:root src /opt/app/src
COPY --chown=airflow:root airflow/dags /opt/airflow/dags

# Install into the image default user site-packages (~/.local). No /opt/venv = less disk during build.
USER airflow
WORKDIR /opt/app

# One `uv pip install` avoids a second `pip` pass (pip was filling /tmp and hitting ENOSPC on small Docker disks).
RUN pip install --no-cache-dir uv \
    && uv pip install --no-cache . "apache-airflow-providers-google>=10.22.0" \
    && pip uninstall -y uv \
    && rm -rf ~/.cache/pip ~/.cache/uv /tmp/* 2>/dev/null || true

WORKDIR /opt/airflow
