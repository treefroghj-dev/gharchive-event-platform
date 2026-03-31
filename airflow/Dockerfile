FROM apache/airflow:2.11.2

ENV AIRFLOW_HOME=/opt/airflow
ENV APP_HOME=/opt/app
ENV PYTHONPATH=/opt/app/src
ENV DBT_PROJECT_DIR=/opt/app/gharchive_dbt
ENV DBT_PROFILES_DIR=/opt/app/gharchive_dbt
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR ${APP_HOME}

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        curl \
        vim \
        openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:root pyproject.toml README.md ${APP_HOME}/
COPY --chown=airflow:root src ${APP_HOME}/src
COPY --chown=airflow:root gharchive_dbt ${APP_HOME}/gharchive_dbt
COPY --chown=airflow:root airflow/dags ${AIRFLOW_HOME}/dags

RUN chown -R airflow:root ${APP_HOME} ${AIRFLOW_HOME} \
    && chmod -R g+rwX ${APP_HOME} ${AIRFLOW_HOME}

USER airflow

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    && pip install --no-cache-dir .

WORKDIR ${AIRFLOW_HOME}