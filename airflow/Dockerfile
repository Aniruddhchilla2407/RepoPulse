FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Pipeline tooling lives in its own virtualenv so it cannot conflict with Airflow's pinned packages.
RUN python -m venv /home/airflow/pipeline-venv \
    && /home/airflow/pipeline-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/pipeline-venv/bin/pip install --no-cache-dir \
        "pyspark==3.5.3" \
        "pyyaml>=6.0" \
        "requests>=2.31" \
        "duckdb>=1.1" \
        "pyarrow>=15" \
        "dbt-core>=1.10,<1.11" \
        "dbt-duckdb>=1.9,<2"

# Empty directory for the optional named volume (see REPOPULSE_DB in docker-compose.yml)
RUN mkdir -p /opt/airflow/warehouse