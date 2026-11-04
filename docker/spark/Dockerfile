FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PYSPARK_PYTHON=python3

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# Keep these pins in sync with pyproject.toml
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    pyyaml==6.0.2 \
    requests==2.32.3 \
    pytest==8.3.3 \
    ruff==0.7.4 \
    numpy>=1.21.0 \
    pandas>=2.0.0 \
    pyarrow>=15.0.0 \
    scikit-learn>=1.3.0

WORKDIR /app