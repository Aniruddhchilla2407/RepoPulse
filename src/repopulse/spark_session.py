"""One place to build a tuned local SparkSession."""

import os
from typing import Any

from pyspark.sql import SparkSession


def build_spark(
    app_name: str,
    cfg: dict[str, Any] | None = None,
    *,
    master: str | None = None,
    driver_memory: str | None = None,
    shuffle_partitions: int | None = None,
) -> SparkSession:
    spark_cfg = (cfg or {}).get("spark", {})
    master = master or spark_cfg.get("master", "local[*]")
    driver_memory = (
        driver_memory or os.environ.get("SPARK_DRIVER_MEMORY") or spark_cfg.get("driver_memory", "3g")
    )
    shuffle_partitions = shuffle_partitions or int(spark_cfg.get("shuffle_partitions", 8))

    conf = {
        "spark.driver.memory": driver_memory,
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.ui.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.sql.adaptive.enabled": "true",
        # Re-running a range replaces only the partitions being written
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        # Partition columns stay strings (no guessing '2024-06-01' is a date)
        "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.local.dir": "/tmp/spark",
    }

    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark