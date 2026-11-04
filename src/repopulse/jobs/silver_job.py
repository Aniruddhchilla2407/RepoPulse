"""Silver job: bronze Parquet -> deduplicated, explicitly typed silver Parquet."""

import argparse
import json
import logging
import time
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import SparkSession

from repopulse.config import load_config, setup_logging
from repopulse.dates import HourPartition, add_range_args, partitions_from_args
from repopulse.schemas import PARTITION_COLUMNS
from repopulse.spark_session import build_spark
from repopulse.storage import (
    append_metrics,
    existing_partitions,
    partition_path,
    purge_partitions,
)
from repopulse.transforms.silver import build_silver

log = logging.getLogger("repopulse.silver")


def run(
    spark: SparkSession,
    partitions: Sequence[HourPartition],
    *,
    bronze_dir: Path,
    silver_dir: Path,
    metrics_file: Path | None = None,
) -> dict:
    bronze_dir, silver_dir = Path(bronze_dir), Path(silver_dir)
    available = existing_partitions(bronze_dir, partitions)
    missing = [p.partition_dir for p in partitions if p not in available]
    stats = {
        "hours_requested": len(partitions),
        "hours_processed": len(available),
        "hours_without_bronze": missing,
        "rows_in": 0,
        "rows_out": 0,
        "duplicates_removed": 0,
        "seconds": 0.0,
        "rows_per_sec": 0.0,
    }
    if missing:
        log.warning("no bronze data for partition(s), skipping: %s", missing)
    if not available:
        log.error("no bronze partitions found for the requested range; run the bronze step first")
        return stats

    started = time.perf_counter()
    paths = [str(partition_path(bronze_dir, p).resolve()) for p in available]
    bronze = spark.read.option("basePath", str(bronze_dir.resolve())).parquet(*paths)
    rows_in = bronze.count()

    silver = build_silver(bronze)

    # Idempotency: replace exactly the partitions in this range. The DataFrame above reads
    # bronze, not silver, so purging silver here is safe.
    purge_partitions(silver_dir, available)
    silver.write.mode("overwrite").partitionBy(*PARTITION_COLUMNS).parquet(str(silver_dir.resolve()))

    written = existing_partitions(silver_dir, available)
    rows_out = 0
    if written:
        rows_out = spark.read.parquet(
            *[str(partition_path(silver_dir, p).resolve()) for p in written]
        ).count()

    elapsed = time.perf_counter() - started
    stats.update(
        rows_in=rows_in,
        rows_out=rows_out,
        duplicates_removed=rows_in - rows_out,
        seconds=round(elapsed, 2),
        rows_per_sec=round(rows_in / elapsed, 1) if elapsed > 0 else 0.0,
    )
    append_metrics(metrics_file, {"job": "silver", **stats})
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bronze Parquet -> silver Parquet.")
    add_range_args(parser)
    parser.add_argument("--config", default=None, help="path to pipeline.yaml")
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    partitions = partitions_from_args(args)
    spark = build_spark("repopulse-silver", cfg)
    try:
        stats = run(
            spark,
            partitions,
            bronze_dir=Path(cfg["paths"]["bronze"]),
            silver_dir=Path(cfg["paths"]["silver"]),
            metrics_file=Path(cfg["paths"]["metrics"]) / "silver_runs.jsonl",
        )
    finally:
        spark.stop()
    print(json.dumps(stats, indent=2))
    return 0 if stats["hours_processed"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())