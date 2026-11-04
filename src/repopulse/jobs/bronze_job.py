"""Bronze job: raw JSON.gz -> Parquet (partitioned by source date/hour) + quarantine."""

import argparse
import json
import logging
import time
from collections.abc import Iterable, Sequence
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from repopulse.config import load_config, setup_logging
from repopulse.dates import HourPartition, add_range_args, partitions_from_args
from repopulse.schemas import PARTITION_COLUMNS
from repopulse.spark_session import build_spark
from repopulse.storage import append_metrics, purge_partitions
from repopulse.transforms.bronze import parse_lines, quarantined, valid_events

log = logging.getLogger("repopulse.bronze")


def run(
    spark: SparkSession,
    partitions: Sequence[HourPartition],
    *,
    raw_dir: Path,
    bronze_dir: Path,
    quarantine_dir: Path,
    event_types: Iterable[str],
    metrics_file: Path | None = None,
) -> dict:
    raw_dir, bronze_dir, quarantine_dir = Path(raw_dir), Path(bronze_dir), Path(quarantine_dir)
    available = [p for p in partitions if (raw_dir / p.filename).is_file()]
    missing = [p.filename for p in partitions if p not in available]
    stats = {
        "hours_requested": len(partitions),
        "hours_processed": len(available),
        "hours_missing": missing,
        "lines_read": 0,
        "kept": 0,
        "quarantined": 0,
        "dropped_by_type_filter": 0,
        "seconds": 0.0,
        "lines_per_sec": 0.0,
    }
    if missing:
        log.warning("raw file(s) not found, skipping: %s", missing)
    if not available:
        log.error("no raw files found for the requested range; run the ingest step first")
        return stats

    types = list(event_types)
    files = [str((raw_dir / p.filename).resolve()) for p in available]
    started = time.perf_counter()

    raw = spark.read.text(files).withColumn("source_file", F.input_file_name())
    parsed = parse_lines(raw).persist(StorageLevel.DISK_ONLY)
    try:
        counts = parsed.agg(
            F.count(F.lit(1)).alias("lines_read"),
            F.sum(F.when(F.col("quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined"),
            F.sum(
                F.when(
                    F.col("quarantine_reason").isNull() & F.col("type").isin(types), 1
                ).otherwise(0)
            ).alias("kept"),
        ).first()

        # Idempotency: drop exactly the hour partitions we are about to rewrite, so an hour
        # that now yields zero rows doesn't keep stale data from a previous run.
        purge_partitions(bronze_dir, available)
        purge_partitions(quarantine_dir, available)

        valid_events(parsed, types).write.mode("overwrite").partitionBy(*PARTITION_COLUMNS).parquet(
            str(bronze_dir.resolve())
        )
        quarantined(parsed).write.mode("overwrite").partitionBy(*PARTITION_COLUMNS).parquet(
            str(quarantine_dir.resolve())
        )
    finally:
        parsed.unpersist()

    elapsed = time.perf_counter() - started
    lines_read = int(counts["lines_read"] or 0)
    n_quarantined = int(counts["quarantined"] or 0)
    n_kept = int(counts["kept"] or 0)
    stats.update(
        lines_read=lines_read,
        kept=n_kept,
        quarantined=n_quarantined,
        dropped_by_type_filter=lines_read - n_quarantined - n_kept,
        seconds=round(elapsed, 2),
        lines_per_sec=round(lines_read / elapsed, 1) if elapsed > 0 else 0.0,
    )
    append_metrics(metrics_file, {"job": "bronze", **stats})
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Raw GH Archive JSON.gz -> bronze Parquet.")
    add_range_args(parser)
    parser.add_argument("--config", default=None, help="path to pipeline.yaml")
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    partitions = partitions_from_args(args)
    spark = build_spark("repopulse-bronze", cfg)
    try:
        stats = run(
            spark,
            partitions,
            raw_dir=Path(cfg["paths"]["raw"]),
            bronze_dir=Path(cfg["paths"]["bronze"]),
            quarantine_dir=Path(cfg["paths"]["quarantine"]),
            event_types=cfg["event_types"],
            metrics_file=Path(cfg["paths"]["metrics"]) / "bronze_runs.jsonl",
        )
    finally:
        spark.stop()
    print(json.dumps(stats, indent=2))
    return 0 if stats["hours_processed"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())