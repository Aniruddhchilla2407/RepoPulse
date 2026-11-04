"""Load local silver Parquet into the DuckDB warehouse. One hour = one replaceable partition."""

import argparse
import json
import logging
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb

from repopulse import warehouse as wh
from repopulse.config import load_config, setup_logging
from repopulse.dates import HourPartition, add_range_args, partitions_from_args
from repopulse.storage import append_metrics, partition_path

log = logging.getLogger("repopulse.load_warehouse")

TABLE = f"{wh.RAW_SCHEMA}.{wh.RAW_TABLE}"
PARTITION_COLUMNS = ("source_date", "source_hour")

# (column, DuckDB type). Timestamps are UTC, stored without a time zone.
COLUMNS: list[tuple[str, str]] = [
    ("event_id", "VARCHAR"),
    ("event_type", "VARCHAR"),
    ("created_at", "TIMESTAMP"),
    ("event_date", "DATE"),
    ("actor_id", "BIGINT"),
    ("actor_login", "VARCHAR"),
    ("repo_id", "BIGINT"),
    ("repo_name", "VARCHAR"),
    ("org_id", "BIGINT"),
    ("org_login", "VARCHAR"),
    ("is_public", "BOOLEAN"),
    ("push_ref", "VARCHAR"),
    ("push_size", "BIGINT"),
    ("push_distinct_size", "BIGINT"),
    ("pr_action", "VARCHAR"),
    ("pr_number", "BIGINT"),
    ("pr_merged", "BOOLEAN"),
    ("pr_additions", "BIGINT"),
    ("pr_deletions", "BIGINT"),
    ("pr_changed_files", "BIGINT"),
    ("issue_action", "VARCHAR"),
    ("issue_number", "BIGINT"),
    ("watch_action", "VARCHAR"),
    ("fork_repo_id", "BIGINT"),
    ("fork_repo_name", "VARCHAR"),
    ("source_file", "VARCHAR"),
    ("source_date", "DATE"),
    ("source_hour", "INTEGER"),
]


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {wh.RAW_SCHEMA}")
    columns = ",\n  ".join(f"{name} {sql_type}" for name, sql_type in COLUMNS)
    con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} (\n  {columns}\n)")


def parquet_glob(silver_dir: Path, part: HourPartition) -> str:
    glob = (partition_path(Path(silver_dir), part) / "*.parquet").as_posix()
    return glob.replace("'", "''")


def has_data(silver_dir: Path, part: HourPartition) -> bool:
    directory = partition_path(Path(silver_dir), part)
    return directory.is_dir() and any(directory.glob("*.parquet"))


def insert_sql(silver_dir: Path, part: HourPartition) -> str:
    """Silver files hold every column except the two partition columns (kept in folder names)."""
    select_cols = ",\n  ".join(
        f"CAST({name} AS {sql_type}) AS {name}"
        for name, sql_type in COLUMNS
        if name not in PARTITION_COLUMNS
    )
    names = ", ".join(name for name, _ in COLUMNS)
    return f"""INSERT INTO {TABLE} ({names})
SELECT
  {select_cols},
  DATE '{part.date}' AS source_date,
  {int(part.hour)} AS source_hour
FROM read_parquet('{parquet_glob(silver_dir, part)}')"""


def load_partitions(
    con: duckdb.DuckDBPyConnection, partitions: Sequence[HourPartition], silver_dir: Path
) -> dict[str, Any]:
    """Replace exactly the given hours, atomically. Returns a stats dict."""
    ensure_table(con)
    available = [p for p in partitions if has_data(silver_dir, p)]
    stats: dict[str, Any] = {
        "hours_requested": len(partitions),
        "hours_loaded": 0,
        "hours_skipped": [p.partition_dir for p in partitions if p not in available],
        "rows_loaded": 0,
        "mismatches": {},
    }
    if not available:
        return stats

    con.execute("BEGIN TRANSACTION")
    try:
        for part in available:
            con.execute(
                f"DELETE FROM {TABLE} WHERE source_date = CAST(? AS DATE) AND source_hour = ?",
                [part.date, part.hour],
            )
            con.execute(insert_sql(silver_dir, part))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    for part in available:
        local = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_glob(silver_dir, part)}')"
        ).fetchone()[0]
        loaded = con.execute(
            f"SELECT COUNT(*) FROM {TABLE} WHERE source_date = CAST(? AS DATE) AND source_hour = ?",
            [part.date, part.hour],
        ).fetchone()[0]
        stats["rows_loaded"] += int(loaded)
        if local != loaded:
            stats["mismatches"][part.partition_dir] = {"local": int(local), "warehouse": int(loaded)}
    stats["hours_loaded"] = len(available)
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load local silver Parquet into DuckDB.")
    add_range_args(parser)
    parser.add_argument("--config", default=None, help="path to pipeline.yaml")
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    partitions = partitions_from_args(args)
    silver_dir = Path(cfg["paths"]["silver"])

    started = time.perf_counter()
    con = wh.open_warehouse(cfg)
    try:
        stats = load_partitions(con, partitions, silver_dir)
        total = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    finally:
        con.close()

    elapsed = time.perf_counter() - started
    stats.update(
        warehouse_path=wh.settings(cfg)["path"],
        table=TABLE,
        table_rows_total=int(total),
        seconds=round(elapsed, 2),
        rows_per_sec=round(stats["rows_loaded"] / elapsed, 1) if elapsed > 0 else 0.0,
    )
    if stats["hours_skipped"]:
        log.warning("no local silver data for: %s", stats["hours_skipped"])
    append_metrics(Path(cfg["paths"]["metrics"]) / "load_runs.jsonl", {"job": "load_warehouse", **stats})
    print(json.dumps(stats, indent=2))
    return 1 if stats["hours_loaded"] == 0 or stats["mismatches"] else 0


if __name__ == "__main__":
    raise SystemExit(main())