"""Data-quality gate between silver and the warehouse load. Exits non-zero if any check fails."""

import argparse
import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from repopulse.config import DEFAULT_EVENT_TYPES, load_config, setup_logging
from repopulse.dates import HourPartition, add_range_args, partitions_from_args
from repopulse.storage import partition_path

log = logging.getLogger("repopulse.dq")

DEFAULT_THRESHOLDS = {
    "min_rows_per_hour": 1000,
    "max_quarantine_rate": 0.01,
    "max_dedup_loss_rate": 0.05,
    "max_key_null_rate": 0.001,
}
ALWAYS_PRESENT = ("event_id", "event_type", "created_at")
MOSTLY_PRESENT = ("repo_id", "actor_id")


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def _parquet_files(directory: Path) -> list[Path]:
    directory = Path(directory)
    return sorted(directory.glob("*.parquet")) if directory.is_dir() else []


def count_rows(base: Path, partitions: Iterable[HourPartition]) -> dict[HourPartition, int]:
    """Row counts from Parquet footers: no data is read."""
    return {
        p: sum(pq.ParquetFile(f).metadata.num_rows for f in _parquet_files(partition_path(base, p)))
        for p in partitions
    }


def read_columns(base: Path, partitions: Iterable[HourPartition], columns: Sequence[str]) -> pa.Table:
    tables = []
    for p in partitions:
        for f in _parquet_files(partition_path(base, p)):
            tables.append(pq.ParquetFile(f).read(columns=list(columns)))
    if not tables:
        return pa.table({c: pa.array([], type=pa.null()) for c in columns})
    return pa.concat_tables(tables)


def run_checks(
    partitions: Iterable[HourPartition],
    *,
    bronze_dir: Path,
    quarantine_dir: Path,
    silver_dir: Path,
    allowed_types: Iterable[str] = DEFAULT_EVENT_TYPES,
    thresholds: dict | None = None,
) -> list[CheckResult]:
    t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    partitions = list(partitions)
    silver_counts = count_rows(Path(silver_dir), partitions)
    silver_total = sum(silver_counts.values())
    bronze_total = sum(count_rows(Path(bronze_dir), partitions).values())
    quarantine_total = sum(count_rows(Path(quarantine_dir), partitions).values())
    results: list[CheckResult] = []

    thin = {p.partition_dir: n for p, n in silver_counts.items() if n < t["min_rows_per_hour"]}
    results.append(
        CheckResult(
            "silver_hours_present",
            not thin,
            f"all {len(partitions)} hours have >= {t['min_rows_per_hour']} rows"
            if not thin
            else f"hours below the minimum: {thin}",
        )
    )

    seen = bronze_total + quarantine_total
    q_rate = quarantine_total / seen if seen else 1.0
    results.append(
        CheckResult(
            "quarantine_rate",
            seen > 0 and q_rate <= t["max_quarantine_rate"],
            f"{quarantine_total} of {seen} lines quarantined "
            f"({q_rate:.3%}, max {t['max_quarantine_rate']:.3%})",
        )
    )

    loss = (bronze_total - silver_total) / bronze_total if bronze_total else 1.0
    results.append(
        CheckResult(
            "silver_vs_bronze",
            bronze_total > 0 and 0 <= loss <= t["max_dedup_loss_rate"],
            f"bronze={bronze_total} silver={silver_total} loss={loss:.3%} "
            f"(max {t['max_dedup_loss_rate']:.3%})",
        )
    )

    if silver_total == 0:
        results.append(CheckResult("silver_not_empty", False, "no silver rows for this range"))
        return results

    table = read_columns(Path(silver_dir), partitions, ALWAYS_PRESENT + MOSTLY_PRESENT)
    n = table.num_rows

    found = set(pc.unique(table["event_type"]).to_pylist()) - {None}
    unexpected = found - set(allowed_types)
    results.append(
        CheckResult(
            "silver_event_types",
            not unexpected,
            f"types found: {sorted(found)}" if not unexpected else f"unexpected types: {sorted(unexpected)}",
        )
    )

    bad = {c: table[c].null_count for c in ALWAYS_PRESENT if table[c].null_count > 0}
    results.append(
        CheckResult(
            "silver_required_not_null",
            not bad,
            f"{', '.join(ALWAYS_PRESENT)} have no nulls" if not bad else f"null counts: {bad}",
        )
    )

    rates = {c: table[c].null_count / n for c in MOSTLY_PRESENT}
    over = {c: f"{r:.3%}" for c, r in rates.items() if r > t["max_key_null_rate"]}
    results.append(
        CheckResult(
            "silver_key_null_rate",
            not over,
            f"null rates {{{', '.join(f'{c}: {r:.3%}' for c, r in rates.items())}}}"
            if not over
            else f"above {t['max_key_null_rate']:.3%}: {over}",
        )
    )

    non_null = n - table["event_id"].null_count
    distinct = pc.count_distinct(table["event_id"]).as_py()
    results.append(
        CheckResult(
            "silver_event_id_unique",
            distinct == non_null,
            f"{distinct} distinct of {non_null} event ids across the range",
        )
    )
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Data-quality gate for a range of hours.")
    add_range_args(parser)
    parser.add_argument("--config", default=None, help="path to pipeline.yaml")
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    results = run_checks(
        partitions_from_args(args),
        bronze_dir=Path(cfg["paths"]["bronze"]),
        quarantine_dir=Path(cfg["paths"]["quarantine"]),
        silver_dir=Path(cfg["paths"]["silver"]),
        allowed_types=cfg["event_types"],
        thresholds=cfg.get("dq"),
    )
    passed = all(r.passed for r in results)
    print(json.dumps({"passed": passed, "checks": [asdict(r) for r in results]}, indent=2))
    if not passed:
        log.error("data-quality gate FAILED: %s", [r.name for r in results if not r.passed])
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())