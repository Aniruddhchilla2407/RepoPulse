import datetime

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from repopulse.dates import HourPartition  # noqa: E402
from repopulse.dq import run_checks  # noqa: E402

LOW = {"min_rows_per_hour": 10}


def silver_table(prefix, n=50, event_type="PushEvent", null_id=False):
    ids = [None if (null_id and i == 0) else f"{prefix}-{i}" for i in range(n)]
    return pa.table(
        {
            "event_id": pa.array(ids, type=pa.string()),
            "event_type": pa.array([event_type] * n, type=pa.string()),
            "created_at": pa.array(
                [datetime.datetime(2026, 10, 27, 0, 0, i % 60) for i in range(n)],
                type=pa.timestamp("us"),
            ),
            "repo_id": pa.array(list(range(n)), type=pa.int64()),
            "actor_id": pa.array(list(range(n)), type=pa.int64()),
        }
    )


def write_partition(base, part, table):
    directory = base / part.partition_dir
    directory.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, directory / "part-0000.snappy.parquet")


def build(tmp_path, hours=(0, 1), prefixes=None, **table_kwargs):
    parts = [HourPartition("2026-10-27", h) for h in hours]
    for part in parts:
        prefix = (prefixes or {}).get(part.hour, f"h{part.hour}")
        write_partition(tmp_path / "silver", part, silver_table(prefix, **table_kwargs))
        write_partition(tmp_path / "bronze", part, silver_table(prefix, **table_kwargs))
    return parts


def run(tmp_path, parts, thresholds=LOW):
    return {
        r.name: r
        for r in run_checks(
            parts,
            bronze_dir=tmp_path / "bronze",
            quarantine_dir=tmp_path / "quarantine",
            silver_dir=tmp_path / "silver",
            thresholds=thresholds,
        )
    }


def test_clean_data_passes_every_check(tmp_path):
    results = run(tmp_path, build(tmp_path))
    assert all(r.passed for r in results.values()), {k: v.detail for k, v in results.items()}


def test_missing_hour_fails(tmp_path):
    parts = build(tmp_path, hours=(0, 1))
    parts.append(HourPartition("2026-10-27", 2))  # never written
    assert not run(tmp_path, parts)["silver_hours_present"].passed


def test_high_quarantine_rate_fails(tmp_path):
    parts = build(tmp_path)
    write_partition(tmp_path / "quarantine", parts[0], pa.table({"raw_line": ["x"] * 40}))
    assert not run(tmp_path, parts)["quarantine_rate"].passed


def test_duplicate_event_ids_across_hours_fail(tmp_path):
    parts = build(tmp_path, prefixes={0: "same", 1: "same"})
    assert not run(tmp_path, parts)["silver_event_id_unique"].passed


def test_unexpected_event_type_fails(tmp_path):
    parts = build(tmp_path, event_type="CreateEvent")
    assert not run(tmp_path, parts)["silver_event_types"].passed


def test_null_event_id_fails(tmp_path):
    parts = build(tmp_path, null_id=True)
    assert not run(tmp_path, parts)["silver_required_not_null"].passed


def test_empty_silver_fails_without_crashing(tmp_path):
    parts = [HourPartition("2026-10-27", 0)]
    results = run(tmp_path, parts)
    assert not results["silver_hours_present"].passed
    assert not results["silver_not_empty"].passed