import pytest

duckdb = pytest.importorskip("duckdb")

from repopulse import load_warehouse as lw  # noqa: E402
from repopulse import warehouse as wh  # noqa: E402
from repopulse.dates import HourPartition  # noqa: E402

DAY = "2026-10-27"
H0, H1, H2 = (HourPartition(DAY, h) for h in (0, 1, 2))


def silver_select(n: int, prefix: str) -> str:
    """SQL producing silver-shaped rows (every column except the two partition columns)."""
    parts = []
    for name, sql_type in lw.COLUMNS:
        if name in lw.PARTITION_COLUMNS:
            continue
        if name == "event_id":
            expr = f"'{prefix}-' || CAST(i AS VARCHAR)"
        elif name == "event_type":
            expr = "'PushEvent'"
        elif name == "created_at":
            expr = f"TIMESTAMP '{DAY} 00:00:00' + to_seconds(i)"
        elif name == "event_date":
            expr = f"DATE '{DAY}'"
        else:
            expr = f"CAST(NULL AS {sql_type})"
        parts.append(f"{expr} AS {name}")
    return f"SELECT {', '.join(parts)} FROM range({n}) t(i)"


def write_hour(silver_dir, part, n, prefix, valid=True):
    directory = silver_dir / part.partition_dir
    directory.mkdir(parents=True, exist_ok=True)
    for old in directory.glob("*.parquet"):
        old.unlink()
    target = (directory / "part-0000.parquet").as_posix()
    query = silver_select(n, prefix) if valid else "SELECT 1 AS not_the_silver_schema"
    writer = duckdb.connect()
    try:
        writer.execute(f"COPY ({query}) TO '{target}' (FORMAT PARQUET)")
    finally:
        writer.close()


@pytest.fixture
def con():
    connection = wh.connect(":memory:", memory_limit="1GB", threads=1)
    yield connection
    connection.close()


def total(con):
    return con.execute(f"SELECT COUNT(*) FROM {lw.TABLE}").fetchone()[0]


def test_load_adds_partition_columns_and_counts_match(con, tmp_path):
    write_hour(tmp_path, H0, 5, "a")
    write_hour(tmp_path, H1, 3, "b")
    stats = lw.load_partitions(con, [H0, H1], tmp_path)

    assert stats["hours_loaded"] == 2
    assert stats["rows_loaded"] == 8
    assert stats["mismatches"] == {}
    hours = con.execute(
        f"SELECT source_hour, COUNT(*) FROM {lw.TABLE} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert hours == [(0, 5), (1, 3)]
    assert con.execute(f"SELECT DISTINCT source_date FROM {lw.TABLE}").fetchone()[0].isoformat() == DAY


def test_rerun_is_idempotent(con, tmp_path):
    write_hour(tmp_path, H0, 5, "a")
    write_hour(tmp_path, H1, 3, "b")
    lw.load_partitions(con, [H0, H1], tmp_path)
    lw.load_partitions(con, [H0, H1], tmp_path)
    assert total(con) == 8


def test_reload_replaces_only_the_requested_hour(con, tmp_path):
    write_hour(tmp_path, H0, 5, "a")
    write_hour(tmp_path, H1, 3, "b")
    lw.load_partitions(con, [H0, H1], tmp_path)

    write_hour(tmp_path, H1, 2, "b2")  # hour 1 now has fewer rows
    lw.load_partitions(con, [H1], tmp_path)

    assert total(con) == 5 + 2
    assert con.execute(f"SELECT COUNT(*) FROM {lw.TABLE} WHERE event_id LIKE 'b-%'").fetchone()[0] == 0


def test_missing_hour_is_skipped_and_existing_rows_are_kept(con, tmp_path):
    for part, n in ((H0, 5), (H1, 3), (H2, 4)):
        write_hour(tmp_path, part, n, f"h{part.hour}")
    lw.load_partitions(con, [H0, H1, H2], tmp_path)

    for old in (tmp_path / H2.partition_dir).glob("*.parquet"):
        old.unlink()
    stats = lw.load_partitions(con, [H0, H1, H2], tmp_path)

    assert stats["hours_skipped"] == [H2.partition_dir]
    assert total(con) == 5 + 3 + 4  # hour 2 keeps its previously loaded rows


def test_failed_load_rolls_back_everything(con, tmp_path):
    write_hour(tmp_path, H0, 5, "a")
    lw.load_partitions(con, [H0], tmp_path)

    write_hour(tmp_path, H0, 9, "a2")  # a valid rewrite of hour 0 ...
    write_hour(tmp_path, H1, 3, "bad", valid=False)  # ... and a file with the wrong schema
    with pytest.raises(duckdb.Error):
        lw.load_partitions(con, [H0, H1], tmp_path)

    assert total(con) == 5  # hour 0 was not replaced