import gzip

from repopulse.config import DEFAULT_EVENT_TYPES
from repopulse.dates import HourPartition
from repopulse.jobs import bronze_job, silver_job


def test_rerun_is_idempotent(spark, tmp_path, sample_lines):
    part = HourPartition("2024-06-01", 0)
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    lines = sample_lines + ["", "   "]  # 15 lines in total
    with gzip.open(raw_dir / part.filename, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    bronze_dir = tmp_path / "bronze"
    quarantine_dir = tmp_path / "quarantine"
    silver_dir = tmp_path / "silver"

    for _ in range(2):
        b = bronze_job.run(
            spark,
            [part],
            raw_dir=raw_dir,
            bronze_dir=bronze_dir,
            quarantine_dir=quarantine_dir,
            event_types=DEFAULT_EVENT_TYPES,
        )
        s = silver_job.run(spark, [part], bronze_dir=bronze_dir, silver_dir=silver_dir)

        assert b["lines_read"] == 15
        assert b["quarantined"] == 6
        assert b["kept"] == 8
        assert b["dropped_by_type_filter"] == 1
        assert s["rows_in"] == 8
        assert s["rows_out"] == 7

        # the on-disk result is the same after every run: nothing is appended twice
        assert spark.read.parquet(str(bronze_dir)).count() == 8
        assert spark.read.parquet(str(quarantine_dir)).count() == 6
        assert spark.read.parquet(str(silver_dir)).count() == 7