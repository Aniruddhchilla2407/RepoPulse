"""Print row counts per layer and check silver for duplicate event ids."""

from pathlib import Path

from repopulse.config import load_config
from repopulse.spark_session import build_spark


def main() -> int:
    cfg = load_config()
    spark = build_spark("repopulse-counts", cfg)
    try:
        for layer in ("bronze", "quarantine", "silver"):
            path = Path(cfg["paths"][layer])
            if not path.is_dir():
                print(f"{layer:<11} (not found)")
                continue
            if not any(path.rglob("*.parquet")):
                print(f"{layer:<11} rows={0:>12,}  (no parquet files: layer is empty)")
                continue
            try:
                df = spark.read.parquet(str(path.resolve()))
                total = df.count()
            except Exception as exc:
                print(f"{layer:<11} (unreadable: {type(exc).__name__})")
                continue
            print(f"{layer:<11} rows={total:>12,}")
            if layer == "quarantine" and total:
                df.groupBy("reason").count().orderBy("reason").show(truncate=False)
            if layer == "silver":
                distinct_ids = df.select("event_id").distinct().count()
                print(f"{'':<11} distinct event_id={distinct_ids:,}  duplicates={total - distinct_ids:,}")
                df.groupBy("event_type").count().orderBy("event_type").show(truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())