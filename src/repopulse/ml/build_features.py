"""Build the ML feature/label table in the warehouse and export it for SparkML."""

import argparse
import json
import logging
import time
from datetime import timedelta, timezone
from pathlib import Path

import duckdb

from repopulse import warehouse as wh
from repopulse.config import load_config, setup_logging
from repopulse.ml.spec import (
    check_contiguous,
    plan_cutoffs,
    render_features_sql,
    resolve_params,
)
from repopulse.storage import append_metrics

log = logging.getLogger("repopulse.ml.features")

FACT_TABLE = "fact_events"
FEATURES_TABLE = "ml_repo_features"
MIN_TRAIN_POSITIVES = 20
WARN_TEST_POSITIVES = 20


def data_span(con):
    """First loaded hour, end (last hour + 1h) and the number of distinct loaded source hours."""
    row = con.execute(
        f"""
SELECT MIN(h), MAX(h), COUNT(DISTINCT h)
FROM (
  SELECT CAST(source_date AS TIMESTAMP) + to_hours(source_hour) AS h
  FROM {wh.GOLD_SCHEMA}.{FACT_TABLE}
)"""
    ).fetchone()
    if row[0] is None:
        raise ValueError("gold.fact_events is empty: run the load and `dbt build` first")
    start = row[0].replace(tzinfo=timezone.utc)
    end = row[1].replace(tzinfo=timezone.utc) + timedelta(hours=1)
    return start, end, int(row[2])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build ML features and labels in the warehouse.")
    parser.add_argument("--feature-hours", type=int, default=None, help="look-back window W (hours)")
    parser.add_argument("--horizon-hours", type=int, default=None, help="label horizon N (hours)")
    parser.add_argument("--threshold", type=int, default=None, help="stars in the horizon = breakout")
    parser.add_argument("--min-events", type=int, default=None, help="min events in the window")
    parser.add_argument("--plan-only", action="store_true", help="print the plan and stop")
    parser.add_argument("--config", default=None, help="path to pipeline.yaml")
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    params = resolve_params(
        cfg.get("ml"),
        feature_hours=args.feature_hours,
        horizon_hours=args.horizon_hours,
        threshold=args.threshold,
        min_events=args.min_events,
    )

    con = wh.open_warehouse(cfg, read_only=args.plan_only)
    try:
        try:
            start, end, present = data_span(con)
            check_contiguous(present, start, end)
            cutoffs = plan_cutoffs(start, end, params.feature_hours, params.horizon_hours)
        except duckdb.CatalogException:
            log.error("gold.fact_events does not exist: run `./tasks.ps1 dbt build` first")
            return 1
        except ValueError as exc:
            log.error("%s", exc)
            return 1

        summary = {"params": params.__dict__, "cutoffs": cutoffs.as_dict()}
        if args.plan_only:
            print(json.dumps(summary, indent=2))
            return 0

        started = time.perf_counter()
        con.execute(
            render_features_sql(wh.GOLD_SCHEMA, FACT_TABLE, FEATURES_TABLE, cutoffs, params)
        )
        balance = {
            split: {"rows": int(n), "positives": int(pos)}
            for split, n, pos in con.execute(
                f"SELECT split, COUNT(*), COALESCE(SUM(label), 0) "
                f"FROM {wh.GOLD_SCHEMA}.{FEATURES_TABLE} GROUP BY split"
            ).fetchall()
        }
        summary["class_balance"] = balance

        train_pos = balance.get("train", {}).get("positives", 0)
        test_pos = balance.get("test", {}).get("positives", 0)
        if train_pos < MIN_TRAIN_POSITIVES:
            log.error(
                "only %d positive training examples (need >= %d). Lower --threshold "
                "(currently %d stars) or use longer windows.",
                train_pos, MIN_TRAIN_POSITIVES, params.threshold,
            )
            print(json.dumps(summary, indent=2))
            return 3
        if test_pos < WARN_TEST_POSITIVES:
            log.warning("only %d positives in the test split: metrics will be very noisy", test_pos)

        out_dir = Path(cfg["paths"].get("ml", "data/ml"))
        out_dir.mkdir(parents=True, exist_ok=True)
        target = (out_dir / "features.parquet").resolve().as_posix().replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM {wh.GOLD_SCHEMA}.{FEATURES_TABLE}) TO '{target}' (FORMAT PARQUET)"
        )
        summary["rows_exported"] = sum(b["rows"] for b in balance.values())
        summary["seconds"] = round(time.perf_counter() - started, 2)
    finally:
        con.close()

    append_metrics(Path(cfg["paths"]["metrics"]) / "ml_runs.jsonl", {"job": "ml_features", **summary})
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())