"""Write batch scores into the warehouse and compute comparable metrics for every model."""

import argparse
import json
import logging
from pathlib import Path

from repopulse import warehouse as wh
from repopulse.config import load_config, setup_logging
from repopulse.ml import metrics as M

log = logging.getLogger("repopulse.ml.evaluate")

FEATURES_TABLE = f"{wh.GOLD_SCHEMA}.ml_repo_features"
SCORES_TABLE = f"{wh.GOLD_SCHEMA}.repo_breakout_scores"
MODELS = {  # model name -> column holding its score
    "gbt_sparkml": "score_gbt",
    "logreg_baseline": "score_logreg",
    "heuristic_stars_in_window": "stars_w",
}
COLUMNS = ("split", "label", "stars_w", "score_gbt", "score_logreg")


def scores_sql(scores_glob: str) -> str:
    return f"""CREATE OR REPLACE TABLE {SCORES_TABLE} AS
SELECT
  f.repo_id,
  f.cutoff_ts,
  f.split,
  f.label,
  f.stars_next,
  f.stars_w,
  s.score_gbt,
  s.score_logreg,
  CAST(now() AS TIMESTAMP) AS scored_at
FROM {FEATURES_TABLE} AS f
LEFT JOIN read_parquet('{scores_glob}') AS s
  ON s.repo_id = f.repo_id AND s.split = f.split
WHERE f.split IN ('valid', 'test', 'score')
ORDER BY f.split, f.repo_id"""


def _fmt(value, pct: bool = False) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.1f}%" if pct else f"{value:.4f}"


def markdown_table(results: dict[str, dict]) -> str:
    header = (
        "| model | AUC-ROC | AUC-PR | precision | recall | precision@top-k | lift@top-k |\n"
        "|---|---|---|---|---|---|---|\n"
    )
    rows = [
        f"| {name} | {_fmt(r['auc_roc'])} | {_fmt(r['auc_pr'])} | {_fmt(r['precision'])} | "
        f"{_fmt(r['recall'])} | {_fmt(r['precision_at_k'])} | {_fmt(r['lift_at_k'])} |"
        for name, r in results.items()
    ]
    return header + "\n".join(rows) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write scores to the warehouse and evaluate.")
    parser.add_argument("--top-fraction", type=float, default=0.01, help="top share for precision@k")
    parser.add_argument("--config", default=None, help="path to pipeline.yaml")
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    ml_dir = Path(cfg["paths"].get("ml", "data/ml"))
    scores_dir = ml_dir / "scores_sparkml"
    if not any(scores_dir.glob("*.parquet")):
        log.error("no Parquet in %s: run `./tasks.ps1 ml-train` first", scores_dir)
        return 1
    glob = (scores_dir / "*.parquet").resolve().as_posix().replace("'", "''")

    con = wh.open_warehouse(cfg)
    try:
        con.execute(scores_sql(glob))
        by_split = {
            split: {"rows": int(n), "scored": int(scored)}
            for split, n, scored in con.execute(
                f"SELECT split, COUNT(*), COUNT(score_gbt) FROM {SCORES_TABLE} GROUP BY split"
            ).fetchall()
        }
        rows = con.execute(
            f"SELECT {', '.join(COLUMNS)} FROM {SCORES_TABLE} WHERE split IN ('valid', 'test')"
        ).fetchall()
    finally:
        con.close()

    index = {name: i for i, name in enumerate(COLUMNS)}
    results: dict[str, dict] = {}
    for name, column in MODELS.items():
        pairs = {
            split: [
                (r[index["label"]], r[index[column]])
                for r in rows
                if r[index["split"]] == split
                and r[index["label"]] is not None
                and r[index[column]] is not None
            ]
            for split in ("valid", "test")
        }
        if not pairs["valid"] or not pairs["test"]:
            log.warning("skipping %s: no scored rows", name)
            continue
        valid = tuple(zip(*pairs["valid"]))
        test = tuple(zip(*pairs["test"]))
        results[name] = M.evaluate(valid, test, args.top_fraction)

    comparison = {}
    main_model = results.get("gbt_sparkml")
    for other in ("logreg_baseline", "heuristic_stars_in_window"):
        if main_model and other in results:
            comparison[f"gbt_sparkml_vs_{other}_pct"] = {
                metric: M.relative_change(main_model[metric], results[other][metric])
                for metric in ("auc_roc", "auc_pr", "precision_at_k")
            }

    report = {
        "scores_table": SCORES_TABLE,
        "rows_per_split": by_split,
        "models": results,
        "comparison": comparison,
        "note": "Threshold chosen on 'valid' (best F1) and applied to 'test'. Check n_pos_test "
        "before trusting any number: small positive counts make these metrics noisy.",
    }
    (ml_dir / "metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (ml_dir / "metrics.md").write_text(markdown_table(results), encoding="utf-8")
    print(markdown_table(results))
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())