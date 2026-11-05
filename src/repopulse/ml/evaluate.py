"""Write batch scores into the warehouse and compute comparable metrics for every model."""

import argparse
import json
import logging
from pathlib import Path

from repopulse import warehouse as wh
from repopulse.config import load_config, setup_logging
from repopulse.ml import metrics as M
from repopulse.ml import stats as S

log = logging.getLogger("repopulse.ml.evaluate")

FEATURES_TABLE = f"{wh.GOLD_SCHEMA}.ml_repo_features"
SCORES_TABLE = f"{wh.GOLD_SCHEMA}.repo_breakout_scores"
MODELS = {  # model name -> column holding its score
    "gbt_sparkml": "score_gbt",
    "logreg_baseline": "score_logreg",
    "heuristic_stars_in_window": "stars_w",
}
PAIRS = [
    ("gbt_sparkml", "logreg_baseline"),
    ("gbt_sparkml", "heuristic_stars_in_window"),
    ("logreg_baseline", "heuristic_stars_in_window"),
]
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
WHERE f.split IN ('train', 'valid', 'test', 'score')
ORDER BY f.split, f.repo_id"""


def _fmt(value, pct: bool = False) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.1f}%" if pct else f"{value:.4f}"


def markdown_table(results: dict[str, dict]) -> str:
    header = (
        "| model | AUC-ROC | AUC-PR | AUC-PR (train) | precision | recall | precision@top-k | lift@top-k |\n"
        "|---|---|---|---|---|---|---|---|\n"
    )
    rows = [
        f"| {name} | {_fmt(r['auc_roc'])} | {_fmt(r['auc_pr'])} | {_fmt(r['train_auc_pr'])} | "
        f"{_fmt(r['precision'])} | {_fmt(r['recall'])} | {_fmt(r['precision_at_k'])} | "
        f"{_fmt(r['lift_at_k'])} |"
        for name, r in results.items()
    ]
    return header + "\n".join(rows) + "\n"


def bootstrap_table(comparisons: dict[str, dict | None]) -> str:
    header = "| AUC-PR difference (test) | diff | 95% interval | significant |\n|---|---|---|---|\n"
    rows = []
    for name, r in comparisons.items():
        if r is None:
            rows.append(f"| {name} | n/a | n/a | n/a |")
        else:
            rows.append(
                f"| {name} | {r['diff']:+.4f} | [{r['ci_low']:+.4f}, {r['ci_high']:+.4f}] | "
                f"{'yes' if r['significant'] else 'no'} |"
            )
    return header + "\n".join(rows) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write scores to the warehouse and evaluate.")
    parser.add_argument("--top-fraction", type=float, default=0.01, help="top share for precision@k")
    parser.add_argument("--bootstrap", type=int, default=200, help="bootstrap resamples (0 = skip)")
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
            f"SELECT {', '.join(COLUMNS)} FROM {SCORES_TABLE} "
            "WHERE split IN ('train', 'valid', 'test')"
        ).fetchall()
    finally:
        con.close()

    index = {name: i for i, name in enumerate(COLUMNS)}

    def pairs_for(split: str, column: str):
        return [
            (r[index["label"]], r[index[column]])
            for r in rows
            if r[index["split"]] == split
            and r[index["label"]] is not None
            and r[index[column]] is not None
        ]

    results: dict[str, dict] = {}
    for name, column in MODELS.items():
        valid, test, train = pairs_for("valid", column), pairs_for("test", column), pairs_for("train", column)
        if not valid or not test:
            log.warning("skipping %s: no scored rows", name)
            continue
        result = M.evaluate(tuple(zip(*valid)), tuple(zip(*test)), args.top_fraction)
        train_labels, train_scores = (tuple(zip(*train)) if train else ((), ()))
        result["train_auc_roc"] = M.roc_auc(train_labels, train_scores) if train else None
        result["train_auc_pr"] = M.average_precision(train_labels, train_scores) if train else None
        result["auc_pr_train_minus_test"] = (
            result["train_auc_pr"] - result["auc_pr"]
            if result["train_auc_pr"] is not None and result["auc_pr"] is not None
            else None
        )
        results[name] = result

    comparisons: dict[str, dict | None] = {}
    if args.bootstrap > 0:
        test_rows = [
            r for r in rows
            if r[index["split"]] == "test"
            and r[index["label"]] is not None
            and all(r[index[c]] is not None for c in MODELS.values())
        ]
        labels = [r[index["label"]] for r in test_rows]
        columns = {name: [r[index[col]] for r in test_rows] for name, col in MODELS.items()}
        for a, b in PAIRS:
            if a in results and b in results:
                comparisons[f"{a} - {b}"] = S.paired_bootstrap_diff(
                    labels, columns[a], columns[b], n_boot=args.bootstrap
                )

    report = {
        "scores_table": SCORES_TABLE,
        "rows_per_split": by_split,
        "models": results,
        "bootstrap_auc_pr_diff": comparisons,
        "note": "Threshold chosen on 'valid' (best F1) and applied to 'test'. 'train' metrics are "
        "in-sample (the train split includes rows the models were fitted on). Check n_pos_test "
        "before trusting any number: small positive counts make these metrics noisy.",
    }
    (ml_dir / "metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    text = markdown_table(results) + ("\n" + bootstrap_table(comparisons) if comparisons else "")
    (ml_dir / "metrics.md").write_text(text, encoding="utf-8")
    print(text)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())