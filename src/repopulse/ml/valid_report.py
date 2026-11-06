"""Validation-split metrics per model, for choosing hyperparameters without looking at test."""

import json

from repopulse import warehouse as wh
from repopulse.config import load_config
from repopulse.ml import metrics as M

MODELS = {
    "gbt_sparkml": "score_gbt",
    "logreg_baseline": "score_logreg",
    "heuristic_stars_in_window": "stars_w",
}


def main() -> int:
    cfg = load_config()
    con = wh.open_warehouse(cfg, read_only=True)
    report = {}
    try:
        for name, column in MODELS.items():
            rows = con.execute(
                f"SELECT label, {column} FROM gold.repo_breakout_scores "
                f"WHERE split = 'valid' AND label IS NOT NULL AND {column} IS NOT NULL"
            ).fetchall()
            labels = [int(r[0]) for r in rows]
            scores = [float(r[1]) for r in rows]
            report[name] = {
                "n": len(rows),
                "positives": sum(labels),
                "valid_auc_roc": M.roc_auc(labels, scores),
                "valid_auc_pr": M.average_precision(labels, scores),
            }
    finally:
        con.close()
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())