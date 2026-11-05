"""SparkML: class-weighted gradient-boosted trees (main) vs logistic regression (baseline).

Both models train on the same rows: the train split, optionally with negatives downsampled,
minus a hash-selected holdout that the GBT uses for early stopping.
"""

import argparse
import json
import logging
import time
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from repopulse.config import load_config, setup_logging
from repopulse.ml.spec import FEATURE_COLUMNS
from repopulse.spark_session import build_spark
from repopulse.storage import append_metrics

log = logging.getLogger("repopulse.ml.sparkml")

LR_COLUMNS = [f"lr_{c}" for c in FEATURE_COLUMNS]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train the SparkML breakout models.")
    parser.add_argument("--config", default=None, help="path to pipeline.yaml")
    parser.add_argument("--neg-per-pos", type=int, default=20,
                        help="keep at most this many negatives per positive (0 = no downsampling)")
    parser.add_argument("--max-iter", type=int, default=200, help="GBT tree cap (early stopping applies)")
    parser.add_argument("--max-depth", type=int, default=3, help="GBT tree depth")
    parser.add_argument("--step-size", type=float, default=0.05, help="GBT learning rate")
    parser.add_argument("--holdout-pct", type=int, default=20, help="train share used for early stopping")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    lr_iter = int((cfg.get("ml") or {}).get("logreg_max_iter", 100))
    ml_dir = Path(cfg["paths"].get("ml", "data/ml"))
    features_path = ml_dir / "features.parquet"
    if not features_path.exists():
        log.error("%s not found: run `./tasks.ps1 ml-features` first", features_path)
        return 1

    spark = build_spark("repopulse-ml", cfg)
    try:
        started = time.perf_counter()
        df = spark.read.parquet(str(features_path.resolve()))
        for column in FEATURE_COLUMNS:
            df = df.withColumn(column, F.col(column).cast("double"))
            # log scale for the linear baseline: heavy-tailed counts, signed for star_accel
            df = df.withColumn(
                f"lr_{column}", F.signum(F.col(column)) * F.log1p(F.abs(F.col(column)))
            )
        df = df.withColumn("label_d", F.col("label").cast("double"))

        train_all = df.where(F.col("split") == "train")
        pos_all = train_all.where(F.col("label") == 1).count()
        neg_all = train_all.where(F.col("label") == 0).count()
        if pos_all == 0 or neg_all == 0:
            log.error("training data has one class only (pos=%d, neg=%d)", pos_all, neg_all)
            return 3

        train = train_all
        if args.neg_per_pos > 0 and neg_all > args.neg_per_pos * pos_all:
            fraction = args.neg_per_pos * pos_all / neg_all
            train = train_all.sampleBy("label", {0: fraction, 1: 1.0}, seed=args.seed)

        # hash-selected holdout: used only by the GBT for early stopping, never for fitting
        train = train.withColumn(
            "is_val", F.pmod(F.hash(F.col("repo_id")), F.lit(100)) < args.holdout_pct
        ).persist()
        fit_rows = train.where(~F.col("is_val"))
        pos_fit = fit_rows.where(F.col("label") == 1).count()
        neg_fit = fit_rows.where(F.col("label") == 0).count()
        if pos_fit == 0:
            log.error("no positive examples left after the holdout split; lower --holdout-pct")
            return 3
        weight = neg_fit / pos_fit  # class weighting on the (possibly downsampled) fit rows
        train = train.withColumn(
            "weight", F.when(F.col("label") == 1, F.lit(weight)).otherwise(F.lit(1.0))
        )
        fit_rows = train.where(~F.col("is_val"))

        gbt_model = Pipeline(
            stages=[
                VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="keep"),
                GBTClassifier(
                    labelCol="label_d",
                    featuresCol="features",
                    weightCol="weight",
                    maxIter=args.max_iter,
                    maxDepth=args.max_depth,
                    stepSize=args.step_size,
                    subsamplingRate=0.8,
                    minInstancesPerNode=10,
                    validationIndicatorCol="is_val",
                    validationTol=0.001,
                    seed=args.seed,
                ),
            ]
        ).fit(train)

        lr_model = Pipeline(
            stages=[
                VectorAssembler(inputCols=LR_COLUMNS, outputCol="lr_features", handleInvalid="keep"),
                LogisticRegression(
                    labelCol="label_d",
                    featuresCol="lr_features",
                    weightCol="weight",
                    probabilityCol="lr_probability",
                    rawPredictionCol="lr_raw",
                    predictionCol="lr_prediction",
                    maxIter=lr_iter,
                    regParam=0.01,
                    standardization=True,
                ),
            ]
        ).fit(fit_rows)
        train_seconds = time.perf_counter() - started

        to_score = df.where(F.col("split").isin("train", "valid", "test", "score"))
        scored = lr_model.transform(gbt_model.transform(to_score)).select(
            F.col("repo_id").cast("long").alias("repo_id"),
            F.col("split"),
            F.col("label").cast("long").alias("label"),
            vector_to_array(F.col("probability"))[1].alias("score_gbt"),
            vector_to_array(F.col("lr_probability"))[1].alias("score_logreg"),
        )
        scored = scored.persist()
        per_split = {r["split"]: r["count"] for r in scored.groupBy("split").count().collect()}

        scores_dir = ml_dir / "scores_sparkml"
        scored.coalesce(1).write.mode("overwrite").parquet(str(scores_dir.resolve()))
        gbt_model.write().overwrite().save(str((ml_dir / "model_gbt").resolve()))
        lr_model.write().overwrite().save(str((ml_dir / "model_logreg").resolve()))

        gbt = gbt_model.stages[-1]
        importances = gbt.featureImportances.toArray()
        stats = {
            "downsampling": {
                "neg_per_pos": args.neg_per_pos,
                "train_positives": pos_all,
                "train_negatives_before": neg_all,
                "negatives_kept_incl_holdout": train.where(F.col("label") == 0).count(),
            },
            "fit_rows": {"positives": pos_fit, "negatives": neg_fit, "holdout_pct": args.holdout_pct},
            "positive_class_weight": round(weight, 2),
            "gbt": {
                "tree_cap": args.max_iter,
                "trees_used": len(gbt.trees),
                "max_depth": args.max_depth,
                "step_size": args.step_size,
            },
            "logreg": {"max_iter": lr_iter, "reg_param": 0.01},
            "scored_rows": per_split,
            "train_seconds": round(train_seconds, 2),
            "gbt_feature_importance": {
                name: round(float(v), 4)
                for name, v in sorted(zip(FEATURE_COLUMNS, importances), key=lambda t: -t[1])
            },
        }
    finally:
        spark.stop()

    (ml_dir / "sparkml_train.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")
    append_metrics(Path(cfg["paths"]["metrics"]) / "ml_runs.jsonl", {"job": "ml_sparkml", **stats})
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())