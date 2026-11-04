"""SparkML: class-weighted gradient-boosted trees (main) vs logistic regression (baseline)."""

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
    parser.add_argument("--max-iter", type=int, default=None, help="GBT iterations")
    parser.add_argument("--max-depth", type=int, default=None, help="GBT tree depth")
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    ml_cfg = cfg.get("ml") or {}
    max_iter = args.max_iter or int(ml_cfg.get("gbt_max_iter", 50))
    max_depth = args.max_depth or int(ml_cfg.get("gbt_max_depth", 3))
    lr_iter = int(ml_cfg.get("logreg_max_iter", 100))
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
            # Log scale transformation for heavy-tailed counts
            df = df.withColumn(
                f"lr_{column}", F.signum(F.col(column)) * F.log1p(F.abs(F.col(column)))
            )
        df = df.withColumn("label_d", F.col("label").cast("double"))

        train = df.where(F.col("split") == "train").persist()
        total = train.count()
        positives = train.where(F.col("label") == 1).count()
        negatives = total - positives
        if positives == 0 or negatives == 0:
            log.error("training data has one class only (pos=%d, neg=%d)", positives, negatives)
            return 3
        
        weight = negatives / positives  # Class weight multiplier for Logistic Regression
        train = train.withColumn(
            "weight", F.when(F.col("label") == 1, F.lit(weight)).otherwise(F.lit(1.0))
        )

        # ------------------------------------------------------------------
        # Downsample majority class for GBT to resolve PySpark weightCol degradation
        # ------------------------------------------------------------------
        pos_train = train.where(F.col("label") == 1)
        neg_train = train.where(F.col("label") == 0)
        
        # Target ~1:10 positive to negative ratio (approx. 2,110 negative samples)
        neg_fraction = min(1.0, (positives * 10) / negatives)
        neg_sampled = neg_train.sample(withReplacement=False, fraction=neg_fraction, seed=42)
        
        gbt_train = pos_train.union(neg_sampled).persist()

        # GBT Pipeline: Trained on downsampled data without weightCol
        gbt_model = Pipeline(
            stages=[
                VectorAssembler(inputCols=LR_COLUMNS, outputCol="gbt_features", handleInvalid="keep"),
                GBTClassifier(
                    labelCol="label_d",
                    featuresCol="gbt_features",
                    maxIter=max_iter,
                    maxDepth=max_depth,
                    stepSize=0.05,
                    subsamplingRate=0.8,
                    maxBins=64,
                    seed=42,
                ),
            ]
        ).fit(gbt_train)

        # Logistic Regression Pipeline: Trained on full dataset using weightCol
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
        ).fit(train)
        train_seconds = time.perf_counter() - started

        to_score = df.where(F.col("split").isin("valid", "test", "score"))
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

        importances = gbt_model.stages[-1].featureImportances.toArray()
        stats = {
            "train_rows": total,
            "train_positives": positives,
            "positive_class_weight": round(weight, 2),
            "gbt": {"max_iter": max_iter, "max_depth": max_depth},
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