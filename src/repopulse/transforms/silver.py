"""Silver transforms: deduplicate, extract typed payload fields, enforce schema."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from repopulse.schemas import PAYLOAD_SCHEMA, SILVER_SCHEMA


def deduplicate(bronze: DataFrame) -> DataFrame:
    """Keep one row per event id. Deterministic: earliest source partition wins."""
    window = Window.partitionBy("id").orderBy(
        F.col("source_date").asc(),
        F.col("source_hour").cast("int").asc(),
        F.col("source_file").asc(),
        F.sha2(F.col("payload"), 256).asc(),
    )
    return (
        bronze.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def extract_fields(bronze: DataFrame) -> DataFrame:
    """Parse payload JSON once; surface per-event-type fields as typed columns."""
    df = bronze.withColumn("_p", F.from_json(F.col("payload"), PAYLOAD_SCHEMA))
    p = F.col("_p")
    pr = p.getField("pull_request")

    def only(event_type: str, value):
        return F.when(F.col("type") == event_type, value)

    return df.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("created_at"),
        F.to_date(F.col("created_at")).alias("event_date"),
        F.col("actor_id"),
        F.col("actor_login"),
        F.col("repo_id"),
        F.col("repo_name"),
        F.col("org_id"),
        F.col("org_login"),
        F.col("public").alias("is_public"),
        only("PushEvent", p.getField("ref")).alias("push_ref"),
        only("PushEvent", p.getField("size")).alias("push_size"),
        only("PushEvent", p.getField("distinct_size")).alias("push_distinct_size"),
        only("PullRequestEvent", p.getField("action")).alias("pr_action"),
        only("PullRequestEvent", F.coalesce(p.getField("number"), pr.getField("number"))).alias(
            "pr_number"
        ),
        only("PullRequestEvent", pr.getField("merged")).alias("pr_merged"),
        only("PullRequestEvent", pr.getField("additions")).alias("pr_additions"),
        only("PullRequestEvent", pr.getField("deletions")).alias("pr_deletions"),
        only("PullRequestEvent", pr.getField("changed_files")).alias("pr_changed_files"),
        only("IssuesEvent", p.getField("action")).alias("issue_action"),
        only("IssuesEvent", p.getField("issue").getField("number")).alias("issue_number"),
        only("WatchEvent", p.getField("action")).alias("watch_action"),
        only("ForkEvent", p.getField("forkee").getField("id")).alias("fork_repo_id"),
        only("ForkEvent", p.getField("forkee").getField("full_name")).alias("fork_repo_name"),
        F.col("source_file"),
        F.col("source_date"),
        F.col("source_hour").cast("int").alias("source_hour"),
    )


def enforce_schema(df: DataFrame, schema: StructType = SILVER_SCHEMA) -> DataFrame:
    """Select exactly the contract columns, in order, cast to the contract types."""
    missing = [f.name for f in schema.fields if f.name not in df.columns]
    if missing:
        raise ValueError(f"Cannot enforce silver schema, missing columns: {missing}")
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def build_silver(bronze: DataFrame) -> DataFrame:
    return enforce_schema(extract_fields(deduplicate(bronze)))