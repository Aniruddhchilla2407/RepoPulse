"""Explicit schemas: bronze envelope, payload fields, silver contract."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CORRUPT_COLUMN = "_corrupt_record"
PARTITION_COLUMNS = ("source_date", "source_hour")

# --- Bronze envelope: only top-level fields that have been stable for years -----------------
# Unknown extra fields are ignored; missing/retyped fields become null (never an exception).
# `payload` is kept as a raw JSON string so payload drift cannot break bronze.
ENVELOPE_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("created_at", StringType()),
        StructField("public", BooleanType()),
        StructField("actor", StructType([StructField("id", LongType()), StructField("login", StringType())])),
        StructField("repo", StructType([StructField("id", LongType()), StructField("name", StringType())])),
        StructField("org", StructType([StructField("id", LongType()), StructField("login", StringType())])),
        StructField("payload", StringType()),
        StructField(CORRUPT_COLUMN, StringType()),
    ]
)

BRONZE_COLUMNS = [
    "id",
    "type",
    "created_at",
    "actor_id",
    "actor_login",
    "repo_id",
    "repo_name",
    "org_id",
    "org_login",
    "public",
    "payload",
    "source_file",
    "source_date",
    "source_hour",
    "ingested_at",
]

QUARANTINE_COLUMNS = [
    "raw_line",
    "reason",
    "source_file",
    "source_date",
    "source_hour",
    "quarantined_at",
]

# --- Payload fields extracted in silver ----------------------------------------------------
PAYLOAD_SCHEMA = StructType(
    [
        StructField("action", StringType()),
        StructField("number", LongType()),
        StructField("ref", StringType()),
        StructField("size", LongType()),
        StructField("distinct_size", LongType()),
        StructField(
            "pull_request",
            StructType(
                [
                    StructField("number", LongType()),
                    StructField("merged", BooleanType()),
                    StructField("additions", LongType()),
                    StructField("deletions", LongType()),
                    StructField("changed_files", LongType()),
                ]
            ),
        ),
        StructField("issue", StructType([StructField("number", LongType())])),
        StructField(
            "forkee",
            StructType([StructField("id", LongType()), StructField("full_name", StringType())]),
        ),
    ]
)

# --- Silver: the explicit, typed contract --------------------------------------------------
SILVER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("actor_id", LongType()),
        StructField("actor_login", StringType()),
        StructField("repo_id", LongType()),
        StructField("repo_name", StringType()),
        StructField("org_id", LongType()),
        StructField("org_login", StringType()),
        StructField("is_public", BooleanType()),
        # PushEvent
        StructField("push_ref", StringType()),
        StructField("push_size", LongType()),
        StructField("push_distinct_size", LongType()),
        # PullRequestEvent
        StructField("pr_action", StringType()),
        StructField("pr_number", LongType()),
        StructField("pr_merged", BooleanType()),
        StructField("pr_additions", LongType()),
        StructField("pr_deletions", LongType()),
        StructField("pr_changed_files", LongType()),
        # IssuesEvent
        StructField("issue_action", StringType()),
        StructField("issue_number", LongType()),
        # WatchEvent
        StructField("watch_action", StringType()),
        # ForkEvent
        StructField("fork_repo_id", LongType()),
        StructField("fork_repo_name", StringType()),
        # lineage / partitioning
        StructField("source_file", StringType()),
        StructField("source_date", StringType()),
        StructField("source_hour", IntegerType()),
    ]
)