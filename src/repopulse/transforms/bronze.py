"""Bronze transforms: raw JSON lines -> parsed envelope, quarantine split, type filter."""

from collections.abc import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from repopulse.schemas import (
    BRONZE_COLUMNS,
    CORRUPT_COLUMN,
    ENVELOPE_SCHEMA,
    QUARANTINE_COLUMNS,
)

SOURCE_FILE_PATTERN = r"(\d{4}-\d{2}-\d{2})-(\d{1,2})\.json\.gz"
MAX_QUARANTINED_LINE_CHARS = 65536

REASON_EMPTY = "empty_line"
REASON_MALFORMED = "malformed_record"
REASON_MISSING_FIELD = "missing_required_field"
REASON_BAD_TIMESTAMP = "invalid_timestamp"


def parse_lines(raw: DataFrame) -> DataFrame:
    """Parse the envelope of each raw line.

    Input columns:  value (the raw line), source_file (full path of the .json.gz).
    Output: one row per input line, flat envelope columns, source_date/source_hour from the
    filename, and quarantine_reason (null when the line is acceptable).
    """
    envelope = F.from_json(F.col("value"), ENVELOPE_SCHEMA, {"mode": "PERMISSIVE"})
    base = raw.select(
        F.col("value").alias("raw_line"),
        F.col("source_file"),
        envelope.alias("e"),
    )

    created_ts = F.to_timestamp(F.col("e.created_at"))
    required_missing = (
        F.col("e.id").isNull()
        | (F.trim(F.col("e.id")) == "")
        | F.col("e.type").isNull()
        | F.col("e.created_at").isNull()
    )
    reason = (
        F.when(F.trim(F.col("raw_line")) == "", F.lit(REASON_EMPTY))
        .when(F.col(f"e.{CORRUPT_COLUMN}").isNotNull(), F.lit(REASON_MALFORMED))
        .when(required_missing, F.lit(REASON_MISSING_FIELD))
        .when(created_ts.isNull(), F.lit(REASON_BAD_TIMESTAMP))
    )

    date_str = F.regexp_extract(F.col("source_file"), SOURCE_FILE_PATTERN, 1)
    hour_str = F.regexp_extract(F.col("source_file"), SOURCE_FILE_PATTERN, 2)

    return base.select(
        F.col("raw_line"),
        F.col("source_file"),
        F.when(date_str != "", date_str).alias("source_date"),
        F.when(hour_str != "", hour_str.cast("int")).alias("source_hour"),
        F.col("e.id").alias("id"),
        F.col("e.type").alias("type"),
        created_ts.alias("created_at"),
        F.col("e.actor.id").alias("actor_id"),
        F.col("e.actor.login").alias("actor_login"),
        F.col("e.repo.id").alias("repo_id"),
        F.col("e.repo.name").alias("repo_name"),
        F.col("e.org.id").alias("org_id"),
        F.col("e.org.login").alias("org_login"),
        F.col("e.public").alias("public"),
        F.col("e.payload").alias("payload"),
        reason.alias("quarantine_reason"),
    )


def valid_events(parsed: DataFrame, event_types: Iterable[str]) -> DataFrame:
    """Acceptable lines of the requested event types, in bronze column order."""
    return (
        parsed.where(F.col("quarantine_reason").isNull() & F.col("type").isin(list(event_types)))
        .withColumn("ingested_at", F.current_timestamp())
        .select(*BRONZE_COLUMNS)
    )


def quarantined(parsed: DataFrame) -> DataFrame:
    """Lines that could not be accepted, with the raw text (capped) and a reason code."""
    return (
        parsed.where(F.col("quarantine_reason").isNotNull())
        .select(
            F.substring(F.col("raw_line"), 1, MAX_QUARANTINED_LINE_CHARS).alias("raw_line"),
            F.col("quarantine_reason").alias("reason"),
            F.col("source_file"),
            F.col("source_date"),
            F.col("source_hour"),
            F.current_timestamp().alias("quarantined_at"),
        )
        .select(*QUARANTINE_COLUMNS)
    )