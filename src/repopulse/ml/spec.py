"""Dependency-free ML definitions: features, windows, walk-forward cutoffs, feature SQL (DuckDB)."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

FEATURE_COLUMNS = [
    "events_w",
    "pushes_w",
    "pr_events_w",
    "merged_prs_w",
    "issue_events_w",
    "stars_w",
    "forks_w",
    "unique_actors_w",
    "active_hours_w",
    "stars_recent_half",
    "stars_early_half",
    "star_accel",
    "hours_since_first_seen",
]

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def safe_ident(value: str) -> str:
    """Identifiers are interpolated into SQL, so only plain characters are accepted."""
    if not isinstance(value, str) or not _IDENT.fullmatch(value):
        raise ValueError(f"unsafe or empty identifier: {value!r}")
    return value


@dataclass(frozen=True)
class MLParams:
    feature_hours: int = 72
    horizon_hours: int = 72
    threshold: int = 10
    min_events: int = 2

    def validate(self) -> None:
        if self.feature_hours < 2:
            raise ValueError("feature_hours must be at least 2 (it is split into two halves)")
        if self.horizon_hours < 1:
            raise ValueError("horizon_hours must be at least 1")
        if self.threshold < 1:
            raise ValueError("threshold must be at least 1 star")
        if self.min_events < 1:
            raise ValueError("min_events must be at least 1")


def resolve_params(
    ml_cfg: dict | None,
    *,
    feature_hours: int | None = None,
    horizon_hours: int | None = None,
    threshold: int | None = None,
    min_events: int | None = None,
) -> MLParams:
    ml_cfg = ml_cfg or {}
    params = MLParams(
        feature_hours=int(feature_hours or ml_cfg.get("feature_window_hours", 72)),
        horizon_hours=int(horizon_hours or ml_cfg.get("horizon_hours", 72)),
        threshold=int(threshold or ml_cfg.get("breakout_threshold", 10)),
        min_events=int(min_events or ml_cfg.get("min_feature_events", 2)),
    )
    params.validate()
    return params


@dataclass(frozen=True)
class Cutoffs:
    train: datetime
    test: datetime
    score: datetime
    data_start: datetime
    data_end: datetime

    def as_dict(self) -> dict:
        return {k: v.isoformat() for k, v in self.__dict__.items()}


def plan_cutoffs(
    data_start: datetime, data_end: datetime, feature_hours: int, horizon_hours: int
) -> Cutoffs:
    """Walk-forward cutoffs. Needs feature_hours + 2 * horizon_hours of data."""
    needed = feature_hours + 2 * horizon_hours
    available = (data_end - data_start) / timedelta(hours=1)
    if available < needed:
        raise ValueError(
            f"the data covers {available:.0f} h but this setup needs {needed} h "
            f"({feature_hours} h of features + 2 x {horizon_hours} h horizon). "
            "Use smaller --feature-hours / --horizon-hours, or process more hours."
        )
    score = data_end
    test = score - timedelta(hours=horizon_hours)
    train = test - timedelta(hours=horizon_hours)
    return Cutoffs(train=train, test=test, score=score, data_start=data_start, data_end=data_end)


def check_contiguous(hours_present: int, data_start: datetime, data_end: datetime) -> None:
    expected = round((data_end - data_start) / timedelta(hours=1))
    if hours_present != expected:
        raise ValueError(
            f"the warehouse has {hours_present} of the {expected} hours between "
            f"{data_start:%Y-%m-%d %H:%M} and {data_end:%Y-%m-%d %H:%M} UTC; "
            "load the missing hours (or reset the warehouse and load one contiguous range)."
        )


def _ts(value: datetime) -> str:
    """Timestamp literal. The warehouse stores UTC without a time zone."""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return "TIMESTAMP '" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"


def render_features_sql(
    schema: str,
    fact_table: str,
    features_table: str,
    cutoffs: Cutoffs,
    params: MLParams,
) -> str:
    """CREATE TABLE ... AS SELECT building one row per (repo, cutoff) with features and label."""
    params.validate()
    fact = f"{safe_ident(schema)}.{safe_ident(fact_table)}"
    target = f"{safe_ident(schema)}.{safe_ident(features_table)}"
    window = int(params.feature_hours)
    horizon = int(params.horizon_hours)
    half = window // 2
    lo = (cutoffs.train - timedelta(hours=window)).date().isoformat()  # prune by event_date
    hi = cutoffs.score.date().isoformat()

    return f"""CREATE OR REPLACE TABLE {target} AS
WITH cutoffs AS (
  SELECT 'train_valid' AS cutoff_name, {_ts(cutoffs.train)} AS cutoff_ts
  UNION ALL SELECT 'test', {_ts(cutoffs.test)}
  UNION ALL SELECT 'score', {_ts(cutoffs.score)}
),
events AS (
  SELECT repo_id, actor_id, event_type, created_at, event_hour, pr_merged
  FROM {fact}
  WHERE repo_id IS NOT NULL
    AND event_date BETWEEN DATE '{lo}' AND DATE '{hi}'
),
windowed AS (
  SELECT
    c.cutoff_name,
    c.cutoff_ts,
    e.repo_id,
    COUNT(*) AS events_w,
    COUNT(*) FILTER (WHERE e.event_type = 'PushEvent') AS pushes_w,
    COUNT(*) FILTER (WHERE e.event_type = 'PullRequestEvent') AS pr_events_w,
    COUNT(*) FILTER (WHERE e.event_type = 'PullRequestEvent' AND e.pr_merged) AS merged_prs_w,
    COUNT(*) FILTER (WHERE e.event_type = 'IssuesEvent') AS issue_events_w,
    COUNT(*) FILTER (WHERE e.event_type = 'WatchEvent') AS stars_w,
    COUNT(*) FILTER (WHERE e.event_type = 'ForkEvent') AS forks_w,
    COUNT(DISTINCT e.actor_id) AS unique_actors_w,
    COUNT(DISTINCT e.event_hour) AS active_hours_w,
    COUNT(*) FILTER (WHERE e.event_type = 'WatchEvent'
                       AND e.created_at >= c.cutoff_ts - INTERVAL {half} HOUR) AS stars_recent_half,
    COUNT(*) FILTER (WHERE e.event_type = 'WatchEvent'
                       AND e.created_at < c.cutoff_ts - INTERVAL {half} HOUR) AS stars_early_half
  FROM cutoffs AS c
  JOIN events AS e
    ON e.created_at >= c.cutoff_ts - INTERVAL {window} HOUR
   AND e.created_at < c.cutoff_ts
  GROUP BY c.cutoff_name, c.cutoff_ts, e.repo_id
),
labels AS (
  SELECT
    c.cutoff_name,
    e.repo_id,
    COUNT(*) FILTER (WHERE e.event_type = 'WatchEvent') AS stars_next
  FROM cutoffs AS c
  JOIN events AS e
    ON e.created_at >= c.cutoff_ts
   AND e.created_at < c.cutoff_ts + INTERVAL {horizon} HOUR
  GROUP BY c.cutoff_name, e.repo_id
),
first_seen AS (
  SELECT repo_id, MIN(created_at) AS first_seen_at
  FROM {fact}
  WHERE repo_id IS NOT NULL
  GROUP BY repo_id
)
SELECT
  w.repo_id,
  w.cutoff_ts,
  CASE
    WHEN w.cutoff_name = 'test' THEN 'test'
    WHEN w.cutoff_name = 'score' THEN 'score'
    WHEN hash(CAST(w.repo_id AS VARCHAR)) % 10 < 8 THEN 'train'
    ELSE 'valid'
  END AS split,
  w.events_w,
  w.pushes_w,
  w.pr_events_w,
  w.merged_prs_w,
  w.issue_events_w,
  w.stars_w,
  w.forks_w,
  w.unique_actors_w,
  w.active_hours_w,
  w.stars_recent_half,
  w.stars_early_half,
  CAST(w.stars_recent_half - w.stars_early_half AS BIGINT) AS star_accel,
  date_diff('second', f.first_seen_at, w.cutoff_ts) / 3600.0 AS hours_since_first_seen,
  CASE WHEN w.cutoff_name = 'score' THEN NULL ELSE COALESCE(l.stars_next, 0) END AS stars_next,
  CASE WHEN w.cutoff_name = 'score' THEN NULL
       ELSE CAST(COALESCE(l.stars_next, 0) >= {int(params.threshold)} AS INTEGER) END AS label
FROM windowed AS w
JOIN first_seen AS f
  ON f.repo_id = w.repo_id
LEFT JOIN labels AS l
  ON l.cutoff_name = w.cutoff_name AND l.repo_id = w.repo_id
WHERE w.events_w >= {int(params.min_events)}
"""