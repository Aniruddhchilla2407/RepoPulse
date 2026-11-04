# import os
# import subprocess
# import sys
# from datetime import datetime, timedelta, timezone
# from pathlib import Path

# import pytest

# pytest.importorskip("duckdb")

# ROOT = Path(__file__).resolve().parents[1]
# DBT = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
# pytestmark = pytest.mark.skipif(not DBT.exists(), reason="dbt is not installed in this environment")

# import synth_events  # noqa: E402

# from repopulse import load_warehouse  # noqa: E402
# from repopulse import warehouse as wh  # noqa: E402
# from repopulse.ml import spec  # noqa: E402


# @pytest.fixture(scope="module")
# def built(tmp_path_factory):
#     work = tmp_path_factory.mktemp("sql_e2e")
#     db_path = work / "warehouse.duckdb"
#     raw_rows, events = synth_events.build()

#     con = wh.connect(str(db_path), memory_limit="1GB", threads=1)
#     try:
#         load_warehouse.ensure_table(con)
#         synth_events.insert_rows(con, raw_rows)
#     finally:
#         con.close()

#     env = {
#         **os.environ,
#         "REPOPULSE_DB": str(db_path),
#         "DBT_TARGET_PATH": str(work / "target"),
#         "DBT_LOG_PATH": str(work / "logs"),
#         "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
#     }
#     result = subprocess.run(
#         [str(DBT), "build", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")],
#         cwd=ROOT,
#         env=env,
#         capture_output=True,
#         text=True,
#         timeout=900,
#     )
#     assert result.returncode == 0, result.stdout[-5000:] + "\n" + result.stderr[-2000:]
#     return {"db": db_path, "events": events, "raw_rows": raw_rows}


# def query(db, sql):
#     con = wh.connect(str(db), read_only=True, memory_limit="1GB", threads=1)
#     try:
#         return con.execute(sql).fetchall()
#     finally:
#         con.close()


# def test_star_schema_row_counts(built):
#     events, db = built["events"], built["db"]
#     assert query(db, "SELECT COUNT(*) FROM raw.silver_events")[0][0] == len(events) + 1  # + duplicate
#     assert query(db, "SELECT COUNT(*) FROM gold.stg_silver_events")[0][0] == len(events)
#     assert query(db, "SELECT COUNT(*) FROM gold.fact_events")[0][0] == len(events)
#     assert query(db, "SELECT COUNT(*) FROM gold.dim_repo")[0][0] == len(synth_events.REPO_IDS)
#     assert query(db, "SELECT COUNT(*) FROM gold.dim_actor")[0][0] == len({e["actor_id"] for e in events})
#     assert query(db, "SELECT COUNT(*) FROM gold.dim_date")[0][0] == 1  # all events share one day
#     assert query(db, "SELECT is_bot FROM gold.dim_actor WHERE actor_id = 999")[0][0] is True


# def test_sessions_exclude_bots_and_cover_every_other_event(built):
#     events, db = built["events"], built["db"]
#     assert query(db, "SELECT COUNT(*) FROM gold.mart_actor_sessions WHERE actor_id = 999")[0][0] == 0
#     covered = query(db, "SELECT SUM(events) FROM gold.mart_actor_sessions")[0][0]
#     assert covered == sum(1 for e in events if e["actor_id"] != synth_events.BOT_ID)
#     assert query(db, "SELECT MIN(duration_seconds) FROM gold.mart_actor_sessions")[0][0] >= 0


# def test_window_function_columns(built):
#     events, db = built["events"], built["db"]
#     stars = sum(1 for e in events if e["repo_id"] == 1000 and e["event_type"] == "WatchEvent")
#     last = query(
#         db,
#         "SELECT cumulative_stars FROM gold.mart_repo_hourly_activity "
#         "WHERE repo_id = 1000 ORDER BY event_hour DESC LIMIT 1",
#     )[0][0]
#     assert last == stars
#     bad = query(
#         db,
#         "SELECT COUNT(*) FROM (SELECT event_hour, MIN(events_rank_in_hour) AS m "
#         "FROM gold.mart_repo_hourly_activity GROUP BY event_hour) WHERE m != 1",
#     )[0][0]
#     assert bad == 0


# def test_retention_cohorts_start_fully_active(built):
#     db = built["db"]
#     assert query(db, "SELECT MIN(periods_since_first) FROM gold.mart_actor_retention")[0][0] == 0
#     rates = query(
#         db, "SELECT retention_rate FROM gold.mart_actor_retention WHERE periods_since_first = 0"
#     )
#     assert rates and all(r[0] == pytest.approx(1.0) for r in rates)


# def test_feature_sql_matches_python_reference(built):
#     day = synth_events.base_day()
#     start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
#     cutoffs = spec.plan_cutoffs(start, start + timedelta(hours=6), 2, 2)
#     params = spec.MLParams(feature_hours=2, horizon_hours=2, threshold=3, min_events=2)
#     sql = spec.render_features_sql("gold", "fact_events", "ml_repo_features", cutoffs, params)

#     con = wh.connect(str(built["db"]), memory_limit="1GB", threads=1)
#     try:
#         con.execute(sql)
#         rows = con.execute(
#             "SELECT repo_id, split, cutoff_ts, events_w, stars_w, stars_next, label "
#             "FROM gold.ml_repo_features"
#         ).fetchall()
#     finally:
#         con.close()

#     def naive(value):
#         return value.replace(tzinfo=None)

#     cutoff_of = {
#         "train": naive(cutoffs.train),
#         "valid": naive(cutoffs.train),
#         "test": naive(cutoffs.test),
#         "score": naive(cutoffs.score),
#     }
#     events = built["events"]

#     def count(repo, lo, hi, event_type=None):
#         return sum(
#             1
#             for e in events
#             if e["repo_id"] == repo
#             and lo <= e["created_at"] < hi
#             and (event_type is None or e["event_type"] == event_type)
#         )

#     assert len(rows) == 3 * len(synth_events.REPO_IDS)
#     assert {r[1] for r in rows} >= {"train", "test", "score"}
#     for repo, split, cutoff_ts, events_w, stars_w, stars_next, label in rows:
#         cutoff = cutoff_of[split]
#         window_start = cutoff - timedelta(hours=2)
#         assert cutoff_ts == cutoff
#         assert events_w == count(repo, window_start, cutoff)
#         assert stars_w == count(repo, window_start, cutoff, "WatchEvent")
#         if split == "score":
#             assert stars_next is None and label is None
#         else:
#             expected = count(repo, cutoff, cutoff + timedelta(hours=2), "WatchEvent")
#             assert stars_next == expected
#             assert label == int(expected >= 3)
#     assert any(r[6] == 1 for r in rows if r[1] == "test")  # the hot repos produce positives
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

ROOT = Path(__file__).resolve().parents[1]
DBT = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
pytestmark = pytest.mark.skipif(not DBT.exists(), reason="dbt is not installed in this environment")

import synth_events  # noqa: E402

from repopulse import load_warehouse  # noqa: E402
from repopulse import warehouse as wh  # noqa: E402
from repopulse.ml import spec  # noqa: E402


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    work = tmp_path_factory.mktemp("sql_e2e")
    db_path = work / "warehouse.duckdb"
    raw_rows, events = synth_events.build()

    con = wh.connect(str(db_path), memory_limit="1GB", threads=1)
    try:
        load_warehouse.ensure_table(con)
        synth_events.insert_rows(con, raw_rows)
    finally:
        con.close()

    env = {
        **os.environ,
        "REPOPULSE_DB": str(db_path),
        "DBT_TARGET_PATH": str(work / "target"),
        "DBT_LOG_PATH": str(work / "logs"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    result = subprocess.run(
        [str(DBT), "build", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert result.returncode == 0, result.stdout[-5000:] + "\n" + result.stderr[-2000:]
    return {"db": db_path, "events": events, "raw_rows": raw_rows}


def query(db, sql):
    con = wh.connect(str(db), read_only=True, memory_limit="1GB", threads=1)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_star_schema_row_counts(built):
    events, db = built["events"], built["db"]
    assert query(db, "SELECT COUNT(*) FROM raw.silver_events")[0][0] == len(events) + 1  # + duplicate
    assert query(db, "SELECT COUNT(*) FROM gold.stg_silver_events")[0][0] == len(events)
    assert query(db, "SELECT COUNT(*) FROM gold.fact_events")[0][0] == len(events)
    assert query(db, "SELECT COUNT(*) FROM gold.dim_repo")[0][0] == len(synth_events.REPO_IDS)
    assert query(db, "SELECT COUNT(*) FROM gold.dim_actor")[0][0] == len({e["actor_id"] for e in events})
    assert query(db, "SELECT COUNT(*) FROM gold.dim_date")[0][0] == 1  # all events share one day
    assert query(db, "SELECT is_bot FROM gold.dim_actor WHERE actor_id = 999")[0][0] is True


def test_sessions_exclude_bots_and_cover_every_other_event(built):
    events, db = built["events"], built["db"]
    assert query(db, "SELECT COUNT(*) FROM gold.mart_actor_sessions WHERE actor_id = 999")[0][0] == 0
    covered = query(db, "SELECT SUM(events) FROM gold.mart_actor_sessions")[0][0]
    assert covered == sum(1 for e in events if e["actor_id"] != synth_events.BOT_ID)
    assert query(db, "SELECT MIN(duration_seconds) FROM gold.mart_actor_sessions")[0][0] >= 0


def test_window_function_columns(built):
    events, db = built["events"], built["db"]
    stars = sum(1 for e in events if e["repo_id"] == 1000 and e["event_type"] == "WatchEvent")
    last = query(
        db,
        "SELECT cumulative_stars FROM gold.mart_repo_hourly_activity "
        "WHERE repo_id = 1000 ORDER BY event_hour DESC LIMIT 1",
    )[0][0]
    assert last == stars
    bad = query(
        db,
        "SELECT COUNT(*) FROM (SELECT event_hour, MIN(events_rank_in_hour) AS m "
        "FROM gold.mart_repo_hourly_activity GROUP BY event_hour) WHERE m != 1",
    )[0][0]
    assert bad == 0


def test_retention_cohorts_start_fully_active(built):
    db = built["db"]
    assert query(db, "SELECT MIN(periods_since_first) FROM gold.mart_actor_retention")[0][0] == 0
    rates = query(
        db, "SELECT retention_rate FROM gold.mart_actor_retention WHERE periods_since_first = 0"
    )
    assert rates and all(r[0] == pytest.approx(1.0) for r in rates)


def test_feature_sql_matches_python_reference(built):
    day = synth_events.base_day()
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    cutoffs = spec.plan_cutoffs(start, start + timedelta(hours=6), 2, 2)
    params = spec.MLParams(feature_hours=2, horizon_hours=2, threshold=3, min_events=2)
    sql = spec.render_features_sql("gold", "fact_events", "ml_repo_features", cutoffs, params)

    con = wh.connect(str(built["db"]), memory_limit="1GB", threads=1)
    try:
        con.execute(sql)
        rows = con.execute(
            "SELECT repo_id, split, cutoff_ts, events_w, stars_w, stars_next, label "
            "FROM gold.ml_repo_features"
        ).fetchall()
    finally:
        con.close()

    def naive(value):
        return value.replace(tzinfo=None)

    cutoff_of = {
        "train": naive(cutoffs.train),
        "valid": naive(cutoffs.train),
        "test": naive(cutoffs.test),
        "score": naive(cutoffs.score),
    }
    events = built["events"]

    def count(repo, lo, hi, event_type=None):
        return sum(
            1
            for e in events
            if e["repo_id"] == repo
            and lo <= e["created_at"] < hi
            and (event_type is None or e["event_type"] == event_type)
        )

    assert len(rows) == 3 * len(synth_events.REPO_IDS)
    assert {r[1] for r in rows} >= {"train", "test", "score"}
    for repo, split, cutoff_ts, events_w, stars_w, stars_next, label in rows:
        cutoff = cutoff_of[split]
        window_start = cutoff - timedelta(hours=2)
        assert cutoff_ts == cutoff
        assert events_w == count(repo, window_start, cutoff)
        assert stars_w == count(repo, window_start, cutoff, "WatchEvent")
        if split == "score":
            assert stars_next is None and label is None
        else:
            expected = count(repo, cutoff, cutoff + timedelta(hours=2), "WatchEvent")
            assert stars_next == expected
            assert label == int(expected >= 3)
    assert any(r[6] == 1 for r in rows if r[1] == "test")
