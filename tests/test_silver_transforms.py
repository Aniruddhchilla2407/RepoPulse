import datetime
import json

from repopulse.config import DEFAULT_EVENT_TYPES
from repopulse.schemas import SILVER_SCHEMA
from repopulse.transforms.bronze import parse_lines, valid_events
from repopulse.transforms.silver import build_silver


def _by_id(df):
    return {r.event_id: r for r in df.collect()}


def test_schema_matches_explicit_definition(bronze_df):
    silver = build_silver(bronze_df)
    actual = [(f.name, f.dataType) for f in silver.schema.fields]
    expected = [(f.name, f.dataType) for f in SILVER_SCHEMA.fields]
    assert actual == expected


def test_dedup_by_event_id(bronze_df):
    silver = build_silver(bronze_df)
    ids = [r.event_id for r in silver.collect()]
    assert len(ids) == len(set(ids)) == 7  # 8 bronze rows, event 1001 appeared twice


def test_dedup_keeps_earliest_source_partition(make_raw):
    base = {
        "id": "5001",
        "type": "WatchEvent",
        "created_at": "2024-06-01T01:00:00Z",
        "actor": {"id": 1, "login": "a"},
        "repo": {"id": 2, "name": "o/r"},
        "public": True,
    }
    first = json.dumps({**base, "payload": {"action": "started"}})
    later = json.dumps({**base, "payload": {"action": "restarted"}})
    raw = make_raw([later], hour=1).unionByName(make_raw([first], hour=0))

    rows = build_silver(valid_events(parse_lines(raw), DEFAULT_EVENT_TYPES)).collect()

    assert len(rows) == 1
    assert rows[0].watch_action == "started"
    assert rows[0].source_hour == 0


def test_payload_fields_extracted_per_type(bronze_df):
    rows = _by_id(build_silver(bronze_df))

    push = rows["1001"]
    assert (push.push_ref, push.push_size, push.push_distinct_size) == ("refs/heads/main", 3, 2)
    assert push.event_date == datetime.date(2024, 6, 1)
    assert push.pr_action is None  # fields of other event types stay null

    pr = rows["1002"]
    assert pr.pr_action == "closed"
    assert pr.pr_number == 7
    assert pr.pr_merged is True
    assert (pr.pr_additions, pr.pr_deletions, pr.pr_changed_files) == (10, 2, 3)
    assert pr.org_login == "acme"
    assert pr.push_size is None

    issue = rows["1003"]
    assert (issue.issue_action, issue.issue_number) == ("opened", 42)

    assert rows["1004"].watch_action == "started"

    fork = rows["1005"]
    assert (fork.fork_repo_id, fork.fork_repo_name) == (900, "bob/proj")


def test_bad_payload_field_becomes_null(bronze_df):
    rows = _by_id(build_silver(bronze_df))
    assert "1008" in rows  # the event is kept...
    assert rows["1008"].push_size is None  # ...but the un-parseable field is null