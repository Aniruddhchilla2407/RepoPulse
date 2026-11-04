from collections import Counter

from repopulse.config import DEFAULT_EVENT_TYPES
from repopulse.transforms.bronze import parse_lines, quarantined, valid_events

EXTRA_BAD_LINES = ["", "   "]


def test_bad_lines_are_quarantined_with_reasons(make_raw, sample_lines):
    parsed = parse_lines(make_raw(sample_lines + EXTRA_BAD_LINES))
    rows = quarantined(parsed).collect()

    assert len(rows) == 6  # truncated, garbage, missing id, bad timestamp, 2 blank lines
    reasons = Counter(r.reason for r in rows)
    assert reasons["empty_line"] == 2
    assert reasons["invalid_timestamp"] == 1
    # truncated JSON, garbage text and the line without an id: reason depends on how Spark
    # reports the failure, but each must be quarantined for one of these two reasons
    assert reasons["malformed_record"] + reasons["missing_required_field"] == 3
    assert set(reasons) <= {
        "empty_line",
        "malformed_record",
        "missing_required_field",
        "invalid_timestamp",
    }
    # the raw text is preserved for later inspection
    assert "this is not json at all" in {r.raw_line for r in rows}


def test_non_target_event_types_are_dropped_not_quarantined(make_raw, sample_lines):
    parsed = parse_lines(make_raw(sample_lines))
    kept = valid_events(parsed, DEFAULT_EVENT_TYPES).collect()
    ids = [r.id for r in kept]

    assert "1006" not in ids  # CreateEvent is filtered out...
    assert len(ids) == 8  # ...5 valid + 1 duplicate + drifted + bad-payload push
    assert not any('"id":"1006"' in r.raw_line for r in quarantined(parsed).collect())


def test_partition_columns_come_from_filename(make_raw, sample_lines):
    parsed = parse_lines(make_raw(sample_lines, date="2024-06-02", hour=13))
    rows = valid_events(parsed, DEFAULT_EVENT_TYPES).collect()
    assert {(r.source_date, r.source_hour) for r in rows} == {("2024-06-02", 13)}


def test_schema_drift_is_tolerated(make_raw, sample_lines):
    parsed = parse_lines(make_raw(sample_lines))
    rows = [r for r in valid_events(parsed, DEFAULT_EVENT_TYPES).collect() if r.id == "1007"]

    assert len(rows) == 1  # numeric id, missing login/org, unknown extra field: all accepted
    row = rows[0]
    assert row.type == "WatchEvent"
    assert row.actor_id == 5
    assert row.actor_login is None
    assert row.org_id is None


def test_custom_event_type_filter(make_raw, sample_lines):
    parsed = parse_lines(make_raw(sample_lines))
    rows = valid_events(parsed, ["PushEvent"]).collect()
    assert sorted(r.id for r in rows) == ["1001", "1001", "1008"]