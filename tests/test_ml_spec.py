from datetime import datetime, timezone

import pytest

from repopulse.ml import spec

UTC = timezone.utc
START = datetime(2026, 10, 27, 0, 0, tzinfo=UTC)
END = datetime(2026, 10, 27, 6, 0, tzinfo=UTC)


def test_cutoffs_are_walk_forward():
    c = spec.plan_cutoffs(START, END, feature_hours=2, horizon_hours=2)
    assert c.score == END
    assert c.test == datetime(2026, 10, 27, 4, 0, tzinfo=UTC)
    assert c.train == datetime(2026, 10, 27, 2, 0, tzinfo=UTC)


def test_plan_needs_enough_data():
    with pytest.raises(ValueError, match="needs"):
        spec.plan_cutoffs(START, END, feature_hours=72, horizon_hours=72)


def test_gaps_in_loaded_hours_are_detected():
    spec.check_contiguous(6, START, END)
    with pytest.raises(ValueError, match="missing"):
        spec.check_contiguous(5, START, END)


def test_param_validation():
    with pytest.raises(ValueError):
        spec.MLParams(feature_hours=1).validate()
    with pytest.raises(ValueError):
        spec.MLParams(threshold=0).validate()
    params = spec.resolve_params({"horizon_hours": 48}, feature_hours=6, threshold=3)
    assert (params.feature_hours, params.horizon_hours, params.threshold) == (6, 48, 3)


def test_feature_sql_contains_cutoffs_parameters_and_features():
    cutoffs = spec.plan_cutoffs(START, END, 2, 2)
    params = spec.MLParams(feature_hours=2, horizon_hours=2, threshold=3, min_events=2)
    sql = spec.render_features_sql("gold", "fact_events", "feat", cutoffs, params)

    assert "gold.feat AS" in sql
    assert "gold.fact_events" in sql
    assert "TIMESTAMP '2026-10-27 02:00:00'" in sql
    assert "TIMESTAMP '2026-10-27 04:00:00'" in sql
    assert "TIMESTAMP '2026-10-27 06:00:00'" in sql
    assert ">= 3 AS INTEGER" in sql
    assert "events_w >= 2" in sql
    for column in spec.FEATURE_COLUMNS:
        assert column in sql
    assert "{" not in sql and "}" not in sql  # nothing left un-rendered


def test_unsafe_identifiers_are_rejected():
    cutoffs = spec.plan_cutoffs(START, END, 2, 2)
    params = spec.MLParams(feature_hours=2, horizon_hours=2)
    with pytest.raises(ValueError):
        spec.render_features_sql("gold; DROP TABLE x", "fact_events", "feat", cutoffs, params)