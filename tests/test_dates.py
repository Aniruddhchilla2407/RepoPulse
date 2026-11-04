import pytest

from repopulse.dates import HourPartition, hour_range


def test_full_day_has_24_hours():
    parts = hour_range("2024-06-01", "2024-06-01")
    assert len(parts) == 24
    assert parts[0] == HourPartition("2024-06-01", 0)
    assert parts[-1] == HourPartition("2024-06-01", 23)


def test_hour_window_and_unpadded_filenames():
    parts = hour_range("2024-06-01", "2024-06-01", 0, 2)
    assert [p.filename for p in parts] == [
        "2024-06-01-0.json.gz",
        "2024-06-01-1.json.gz",
        "2024-06-01-2.json.gz",
    ]
    assert parts[0].partition_dir == "source_date=2024-06-01/source_hour=0"


def test_range_crosses_midnight():
    parts = hour_range("2024-06-01", "2024-06-02", 22, 1)
    assert [(p.date, p.hour) for p in parts] == [
        ("2024-06-01", 22),
        ("2024-06-01", 23),
        ("2024-06-02", 0),
        ("2024-06-02", 1),
    ]


def test_end_before_start_raises():
    with pytest.raises(ValueError):
        hour_range("2024-06-02", "2024-06-01")


def test_invalid_hour_raises():
    with pytest.raises(ValueError):
        hour_range("2024-06-01", "2024-06-01", 0, 24)


def test_invalid_date_raises():
    with pytest.raises(ValueError):
        hour_range("2024-13-01", "2024-13-02")