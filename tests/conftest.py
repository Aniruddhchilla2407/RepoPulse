from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "sample_events.jsonl"


@pytest.fixture(scope="session")
def spark():
    from repopulse.spark_session import build_spark

    session = build_spark(
        "repopulse-tests", master="local[2]", driver_memory="1g", shuffle_partitions=2
    )
    yield session
    session.stop()


@pytest.fixture
def sample_lines() -> list[str]:
    return FIXTURE.read_text(encoding="utf-8").splitlines()


@pytest.fixture
def make_raw(spark):
    """Factory: list of raw lines -> DataFrame(value, source_file) like the bronze job reads."""

    def _make(lines, date="2024-06-01", hour=0):
        source = f"file:///app/data/raw/{date}-{hour}.json.gz"
        return spark.createDataFrame(
            [(line, source) for line in lines], "value string, source_file string"
        )

    return _make


@pytest.fixture
def bronze_df(make_raw, sample_lines):
    from repopulse.config import DEFAULT_EVENT_TYPES
    from repopulse.transforms.bronze import parse_lines, valid_events

    return valid_events(parse_lines(make_raw(sample_lines)), DEFAULT_EVENT_TYPES)