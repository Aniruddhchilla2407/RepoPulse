import pytest

pytest.importorskip("duckdb")

from repopulse import warehouse as wh 


def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("REPOPULSE_DB", raising=False)
    monkeypatch.delenv("REPOPULSE_DUCKDB_MEMORY", raising=False)
    s = wh.settings({})
    assert s["path"] == wh.DEFAULT_DB_PATH
    assert s["memory_limit"] == "3GB"
    assert s["threads"] == 4


def test_environment_overrides_config(monkeypatch):
    monkeypatch.setenv("REPOPULSE_DB", "other/path.duckdb")
    monkeypatch.setenv("REPOPULSE_DUCKDB_MEMORY", "2GB")
    s = wh.settings({"warehouse": {"path": "cfg.duckdb", "memory_limit": "9GB"}})
    assert s["path"] == "other/path.duckdb"
    assert s["memory_limit"] == "2GB"


def test_invalid_memory_limit_is_rejected(monkeypatch):
    monkeypatch.delenv("REPOPULSE_DUCKDB_MEMORY", raising=False)
    with pytest.raises(ValueError):
        wh.settings({"warehouse": {"memory_limit": "3GB; DROP TABLE x"}})
    with pytest.raises(ValueError):
        wh.connect(":memory:", memory_limit="lots")


def test_connect_uses_utc_and_creates_parent_directory(tmp_path):
    path = tmp_path / "nested" / "w.duckdb"
    con = wh.connect(str(path), memory_limit="1GB", threads=1)
    try:
        assert con.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"
    finally:
        con.close()
    assert path.exists()


def test_describe_lists_tables_and_views_with_row_counts():
    con = wh.connect(":memory:", memory_limit="1GB", threads=1)
    try:
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE SCHEMA gold")
        con.execute("CREATE TABLE raw.t AS SELECT * FROM range(3) t(x)")
        con.execute("CREATE VIEW gold.v AS SELECT * FROM raw.t")
        described = {d["table"]: d for d in wh.describe(con)}
    finally:
        con.close()
    assert described["raw.t"]["rows"] == 3
    assert described["gold.v"]["rows"] == 3
    assert described["gold.v"]["type"] == "VIEW"