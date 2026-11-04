"""DuckDB warehouse: settings, connections, and a tiny read-only inspection CLI."""

import argparse
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import duckdb

from repopulse.config import load_config, setup_logging

log = logging.getLogger("repopulse.warehouse")

DEFAULT_DB_PATH = "data/warehouse/repopulse.duckdb"
RAW_SCHEMA = "raw"
GOLD_SCHEMA = "gold"
RAW_TABLE = "silver_events"

_MEMORY = re.compile(r"\d+(\.\d+)?\s?(KB|MB|GB|TB|KiB|MiB|GiB|TiB)", re.IGNORECASE)


def settings(cfg: dict[str, Any]) -> dict[str, Any]:
    """Resolve warehouse settings. Environment variables win over config/pipeline.yaml."""
    wh = cfg.get("warehouse") or {}
    memory = str(os.environ.get("REPOPULSE_DUCKDB_MEMORY") or wh.get("memory_limit", "3GB"))
    if not _MEMORY.fullmatch(memory):
        raise ValueError(f"invalid memory_limit {memory!r} (example: 3GB)")
    return {
        "path": str(os.environ.get("REPOPULSE_DB") or wh.get("path", DEFAULT_DB_PATH)),
        "memory_limit": memory,
        "threads": int(wh.get("threads", 4)),
    }


def connect(
    path: str, *, read_only: bool = False, memory_limit: str = "3GB", threads: int = 4
) -> duckdb.DuckDBPyConnection:
    """Open the warehouse. All timestamps in it are UTC (stored without a time zone)."""
    if not _MEMORY.fullmatch(memory_limit):
        raise ValueError(f"invalid memory_limit {memory_limit!r}")
    if path != ":memory:" and not read_only:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(path, read_only=read_only)
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET threads = {int(threads)}")
    return con


def open_warehouse(cfg: dict[str, Any], *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    s = settings(cfg)
    return connect(
        s["path"], read_only=read_only, memory_limit=s["memory_limit"], threads=s["threads"]
    )


def describe(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = con.execute(
        "SELECT table_schema, table_name, table_type FROM information_schema.tables "
        "WHERE table_schema IN (?, ?) ORDER BY table_schema, table_name",
        [RAW_SCHEMA, GOLD_SCHEMA],
    ).fetchall()
    out = []
    for schema, name, kind in rows:
        count = con.execute(f'SELECT COUNT(*) FROM "{schema}"."{name}"').fetchone()[0]
        out.append({"table": f"{schema}.{name}", "type": kind, "rows": int(count)})
    return out


def _print_rows(columns: list[str], rows: list[tuple]) -> None:
    widths = [
        max([len(str(col))] + [len(str(row[i])) for row in rows]) for i, col in enumerate(columns)
    ]
    print("  ".join(str(col).ljust(w) for col, w in zip(columns, widths)))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(row, widths)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect the DuckDB warehouse (read-only).")
    parser.add_argument("--sql", default=None, help="run one query and print up to 50 rows")
    parser.add_argument("--config", default=None, help="path to pipeline.yaml")
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    s = settings(cfg)
    exists = s["path"] == ":memory:" or Path(s["path"]).exists()
    info: dict[str, Any] = {"duckdb_version": duckdb.__version__, "path": s["path"], "exists": exists}
    if not exists:
        print(json.dumps(info, indent=2))
        return 0

    con = open_warehouse(cfg, read_only=True)
    try:
        if args.sql:
            cursor = con.execute(args.sql)
            columns = [d[0] for d in cursor.description]
            _print_rows(columns, cursor.fetchmany(50))
        else:
            info["tables"] = describe(con)
            print(json.dumps(info, indent=2))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())