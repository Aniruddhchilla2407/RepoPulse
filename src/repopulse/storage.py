"""Partition helpers (idempotent re-runs) and run-metrics logging."""

import json
import shutil
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from repopulse.dates import HourPartition


def partition_path(base: Path, part: HourPartition) -> Path:
    return Path(base) / part.partition_dir


def existing_partitions(base: Path, partitions: Iterable[HourPartition]) -> list[HourPartition]:
    return [p for p in partitions if partition_path(base, p).is_dir()]


def purge_partitions(base: Path, partitions: Iterable[HourPartition]) -> int:
    """Delete exactly the given hour partitions under base. Returns how many existed."""
    removed = 0
    for part in partitions:
        target = partition_path(base, part)
        if target.is_dir():
            shutil.rmtree(target)
            removed += 1
    return removed


def append_metrics(metrics_file: Path | None, record: dict) -> None:
    """Append one JSON line describing a job run. No-op when metrics_file is None."""
    if metrics_file is None:
        return
    metrics_file = Path(metrics_file)
    metrics_file.parent.mkdir(parents=True, exist_ok=True)
    entry = {"timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"), **record}
    with metrics_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")