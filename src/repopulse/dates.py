"""Date-range handling: a range of hours -> a list of hourly partitions."""

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta

DATE_FORMAT = "%Y-%m-%d"


@dataclass(frozen=True, order=True)
class HourPartition:
    """One GH Archive hourly file, and the Parquet partition derived from it."""

    date: str  # YYYY-MM-DD (UTC)
    hour: int  # 0-23, not zero-padded in GH Archive filenames

    @property
    def filename(self) -> str:
        return f"{self.date}-{self.hour}.json.gz"

    @property
    def partition_dir(self) -> str:
        return f"source_date={self.date}/source_hour={self.hour}"


def _to_datetime(date_str: str, hour: int) -> datetime:
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be between 0 and 23, got {hour}")
    return datetime.strptime(date_str, DATE_FORMAT).replace(hour=hour)


def hour_range(
    start_date: str, end_date: str, start_hour: int = 0, end_hour: int = 23
) -> list[HourPartition]:
    """Inclusive range from (start_date, start_hour) to (end_date, end_hour)."""
    start = _to_datetime(start_date, start_hour)
    end = _to_datetime(end_date, end_hour)
    if end < start:
        raise ValueError(f"end ({end:%Y-%m-%d %H}h) is before start ({start:%Y-%m-%d %H}h)")

    partitions = []
    current = start
    while current <= end:
        partitions.append(HourPartition(current.strftime(DATE_FORMAT), current.hour))
        current += timedelta(hours=1)
    return partitions


def add_range_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", required=True, help="first date, YYYY-MM-DD (UTC)")
    parser.add_argument("--end", default=None, help="last date, YYYY-MM-DD (default: --start)")
    parser.add_argument("--start-hour", type=int, default=0, help="first hour on --start (0-23)")
    parser.add_argument("--end-hour", type=int, default=23, help="last hour on --end (0-23)")


def partitions_from_args(args: argparse.Namespace) -> list[HourPartition]:
    return hour_range(args.start, args.end or args.start, args.start_hour, args.end_hour)