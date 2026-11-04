"""Download GH Archive hourly dumps: resumable, verified, atomic, idempotent."""

import argparse
import gzip
import logging
import os
import shutil
import time
import zlib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import requests

from repopulse.config import load_config, setup_logging
from repopulse.dates import HourPartition, add_range_args, partitions_from_args

log = logging.getLogger("repopulse.ingest")


@dataclass(frozen=True)
class DownloadResult:
    partition: HourPartition
    status: str  # downloaded | skipped | missing | failed
    size_bytes: int = 0
    detail: str = ""


def _gzip_is_valid(path: Path) -> bool:
    """Read the whole file through gzip to catch truncation/corruption."""
    try:
        with gzip.open(path, "rb") as fh:
            while fh.read(1 << 20):
                pass
        return True
    except (OSError, EOFError, zlib.error):
        return False


def download_hour(
    part: HourPartition,
    raw_dir: Path,
    *,
    base_url: str,
    max_retries: int = 4,
    backoff_seconds: float = 2.0,
    timeout_seconds: float = 60.0,
    force: bool = False,
) -> DownloadResult:
    dest = raw_dir / part.filename
    if dest.is_file() and dest.stat().st_size > 0 and not force:
        return DownloadResult(part, "skipped", dest.stat().st_size)

    raw_dir.mkdir(parents=True, exist_ok=True)
    tmp = raw_dir / (part.filename + ".part")
    url = f"{base_url.rstrip('/')}/{part.filename}"
    last_error = ""

    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout_seconds) as resp:
                if resp.status_code == 404:
                    return DownloadResult(part, "missing", 0, "HTTP 404 (hour not published)")
                resp.raise_for_status()
                expected = int(resp.headers.get("Content-Length", "0") or 0)
                with tmp.open("wb") as fh:
                    # resp.raw = undecoded bytes, so the .gz is stored exactly as served
                    shutil.copyfileobj(resp.raw, fh, length=1 << 20)

            size = tmp.stat().st_size
            if expected and size != expected:
                raise OSError(f"truncated download: got {size} of {expected} bytes")
            if not _gzip_is_valid(tmp):
                raise OSError("downloaded file is not a valid gzip stream")

            os.replace(tmp, dest)  # atomic: dest only ever exists complete
            return DownloadResult(part, "downloaded", size)
        except (requests.RequestException, OSError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            log.warning("attempt %d/%d failed for %s: %s", attempt, max_retries, part.filename, last_error)
            tmp.unlink(missing_ok=True)
            if attempt < max_retries:
                time.sleep(backoff_seconds * 2 ** (attempt - 1))

    return DownloadResult(part, "failed", 0, last_error)


def _human(num_bytes: int) -> str:
    return f"{num_bytes / (1 << 20):.1f} MB" if num_bytes else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download GH Archive hourly files.")
    add_range_args(parser)
    parser.add_argument("--config", default=None, help="path to pipeline.yaml")
    parser.add_argument("--workers", type=int, default=None, help="parallel downloads")
    parser.add_argument("--force", action="store_true", help="re-download existing files")
    args = parser.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    icfg = cfg["ingest"]
    raw_dir = Path(cfg["paths"]["raw"])
    partitions = partitions_from_args(args)
    workers = args.workers or int(icfg.get("workers", 3))
    log.info("requesting %d hourly file(s) with %d worker(s)", len(partitions), workers)

    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                download_hour,
                part,
                raw_dir,
                base_url=icfg.get("base_url", "https://data.gharchive.org"),
                max_retries=int(icfg.get("max_retries", 4)),
                backoff_seconds=float(icfg.get("backoff_seconds", 2.0)),
                timeout_seconds=float(icfg.get("timeout_seconds", 60)),
                force=args.force,
            )
            for part in partitions
        ]
        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)
            log.info(
                "%-10s %s %s %s", res.status, res.partition.filename, _human(res.size_bytes), res.detail
            )

    counts = Counter(r.status for r in results)
    log.info(
        "summary: downloaded=%d skipped=%d missing=%d failed=%d",
        counts["downloaded"], counts["skipped"], counts["missing"], counts["failed"],
    )
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())