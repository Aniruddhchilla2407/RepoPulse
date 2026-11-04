"""Configuration loading and logging setup."""

import logging
import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = "config/pipeline.yaml"

DEFAULT_EVENT_TYPES = (
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "ForkEvent",
)

_REQUIRED_PATH_KEYS = ("raw", "bronze", "quarantine", "silver", "metrics")


def load_config(path: str | os.PathLike | None = None) -> dict[str, Any]:
    """Load the YAML config. Falls back to $REPOPULSE_CONFIG, then config/pipeline.yaml."""
    cfg_path = Path(path or os.environ.get("REPOPULSE_CONFIG", DEFAULT_CONFIG_PATH))
    if not cfg_path.is_file():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")
    with cfg_path.open(encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}

    paths = cfg.get("paths", {})
    missing = [key for key in _REQUIRED_PATH_KEYS if key not in paths]
    if missing:
        raise ValueError(f"Config is missing paths: {missing}")

    cfg.setdefault("event_types", list(DEFAULT_EVENT_TYPES))
    cfg.setdefault("ingest", {})
    cfg.setdefault("spark", {})
    return cfg


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )