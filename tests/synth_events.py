"""Deterministic synthetic events for the SQL end-to-end tests (no network, no Spark)."""

import random
from datetime import date, datetime, timedelta, timezone

from repopulse import load_warehouse

TYPES = ["PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent", "ForkEvent"]
REPO_IDS = list(range(1000, 1020))
HOT_REPOS = REPO_IDS[:5]
ACTORS = list(range(100, 140))
BOT_ID = 999
HOURS = 6


def base_day() -> date:
    """A recent past day, so the 'no future timestamps' test always holds."""
    return datetime.now(timezone.utc).date() - timedelta(days=2)


def _event(counter, day, hour, repo, actor, login, event_type, rng):
    created = datetime(day.year, day.month, day.day, hour) + timedelta(seconds=rng.randint(0, 3599))
    row = {name: None for name, _ in load_warehouse.COLUMNS}
    row.update(
        event_id=f"evt-{counter}",
        event_type=event_type,
        created_at=created,
        event_date=created.date(),
        actor_id=actor,
        actor_login=login,
        repo_id=repo,
        repo_name=f"org{repo % 5}/repo{repo}",
        org_id=repo % 5,
        org_login=f"org{repo % 5}",
        is_public=True,
        source_file="synthetic",
        source_date=day,
        source_hour=hour,
    )
    if event_type == "PushEvent":
        row.update(push_ref="refs/heads/main", push_size=rng.randint(1, 5), push_distinct_size=1)
    elif event_type == "PullRequestEvent":
        action = rng.choice(["opened", "closed", "closed"])
        row.update(
            pr_action=action,
            pr_number=rng.randint(1, 50),
            pr_merged=(action == "closed" and rng.random() < 0.5),
            pr_additions=rng.randint(1, 100),
            pr_deletions=rng.randint(0, 50),
            pr_changed_files=rng.randint(1, 10),
        )
    elif event_type == "IssuesEvent":
        row.update(issue_action="opened", issue_number=rng.randint(1, 50))
    elif event_type == "WatchEvent":
        row.update(watch_action="started")
    elif event_type == "ForkEvent":
        row.update(fork_repo_id=repo * 10, fork_repo_name=f"fork/repo{repo}")
    return row


def build(day: date | None = None, seed: int = 7):
    """Returns (raw_rows, unique_events). raw_rows has one extra duplicate of an early event
    delivered in a later source hour; unique_events is what the warehouse should end up with."""
    rng = random.Random(seed)
    day = day or base_day()
    events: list[dict] = []
    counter = 0
    for hour in range(HOURS):
        for repo in REPO_IDS:
            for _ in range(4 + rng.randint(0, 3)):
                counter += 1
                event_type = rng.choices(TYPES, weights=[6, 2, 1, 1, 1])[0]
                actor = rng.choice(ACTORS)
                events.append(_event(counter, day, hour, repo, actor, f"user{actor}", event_type, rng))
            if repo in HOT_REPOS and hour >= 2:  # a burst of stars => positive labels
                for _ in range(4):
                    counter += 1
                    actor = rng.choice(ACTORS)
                    events.append(
                        _event(counter, day, hour, repo, actor, f"user{actor}", "WatchEvent", rng)
                    )
        counter += 1
        events.append(_event(counter, day, hour, REPO_IDS[0], BOT_ID, "dependabot[bot]", "PushEvent", rng))
    duplicate = {**events[0], "source_hour": 1}  # same event_id, delivered in a later file
    return events + [duplicate], events


def insert_rows(con, rows) -> None:
    names = [name for name, _ in load_warehouse.COLUMNS]
    placeholders = ", ".join("?" for _ in names)
    con.executemany(
        f"INSERT INTO {load_warehouse.TABLE} ({', '.join(names)}) VALUES ({placeholders})",
        [[row[name] for name in names] for row in rows],
    )