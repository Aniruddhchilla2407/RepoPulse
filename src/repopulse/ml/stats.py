"""Bootstrap helpers for comparing two models on the same rows."""

import random
from collections.abc import Callable, Sequence

from repopulse.ml import metrics as M


def paired_bootstrap_diff(
    labels: Sequence[int],
    scores_a: Sequence[float],
    scores_b: Sequence[float],
    metric: Callable[[Sequence[int], Sequence[float]], float | None] = M.average_precision,
    n_boot: int = 200,
    seed: int = 0,
    alpha: float = 0.05,
) -> dict | None:
    """metric(a) - metric(b) with a percentile bootstrap interval; rows are resampled together.

    Returns None when the metric is undefined on the data (for example, no positives).
    `significant` is True when the interval excludes zero.
    """
    point_a, point_b = metric(labels, scores_a), metric(labels, scores_b)
    if point_a is None or point_b is None:
        return None

    rng = random.Random(seed)
    n = len(labels)
    diffs = []
    for _ in range(n_boot):
        idx = [rng.randrange(n) for _ in range(n)]
        lab = [labels[i] for i in idx]
        a = metric(lab, [scores_a[i] for i in idx])
        b = metric(lab, [scores_b[i] for i in idx])
        if a is not None and b is not None:
            diffs.append(a - b)
    if not diffs:
        return None

    diffs.sort()
    low = diffs[int(alpha / 2 * len(diffs))]
    high = diffs[min(len(diffs) - 1, int((1 - alpha / 2) * len(diffs)))]
    return {
        "diff": point_a - point_b,
        "ci_low": low,
        "ci_high": high,
        "n_boot": len(diffs),
        "significant": low > 0 or high < 0,
    }