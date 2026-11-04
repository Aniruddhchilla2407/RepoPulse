"""Binary-classification metrics in pure Python, so every model is scored by identical code."""

import math
from collections.abc import Iterator, Sequence


def _groups(labels: Sequence[int], scores: Sequence[float]) -> Iterator[tuple[float, int, int]]:
    """Yield (score, group size, positives in group) per distinct score, highest first."""
    pairs = sorted(zip(scores, labels), key=lambda t: -t[0])
    i, n = 0, len(pairs)
    while i < n:
        score = pairs[i][0]
        j, positives = i, 0
        while j < n and pairs[j][0] == score:
            positives += 1 if pairs[j][1] == 1 else 0
            j += 1
        yield score, j - i, positives
        i = j


def roc_auc(labels: Sequence[int], scores: Sequence[float]) -> float | None:
    total_pos = sum(1 for y in labels if y == 1)
    total_neg = len(labels) - total_pos
    if total_pos == 0 or total_neg == 0:
        return None
    neg_above, wins = 0, 0.0
    for _, size, pos in _groups(labels, scores):
        neg = size - pos
        neg_below = total_neg - neg_above - neg
        wins += pos * (neg_below + 0.5 * neg)  # ties count half
        neg_above += neg
    return wins / (total_pos * total_neg)


def average_precision(labels: Sequence[int], scores: Sequence[float]) -> float | None:
    """Area under the precision-recall curve (step-wise, i.e. average precision)."""
    total_pos = sum(1 for y in labels if y == 1)
    if total_pos == 0:
        return None
    tp = seen = 0
    prev_recall = ap = 0.0
    for _, size, pos in _groups(labels, scores):
        tp += pos
        seen += size
        recall = tp / total_pos
        ap += (recall - prev_recall) * (tp / seen)
        prev_recall = recall
    return ap


def precision_recall_at(
    labels: Sequence[int], scores: Sequence[float], threshold: float
) -> tuple[float | None, float | None, int, int, int]:
    """Predict positive when score >= threshold. Returns precision, recall, tp, fp, fn."""
    tp = fp = fn = 0
    for y, s in zip(labels, scores):
        predicted = s >= threshold
        if predicted and y == 1:
            tp += 1
        elif predicted:
            fp += 1
        elif y == 1:
            fn += 1
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    return precision, recall, tp, fp, fn


def best_f1_threshold(
    labels: Sequence[int], scores: Sequence[float]
) -> tuple[float | None, float]:
    """Score threshold maximising F1 on the given data. (None, 0.0) if there are no positives."""
    total_pos = sum(1 for y in labels if y == 1)
    if total_pos == 0:
        return None, 0.0
    tp = fp = 0
    best_threshold, best_f1 = None, 0.0
    for score, size, pos in _groups(labels, scores):
        tp += pos
        fp += size - pos
        precision = tp / (tp + fp)
        recall = tp / total_pos
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        if f1 > best_f1:
            best_threshold, best_f1 = score, f1
    return best_threshold, best_f1


def precision_at_k(labels: Sequence[int], scores: Sequence[float], k: int) -> float | None:
    """Precision among the top-k scores. Ties straddling k are resolved by expected value."""
    n = len(labels)
    if n == 0 or k <= 0:
        return None
    k = min(k, n)
    taken, expected_pos = 0, 0.0
    for _, size, pos in _groups(labels, scores):
        room = k - taken
        if room <= 0:
            break
        if size <= room:
            expected_pos += pos
            taken += size
        else:
            expected_pos += room * pos / size
            taken += room
    return expected_pos / k


def relative_change(new: float | None, old: float | None) -> float | None:
    """Percent change of `new` relative to `old`."""
    if new is None or old is None or old == 0:
        return None
    return (new - old) / old * 100.0


def evaluate(
    valid: tuple[Sequence[int], Sequence[float]],
    test: tuple[Sequence[int], Sequence[float]],
    top_fraction: float = 0.01,
) -> dict:
    """Threshold is chosen on `valid` (best F1) and applied to `test`; ranking metrics use `test`."""
    labels_v, scores_v = valid
    labels_t, scores_t = test
    n_test = len(labels_t)
    n_pos = sum(1 for y in labels_t if y == 1)
    base_rate = n_pos / n_test if n_test else None
    k = max(1, math.ceil(top_fraction * n_test)) if n_test else 0

    threshold, _ = best_f1_threshold(labels_v, scores_v)
    precision = recall = f1 = None
    if threshold is not None:
        precision, recall, *_ = precision_recall_at(labels_t, scores_t, threshold)
        if precision is not None and recall is not None and precision + recall > 0:
            f1 = 2 * precision * recall / (precision + recall)

    p_at_k = precision_at_k(labels_t, scores_t, k)
    lift = p_at_k / base_rate if p_at_k is not None and base_rate else None
    return {
        "n_valid": len(labels_v),
        "n_test": n_test,
        "n_pos_test": n_pos,
        "base_rate_test": base_rate,
        "threshold": threshold,
        "auc_roc": roc_auc(labels_t, scores_t),
        "auc_pr": average_precision(labels_t, scores_t),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "top_fraction": top_fraction,
        "k": k,
        "precision_at_k": p_at_k,
        "lift_at_k": lift,
    }