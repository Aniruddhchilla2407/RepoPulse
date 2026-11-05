import pytest

from repopulse.ml import metrics as M
from repopulse.ml import stats

LABELS = [i % 2 for i in range(200)]
PERFECT = [0.9 if y == 1 else 0.1 for y in LABELS]
NOISE = [((i * 37) % 101) / 101 for i in range(200)]


def test_identical_models_have_zero_difference_and_are_not_significant():
    result = stats.paired_bootstrap_diff(LABELS, NOISE, NOISE, metric=M.roc_auc, n_boot=50)
    assert result["diff"] == pytest.approx(0.0)
    assert (result["ci_low"], result["ci_high"]) == (0.0, 0.0)
    assert result["significant"] is False


def test_clearly_better_model_is_significant():
    result = stats.paired_bootstrap_diff(LABELS, PERFECT, NOISE, metric=M.roc_auc, n_boot=100)
    assert result["diff"] > 0.3
    assert result["ci_low"] > 0
    assert result["significant"] is True


def test_undefined_metric_returns_none():
    assert stats.paired_bootstrap_diff([0, 0, 0], [0.1, 0.2, 0.3], [0.3, 0.2, 0.1]) is None