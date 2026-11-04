import pytest

from repopulse.ml import metrics as M

LABELS = [1, 0, 1, 0]
SCORES = [0.9, 0.8, 0.7, 0.1]


def test_roc_auc_cases():
    assert M.roc_auc([1, 1, 0, 0], [0.9, 0.8, 0.2, 0.1]) == pytest.approx(1.0)
    assert M.roc_auc([1, 1, 0, 0], [0.1, 0.2, 0.8, 0.9]) == pytest.approx(0.0)
    assert M.roc_auc([1, 0, 1, 0], [0.5, 0.5, 0.5, 0.5]) == pytest.approx(0.5)  # all tied
    assert M.roc_auc([0, 0, 0], [0.1, 0.2, 0.3]) is None  # one class only
    assert M.roc_auc(LABELS, SCORES) == pytest.approx(0.75)


def test_average_precision_known_value():
    assert M.average_precision(LABELS, SCORES) == pytest.approx(0.5 * 1.0 + 0.5 * (2 / 3))
    assert M.average_precision([0, 0], [0.2, 0.1]) is None


def test_precision_recall_at_threshold():
    precision, recall, tp, fp, fn = M.precision_recall_at(LABELS, SCORES, 0.75)
    assert (tp, fp, fn) == (1, 1, 1)
    assert precision == pytest.approx(0.5)
    assert recall == pytest.approx(0.5)


def test_best_f1_threshold():
    threshold, f1 = M.best_f1_threshold(LABELS, SCORES)
    assert threshold == pytest.approx(0.7)
    assert f1 == pytest.approx(0.8)
    assert M.best_f1_threshold([0, 0], [0.1, 0.2]) == (None, 0.0)


def test_precision_at_k_handles_ties_by_expected_value():
    assert M.precision_at_k(LABELS, SCORES, 2) == pytest.approx(0.5)
    assert M.precision_at_k([1, 0], [0.5, 0.5], 1) == pytest.approx(0.5)  # tie at the cut
    assert M.precision_at_k([], [], 1) is None


def test_evaluate_end_to_end():
    result = M.evaluate((LABELS, SCORES), (LABELS, SCORES), top_fraction=0.5)
    assert result["threshold"] == pytest.approx(0.7)
    assert result["precision"] == pytest.approx(2 / 3)
    assert result["recall"] == pytest.approx(1.0)
    assert result["k"] == 2
    assert result["precision_at_k"] == pytest.approx(0.5)
    assert result["base_rate_test"] == pytest.approx(0.5)
    assert result["lift_at_k"] == pytest.approx(1.0)
    assert result["n_pos_test"] == 2


def test_relative_change():
    assert M.relative_change(0.6, 0.5) == pytest.approx(20.0)
    assert M.relative_change(None, 0.5) is None
    assert M.relative_change(0.6, 0) is None