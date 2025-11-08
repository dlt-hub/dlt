import pytest
from dlt.helpers.robust import (
    RobustScaleConfig,
    robust_scale_values,
    robust_scale_rows,
    robust_scale_values_to_range,
    robust_scale_rows_to_range,
)

# ---------- base robust scaling ----------

def test_values_basic():
    x = [10, 20, 30, 40, 50]
    y = robust_scale_values(x)
    # with (x - median)/IQR, this maps to [-1, -0.5, 0, 0.5, 1]
    assert y == pytest.approx([-1.0, -0.5, 0.0, 0.5, 1.0], abs=1e-9)


def test_values_constant_returns_zeros():
    x = [5, 5, 5, 5]
    y = robust_scale_values(x)
    assert y == [0.0, 0.0, 0.0, 0.0]

def test_values_clip_range():
    x = [0, 100, 200, 300, 1_000_000]
    y = robust_scale_values(x, RobustScaleConfig(clip_min=-1.5, clip_max=1.5))
    assert all(-1.5 <= v <= 1.5 for v in y)

def test_rows_non_numeric_untouched_and_scaled_numeric():
    rows = [{"a": 10, "b": "x"}, {"a": 30, "b": 15}, {"a": 50, "b": None}]
    out = list(robust_scale_rows(rows, columns=["a"]))
    # 'b' unchanged
    assert [r["b"] for r in out] == ["x", 15, None]
    # 'a' scaled around [-1, 0, 1]
    assert out[0]["a"] == pytest.approx(-1.0, abs=1e-9)
    assert out[1]["a"] == pytest.approx( 0.0, abs=1e-9)
    assert out[2]["a"] == pytest.approx( 1.0, abs=1e-9)

def test_values_empty_returns_empty():
    assert robust_scale_values([]) == []

# ---------- to-range helpers ----------

def test_values_to_range_0_1_matches_expected_for_simple_series():
    x = [10, 20, 30, 40, 50]
    y = robust_scale_values_to_range(x, 0.0, 1.0)
    # robust scores are [-1, -0.5, 0, 0.5, 1] → normalized to [0,1]
    assert y == pytest.approx([0.0, 0.25, 0.5, 0.75, 1.0], abs=1e-9)

def test_values_to_range_constant_series_to_midpoint():
    x = [5, 5, 5]
    y = robust_scale_values_to_range(x, -1.0, 1.0)
    # midpoint of [-1,1] is 0
    assert y == pytest.approx([0.0, 0.0, 0.0], abs=1e-9)

def test_rows_to_range_basic():
    rows = [{"a": 10, "b": "x"}, {"a": 30, "b": 15}, {"a": 50, "b": None}]
    out = list(robust_scale_rows_to_range(rows, columns=["a"], target_min=0.0, target_max=1.0))
    assert [r["b"] for r in out] == ["x", 15, None]
    # 'a' expected → [0.0, 0.5, 1.0]
    assert out[0]["a"] == pytest.approx(0.0, abs=1e-9)
    assert out[1]["a"] == pytest.approx(0.5, abs=1e-9)
    assert out[2]["a"] == pytest.approx(1.0, abs=1e-9)
    
