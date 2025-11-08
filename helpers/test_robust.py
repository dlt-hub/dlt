import pytest
from dlt.helpers.robust import (
    RobustScaleConfig, robust_scale_values, robust_scale_rows
)

def test_values_basic():
    x = [10, 20, 30, 40, 50]
    y = robust_scale_values(x)
    # with (x - median)/IQR, this maps to [-1, -0.5, 0, 0.5, 1]
    assert y == pytest.approx([-1.0, -0.5, 0.0, 0.5, 1.0], abs=1e-9)

def test_values_constant_returns_zeros():
    x = [5, 5, 5]
    y = robust_scale_values(x)
    assert y == [0.0, 0.0, 0.0]

def test_values_clip_range():
    x = [0, 100, 200, 300, 1_000_000]
    y = robust_scale_values(x, RobustScaleConfig(clip_min=-1.5, clip_max=1.5))
    assert all(-1.5 <= v <= 1.5 for v in y)

def test_rows_basic_and_non_numeric_untouched():
    rows = [{"a": 10, "b": "x"}, {"a": 30, "b": 15}, {"a": 50, "b": None}]
    out = list(robust_scale_rows(rows, columns=["a"]))
    # non-numeric column remains unchanged
    assert [r["b"] for r in out] == ["x", 15, None]
    # numeric column scaled; median row ~0.0; ends ±1.0
    assert out[0]["a"] == pytest.approx(-1.0, abs=1e-9)
    assert out[1]["a"] == pytest.approx( 0.0, abs=1e-9)
    assert out[2]["a"] == pytest.approx( 1.0, abs=1e-9)
