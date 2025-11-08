import pytest
from dlt.helpers.robust import (
    RobustScaleConfig, robust_scale_values, robust_scale_rows
)

def test_values_basic():
    x = [10, 20, 30, 40, 50]
    y = robust_scale_values(x)
    assert len(y) == 5
    assert pytest.approx(y[2], abs=1e-9) == 0.0
    assert pytest.approx(y[0], rel=1e-2) == -2/3
    assert pytest.approx(y[-1], rel=1e-2) ==  2/3

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
    assert [r["b"] for r in out] == ["x", 15, None]
    assert pytest.approx(out[1]["a"], abs=1e-9) == 0.0
