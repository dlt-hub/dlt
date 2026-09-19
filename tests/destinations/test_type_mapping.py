"""Unit tests for TypeMapperImpl.precision_tuple_or_default.

Regression tests for https://github.com/dlt-hub/dlt/issues/4463: when a column
drifts from decimal to text, a stale scale may remain on the column schema.
precision_tuple_or_default must not return the scale for non-decimal types, or
single-argument templates like VARCHAR(%i) crash with a TypeError.
"""
from types import SimpleNamespace

import pytest

from dlt.destinations.type_mapping import TypeMapperImpl


class SnowflakeLikeMapper(TypeMapperImpl):
    """Minimal mapper with Snowflake-style templates for unit testing."""

    sct_to_unbound_dbt = {"text": "VARCHAR", "decimal": "DECIMAL", "time": "TIME"}
    sct_to_dbt = {
        "text": "VARCHAR(%i)",
        "decimal": "NUMBER(%i,%i)",
        "time": "TIME(%i)",
    }
    dbt_to_sct = {"VARCHAR": "text"}


@pytest.fixture
def mapper():
    capabilities = SimpleNamespace(decimal_precision=(38, 9), wei_precision=(38, 0))
    return SnowflakeLikeMapper(capabilities)


def test_stale_scale_ignored_for_text(mapper):
    """A text column carrying a stale decimal scale must map without crashing."""
    column = {"name": "value", "data_type": "text", "precision": 50, "scale": 2}
    assert mapper.to_destination_type(column, {}) == "VARCHAR(50)"


def test_precision_tuple_ignores_stale_scale(mapper):
    column = {"name": "value", "data_type": "text", "precision": 50, "scale": 2}
    assert mapper.precision_tuple_or_default("text", column) == (50,)


def test_text_precision_without_scale_unchanged(mapper):
    column = {"name": "value", "data_type": "text", "precision": 50}
    assert mapper.to_destination_type(column, {}) == "VARCHAR(50)"


def test_text_without_precision_unchanged(mapper):
    column = {"name": "value", "data_type": "text"}
    assert mapper.to_destination_type(column, {}) == "VARCHAR"


def test_decimal_still_uses_scale(mapper):
    column = {"name": "value", "data_type": "decimal", "precision": 10, "scale": 3}
    assert mapper.to_destination_type(column, {}) == "NUMBER(10,3)"


def test_decimal_defaults_still_apply(mapper):
    column = {"name": "value", "data_type": "decimal"}
    assert mapper.to_destination_type(column, {}) == "NUMBER(38,9)"


def test_time_precision_with_stale_scale(mapper):
    """Single-argument timestamp/time templates must not receive a stale scale."""
    column = {"name": "value", "data_type": "time", "precision": 3, "scale": 2}
    assert mapper.to_destination_type(column, {}) == "TIME(3)"
