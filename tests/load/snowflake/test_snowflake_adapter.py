"""Unit tests for snowflake_adapter — validates the API + hint
propagation without hitting a live Snowflake account.

Integration tests that exercise the full ALTER TABLE/COMMENT/SET TAG
SQL emission live in test_snowflake_table_builder.py.
"""
from tests.utils import skip_if_not_active

skip_if_not_active("snowflake")

from typing import Iterator

import pytest

import dlt
from dlt.destinations.adapters import snowflake_adapter
from dlt.destinations.impl.snowflake.snowflake_adapter import (
    COLUMN_COMMENT_HINT,
    COLUMN_TAGS_HINT,
    TABLE_COMMENT_HINT,
    TABLE_TAGS_HINT,
)


pytestmark = pytest.mark.essential


@dlt.resource(columns={"id": {"data_type": "bigint"}})
def _demo() -> Iterator[dict]:
    yield {"id": 1}


def test_adapter_applies_table_comment() -> None:
    res = snowflake_adapter(_demo.with_name("a"), table_comment="hello")
    assert res.compute_table_schema().get(TABLE_COMMENT_HINT) == "hello"


def test_adapter_applies_table_tags() -> None:
    res = snowflake_adapter(
        _demo.with_name("b"),
        table_tags={"governance.domain": "commerce", "cost_center": "eng"},
    )
    tags = res.compute_table_schema().get(TABLE_TAGS_HINT)
    assert tags == {"governance.domain": "commerce", "cost_center": "eng"}


def test_adapter_applies_column_comments_and_tags() -> None:
    res = snowflake_adapter(
        _demo.with_name("c"),
        column_comments={"id": "primary identifier"},
        column_tags={"id": {"governance.pii_class": "public"}},
    )
    cols = res.compute_table_schema()["columns"]
    assert cols["id"].get(COLUMN_COMMENT_HINT) == "primary identifier"
    assert cols["id"].get(COLUMN_TAGS_HINT) == {"governance.pii_class": "public"}


def test_adapter_table_tags_must_be_dict() -> None:
    """Snowflake tags require key=value pairs, not free strings (which
    Databricks accepts). Reject the list shape early."""
    with pytest.raises(ValueError, match="table_tags.*must be a dict"):
        snowflake_adapter(_demo.with_name("d"), table_tags=["foo", "bar"])  # type: ignore[arg-type]


def test_adapter_table_tag_value_types() -> None:
    """Tag values can be string/int/bool/float — reject everything else."""
    with pytest.raises(ValueError, match="value must be a string"):
        snowflake_adapter(
            _demo.with_name("e"),
            table_tags={"governance.domain": ["nested", "list"]},  # type: ignore[dict-item]
        )


def test_adapter_column_tags_per_column_must_be_dict() -> None:
    with pytest.raises(ValueError, match="column_tags.*must be a dict"):
        snowflake_adapter(
            _demo.with_name("f"),
            column_tags={"id": ["a", "b"]},  # type: ignore[dict-item]
        )


def test_adapter_no_arguments_raises() -> None:
    """An empty adapter() call is almost always a bug — surface it."""
    with pytest.raises(ValueError, match="at least one of"):
        snowflake_adapter(_demo.with_name("g"))


def test_adapter_table_comment_string_only() -> None:
    with pytest.raises(ValueError, match="table_comment.*must be a string"):
        snowflake_adapter(_demo.with_name("h"), table_comment=123)  # type: ignore[arg-type]


def test_adapter_chains_with_existing_hints() -> None:
    """The adapter must not clobber column hints set via @dlt.resource()."""
    res = snowflake_adapter(
        _demo.with_name("i"),
        column_comments={"id": "the id column"},
    )
    cols = res.compute_table_schema()["columns"]
    # Original hint from @dlt.resource preserved …
    assert cols["id"]["data_type"] == "bigint"
    # … plus the new one stacked on top.
    assert cols["id"].get(COLUMN_COMMENT_HINT) == "the id column"
