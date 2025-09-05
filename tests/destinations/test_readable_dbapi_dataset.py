"""Unit tests for readable db api dataset and relation"""
from typing import cast

import dlt
import pytest

from dlt.dataset.exceptions import LineageFailedException
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import LOADS_TABLE_NAME, VERSION_TABLE_NAME
from dlt.common.schema.utils import new_table
from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
from dlt.destinations.dataset.relation import ReadableDBAPIRelation


@pytest.fixture
def mock_dataset() -> ReadableDBAPIDataset:
    s = Schema("my_schema")
    t = new_table(
        "my_table",
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "text"},
        ],
    )
    s.update_table(t)
    dataset = dlt.dataset(
        dlt.destinations.duckdb(destination_name="duck_db"),
        "pipeline_dataset",
        schema=s,
    )
    return dataset


def test_dataset_autocompletion(mock_dataset: ReadableDBAPIDataset):
    expected_suggestions = ["my_table", LOADS_TABLE_NAME, VERSION_TABLE_NAME]
    suggestions = mock_dataset._ipython_key_completions_()
    assert set(expected_suggestions) == set(suggestions)


def test_relation_autocompletion(mock_dataset: ReadableDBAPIDataset):
    expected_suggestions = ["col1", "col2"]
    suggestions = mock_dataset["my_table"]._ipython_key_completions_()
    assert set(expected_suggestions) == set(suggestions)


def test_query_builder(mock_dataset: ReadableDBAPIDataset) -> None:
    relation = cast(ReadableDBAPIRelation, mock_dataset.my_table)

    # default query for a table
    assert (
        relation.to_sql().strip()
        == 'SELECT "my_table"."col1" AS "col1", "my_table"."col2" AS "col2" FROM'
        ' "pipeline_dataset"."my_table" AS "my_table"'
    )

    # head query
    assert (
        relation.head().to_sql().strip()
        == 'SELECT "my_table"."col1" AS "col1", "my_table"."col2" AS "col2" FROM'
        ' "pipeline_dataset"."my_table" AS "my_table" LIMIT 5'
    )

    # limit query
    assert (
        relation.limit(24).to_sql().strip()
        == 'SELECT "my_table"."col1" AS "col1", "my_table"."col2" AS "col2" FROM'
        ' "pipeline_dataset"."my_table" AS "my_table" LIMIT 24'
    )

    # select columns
    assert (
        relation.select("col1").to_sql().strip()
        == 'SELECT "my_table"."col1" AS "col1" FROM "pipeline_dataset"."my_table" AS "my_table"'
    )
    # also indexer notation
    assert (
        relation[["col2"]].to_sql().strip()
        == 'SELECT "my_table"."col2" AS "col2" FROM "pipeline_dataset"."my_table" AS "my_table"'
    )

    # limit and select chained
    assert (
        relation.select("col1").limit(24).to_sql().strip()
        == 'SELECT "my_table"."col1" AS "col1" FROM "pipeline_dataset"."my_table" AS "my_table"'
        " LIMIT 24"
    )


def test_copy_and_chaining() -> None:
    dataset = dlt.dataset(
        dlt.destinations.duckdb(destination_name="duck_db"),
        "pipeline_dataset",
    )

    dataset.schema.tables["items"] = new_table(
        "items",
        columns=[{"data_type": "text", "name": "one"}, {"data_type": "json", "name": "two"}],
    )

    # create relation and set some stuff on it
    relation = cast(ReadableDBAPIRelation, dataset.items)
    relation = relation.limit(34)
    relation = relation[["one", "two"]]

    relation2 = relation.__copy__()
    assert relation != relation2
    assert relation.sqlglot_expression == relation2.sqlglot_expression

    # test copy while chaining limit
    relation3 = relation2.limit(22)
    assert relation2 != relation3
    assert relation2.sqlglot_expression != relation3.sqlglot_expression

    # test last setting prevails chaining
    limit_expr = relation.limit(23).limit(67).limit(11).sqlglot_expression.args["limit"]
    literal_expr = limit_expr.args["expression"]
    assert int(literal_expr.this) == 11


def test_computed_schema_columns() -> None:
    dataset = dlt.dataset(
        dlt.destinations.duckdb(destination_name="duck_db"),
        "pipeline_dataset",
    )

    # missing attribute should raise Attribute error
    with pytest.raises(AttributeError):
        dataset.items

    # missing key should raise KeyError
    with pytest.raises(KeyError):
        dataset["items"]

    with pytest.raises(ValueError):
        dataset.table("items")

    dataset.schema.tables["items"] = new_table(
        "items",
        columns=[{"data_type": "text", "name": "one"}, {"data_type": "json", "name": "two"}],
    )

    # now add columns
    relation = dataset.items

    # computed columns are same as above
    assert relation.columns_schema == {
        "one": {"data_type": "text", "name": "one"},
        "two": {"data_type": "json", "name": "two"},
    }

    # when selecting only one column, computing schema columns will only show that one
    assert relation.select("one").columns_schema == {"one": {"data_type": "text", "name": "one"}}
    assert relation["one"].columns_schema == {"one": {"data_type": "text", "name": "one"}}
    assert relation[["one"]].columns_schema == {"one": {"data_type": "text", "name": "one"}}

    # selecting unknown column fails
    with pytest.raises(KeyError):
        relation[["unknown_columns"]]
    with pytest.raises(KeyError):
        relation["unknown_columns"]


def test_changing_relation_with_query() -> None:
    s = Schema("my_schema")
    t = new_table(
        "something",
        columns=[
            {"name": "this", "data_type": "text"},
            {"name": "that", "data_type": "text"},
        ],
    )

    s.update_table(t)
    dataset = dlt.dataset(
        dlt.destinations.duckdb(destination_name="duck_db"),
        "pipeline_dataset",
        schema=s,
    )

    relation = dataset("SELECT * FROM something")
    query = relation.to_sql()
    assert (
        'SELECT "something"."this" AS "this", "something"."that" AS "that" FROM'
        ' "pipeline_dataset"."something" AS "something"'
        == query
    )

    query = dataset("SELECT this, that FROM something").limit(5).to_sql()
    assert (
        'SELECT "something"."this" AS "this", "something"."that" AS "that" FROM'
        ' "pipeline_dataset"."something" AS "something" LIMIT 5'
        == query
    )

    query = relation.select("this").to_sql()
    assert (
        'SELECT "something"."this" AS "this" FROM "pipeline_dataset"."something" AS "something"'
        == query
    )

    with pytest.raises(LineageFailedException):
        relation.select("hello", "hillo").to_sql()
