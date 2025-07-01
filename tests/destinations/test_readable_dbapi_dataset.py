"""Unit tests for readable db api dataset and relation"""
from typing import cast

import dlt
import pytest

import dlt.destinations.dataset
from dlt.destinations.dataset.exceptions import (
    ReadableRelationUnknownColumnException,
)
from dlt.transformations.exceptions import LineageFailedException
from dlt.common.schema.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.destinations.dataset.dataset import ReadableDBAPIDataset, ReadableDBAPIRelation


def test_query_builder() -> None:
    s = Schema("my_schema")
    t = new_table(
        "my_table",
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "text"},
        ],
    )
    s.update_table(t)

    dataset = cast(
        ReadableDBAPIDataset,
        dlt.destinations.dataset.dataset(
            dlt.destinations.duckdb(destination_name="duck_db"),
            "pipeline_dataset",
            schema=s,
        ),
    )

    # default query for a table
    assert (
        dataset.my_table.query().strip()
        == 'SELECT "my_table"."col1" AS "col1", "my_table"."col2" AS "col2" FROM'
        ' "pipeline_dataset"."my_table" AS "my_table"'
    )

    # head query
    assert (
        dataset.my_table.head().query().strip()
        == 'SELECT "my_table"."col1" AS "col1", "my_table"."col2" AS "col2" FROM'
        ' "pipeline_dataset"."my_table" AS "my_table" LIMIT 5'
    )

    # limit query
    assert (
        dataset.my_table.limit(24).query().strip()
        == 'SELECT "my_table"."col1" AS "col1", "my_table"."col2" AS "col2" FROM'
        ' "pipeline_dataset"."my_table" AS "my_table" LIMIT 24'
    )

    # select columns
    assert (
        dataset.my_table.select("col1").query().strip()
        == 'SELECT "my_table"."col1" AS "col1" FROM "pipeline_dataset"."my_table" AS "my_table"'
    )
    # also indexer notation
    assert (
        dataset.my_table[["col2"]].query().strip()
        == 'SELECT "my_table"."col2" AS "col2" FROM "pipeline_dataset"."my_table" AS "my_table"'
    )

    # limit and select chained
    assert (
        dataset.my_table.select("col1").limit(24).query().strip()
        == 'SELECT "my_table"."col1" AS "col1" FROM "pipeline_dataset"."my_table" AS "my_table"'
        " LIMIT 24"
    )


def test_copy_and_chaining() -> None:
    dataset = cast(
        ReadableDBAPIDataset,
        dlt.destinations.dataset.dataset(
            dlt.destinations.duckdb(destination_name="duck_db"),
            "pipeline_dataset",
        ),
    )

    dataset.schema.tables["items"] = {
        "columns": {
            "one": {"data_type": "text", "name": "one"},
            "two": {"data_type": "json", "name": "two"},
        }
    }

    # create relation and set some stuff on it
    relation = dataset.items
    relation = relation.limit(34)
    relation = relation[["one", "two"]]

    relation2 = relation.__copy__()
    assert relation != relation2
    assert relation._sqlglot_expression == relation2._sqlglot_expression

    # test copy while chaining limit
    relation3 = relation2.limit(22)
    assert relation2 != relation3
    assert relation2._sqlglot_expression != relation3._sqlglot_expression

    # test last setting prevails chaining
    limit_expr = relation.limit(23).limit(67).limit(11)._sqlglot_expression.args["limit"]
    literal_expr = limit_expr.args["expression"]
    assert int(literal_expr.this) == 11


def test_computed_schema_columns() -> None:
    dataset = dlt.destinations.dataset.dataset(
        dlt.destinations.duckdb(destination_name="duck_db"),
        "pipeline_dataset",
    )

    with pytest.raises(ValueError):
        dataset.items

    dataset.schema.tables["items"] = {
        "columns": {
            "one": {"data_type": "text", "name": "one"},
            "two": {"data_type": "json", "name": "two"},
        }
    }

    # now add columns
    relation = dataset.items

    # computed columns are same as above
    assert relation.columns_schema == {
        "one": {"data_type": "text", "name": "one"},
        "two": {"data_type": "json", "name": "two"},
    }

    # when selecting only one column, computing schema columns will only show that one
    assert relation.select("one").columns_schema == {"one": {"data_type": "text", "name": "one"}}

    # selecting unknown column fails

    # TODO: add correct exception
    import sqlglot

    with pytest.raises(LineageFailedException):
        relation[["unknown_columns"]].compute_columns_schema()


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
    dataset = cast(
        ReadableDBAPIDataset,
        dlt.destinations.dataset.dataset(
            dlt.destinations.duckdb(destination_name="duck_db"),
            "pipeline_dataset",
            schema=s,
        ),
    )

    relation = dataset("SELECT * FROM something")
    query = relation.query()
    assert (
        'SELECT "something"."this" AS "this", "something"."that" AS "that" FROM'
        ' "pipeline_dataset"."something" AS "something"'
        == query
    )

    query = dataset("SELECT this, that FROM something").limit(5).query()
    assert (
        'SELECT "something"."this" AS "this", "something"."that" AS "that" FROM'
        ' "pipeline_dataset"."something" AS "something" LIMIT 5'
        == query
    )

    query = relation.select("this").query()
    assert (
        'SELECT "something"."this" AS "this" FROM "pipeline_dataset"."something" AS "something"'
        == query
    )

    with pytest.raises(LineageFailedException):
        relation.select("hello", "hillo").query()


def test_repr_and_str() -> None:
    # dataset not present
    ds_ = dlt.dataset("duckdb", "test_repr_and_str")
    # make sure we do not raise on empty dataset
    assert repr(ds_).startswith("<dlt.dataset(dataset_name='test_repr_and_str'")
    assert str(ds_).startswith("Dataset `test_repr_and_str` at `duckdb")
    assert "Dataset is not available" in str(ds_)

    relation = ds_("SELECT something FROM something")
    with pytest.raises(LineageFailedException):
        # TODO: maybe we should fallback to super() and not raise?
        print(str(relation))

    # materialized dataset, known schema
    pipeline = dlt.pipeline("test_repr_and_str", destination="duckdb", dataset_name="table_data")
    pipeline.run([1, 2, 3], table_name="digits")
    ds_ = pipeline.dataset()
    assert repr(ds_).startswith("<dlt.dataset(dataset_name='table_data'")
    # ends with list of tables
    assert str(ds_).endswith("digits")
    # query (table name not known)
    rel_ = ds_("SELECT * FROM digits")
    assert (
        str(rel_)
        == """Relation query:\n  SELECT * FROM digits\nColumns:\n  value bigint\n  _dlt_load_id text\n  _dlt_id text\n"""
    )
