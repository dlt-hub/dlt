"""Unit tests for readable db api dataset and relation"""
import dlt
import pytest

from dlt.common.destination.typing import TDatasetType
import dlt.destinations.dataset
from dlt.destinations.dataset.exceptions import (
    ReadableRelationHasQueryException,
    ReadableRelationUnknownColumnException,
)
from dlt.transformations.exceptions import LineageFailedException
from dlt.common.schema.schema import Schema
from dlt.common.schema.utils import new_table


@pytest.mark.parametrize("dataset_type", ("default",))
def test_query_builder(dataset_type: TDatasetType) -> None:
    s = Schema("my_schema")
    t = new_table(
        "my_table",
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "text"},
        ],
    )
    s.update_table(t)

    dataset = dlt.destinations.dataset.dataset(
        dlt.destinations.duckdb(destination_name="duck_db"),
        "pipeline_dataset",
        dataset_type=dataset_type,
        schema=s,
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


@pytest.mark.parametrize("dataset_type", ("default",))
def test_copy_and_chaining(dataset_type: TDatasetType) -> None:
    dataset = dlt.destinations.dataset.dataset(
        dlt.destinations.duckdb(destination_name="duck_db"),
        "pipeline_dataset",
        dataset_type=dataset_type,
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
    assert relation._limit == relation2._limit
    assert relation._table_name == relation2._table_name
    assert relation._provided_query == relation2._provided_query
    assert relation._selected_columns == relation2._selected_columns

    # test copy while chaining limit
    relation3 = relation2.limit(22)
    assert relation2 != relation3
    assert relation2._limit != relation3._limit

    # test last setting prevails chaining
    assert relation.limit(23).limit(67).limit(11)._limit == 11


@pytest.mark.parametrize("dataset_type", ("default",))
def test_computed_schema_columns(dataset_type: TDatasetType) -> None:
    dataset = dlt.destinations.dataset.dataset(
        dlt.destinations.duckdb(destination_name="duck_db"),
        "pipeline_dataset",
        dataset_type=dataset_type,
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


@pytest.mark.parametrize("dataset_type", ("default",))
def test_prevent_changing_relation_with_query(dataset_type: TDatasetType) -> None:
    dataset = dlt.destinations.dataset.dataset(
        dlt.destinations.duckdb(destination_name="duck_db"),
        "pipeline_dataset",
        dataset_type=dataset_type,
    )
    relation = dataset("SELECT * FROM something")

    with pytest.raises(ReadableRelationHasQueryException):
        relation.limit(5)

    with pytest.raises(ReadableRelationHasQueryException):
        relation.head()

    with pytest.raises(ReadableRelationHasQueryException):
        relation.select("hello", "hillo")
