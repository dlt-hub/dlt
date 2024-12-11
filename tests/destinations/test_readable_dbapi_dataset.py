"""Unit tests for readable db api dataset and relation"""
import dlt
import pytest

from dlt.destinations.dataset.exceptions import (
    ReadableRelationHasQueryException,
    ReadableRelationUnknownColumnException,
)


def test_query_builder() -> None:
    dataset = dlt.pipeline(destination="duckdb", pipeline_name="pipeline").dataset()

    # default query for a table
    assert dataset.my_table.query.strip() == 'SELECT  * FROM "pipeline_dataset"."my_table"'

    # head query
    assert (
        dataset.my_table.head().query.strip()
        == 'SELECT  * FROM "pipeline_dataset"."my_table" LIMIT 5'
    )

    # limit query
    assert (
        dataset.my_table.limit(24).query.strip()
        == 'SELECT  * FROM "pipeline_dataset"."my_table" LIMIT 24'
    )

    # select columns
    assert (
        dataset.my_table.select("col1", "col2").query.strip()
        == 'SELECT  "col1","col2" FROM "pipeline_dataset"."my_table"'
    )
    # also indexer notation
    assert (
        dataset.my_table[["col1", "col2"]].query.strip()
        == 'SELECT  "col1","col2" FROM "pipeline_dataset"."my_table"'
    )

    # identifiers are normalized
    assert (
        dataset["MY_TABLE"].select("CoL1", "cOl2").query.strip()
        == 'SELECT  "co_l1","c_ol2" FROM "pipeline_dataset"."my_table"'
    )
    assert (
        dataset["MY__TABLE"].select("Co__L1", "cOl2").query.strip()
        == 'SELECT  "co__l1","c_ol2" FROM "pipeline_dataset"."my__table"'
    )

    # limit and select chained
    assert (
        dataset.my_table.select("col1", "col2").limit(24).query.strip()
        == 'SELECT  "col1","col2" FROM "pipeline_dataset"."my_table" LIMIT 24'
    )


def test_copy_and_chaining() -> None:
    dataset = dlt.pipeline(destination="duckdb", pipeline_name="pipeline").dataset()

    # create releation and set some stuff on it
    relation = dataset.items
    relation = relation.limit(34)
    relation = relation[["one", "two"]]
    relation._schema_columns = {"one": {}, "two": {}}  # type: ignore[attr-defined]

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


def test_computed_schema_columns() -> None:
    dataset = dlt.pipeline(destination="duckdb", pipeline_name="pipeline").dataset()
    relation = dataset.items

    # no schema present
    assert relation.columns_schema is None

    # we can select any columns because it can't be verified
    relation["one", "two"]

    # now add columns
    relation = dataset.items
    dataset.schema.tables["items"] = {
        "columns": {"one": {"data_type": "text"}, "two": {"data_type": "json"}}
    }

    # computed columns are same as above
    assert relation.columns_schema == {"one": {"data_type": "text"}, "two": {"data_type": "json"}}

    # when selecting only one column, computing schema columns will only show that one
    assert relation.select("one").columns_schema == {"one": {"data_type": "text"}}

    # selecting unkonwn column fails
    with pytest.raises(ReadableRelationUnknownColumnException):
        relation["unknown_columns"]


def test_prevent_changing_relation_with_query() -> None:
    dataset = dlt.pipeline(destination="duckdb", pipeline_name="pipeline").dataset()
    relation = dataset("SELECT * FROM something")

    with pytest.raises(ReadableRelationHasQueryException):
        relation.limit(5)

    with pytest.raises(ReadableRelationHasQueryException):
        relation.head()

    with pytest.raises(ReadableRelationHasQueryException):
        relation.select("hello", "hillo")
