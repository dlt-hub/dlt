import pytest

# this try/except should trigger for python <3.10 because ibis is not supported
try:
    import ibis
except ImportError:
    pytest.skip(allow_module_level=True)

import ibis
import ibis.expr.types as ir
import pandas as pd

import dlt
from dlt.common.libs.ibis import _DltBackend
from dlt.common.libs.pyarrow import pyarrow as pa

from tests.load.test_read_interfaces import populated_pipeline, configs
from tests.utils import (
    preserve_module_environ,
    auto_module_test_storage,
    auto_module_test_run_context,
)


duckdb_conf = [c for c in configs if c.destination_type == "duckdb" and c.file_format is None]


def test_instantiate_backend():
    _DltBackend()


# TODO test for all destinations
@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_connect_to_backend(populated_pipeline: dlt.Pipeline):
    backend = _DltBackend.from_dataset(populated_pipeline.dataset())
    assert isinstance(backend, _DltBackend)


@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_list_tables(populated_pipeline: dlt.Pipeline):
    backend = _DltBackend.from_dataset(populated_pipeline.dataset())
    expected_table_names = [
        "_dlt_version",
        "_dlt_loads",
        "items",
        "double_items",
        "orderable_in_chain",
        "_dlt_pipeline_state",
        "items__children",
    ]

    assert backend.list_tables() == expected_table_names


@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_get_schema(populated_pipeline: dlt.Pipeline):
    backend = _DltBackend.from_dataset(populated_pipeline.dataset())
    expected_schema = ibis.Schema(
        {
            "id": ibis.dtype("int64", nullable=True),
            "decimal": ibis.dtype("decimal", nullable=True),
            "other_decimal": ibis.dtype("decimal", nullable=True),
            "_dlt_load_id": ibis.dtype("string", nullable=False),
            "_dlt_id": ibis.dtype("string", nullable=False),
        }
    )

    ibis_schema = backend.get_schema("items")

    assert expected_schema.equals(ibis_schema)


@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_get_bound_table(populated_pipeline: dlt.Pipeline):
    backend = _DltBackend.from_dataset(populated_pipeline.dataset())
    expected_schema = ibis.Schema(
        {
            "id": ibis.dtype("int64", nullable=True),
            "decimal": ibis.dtype("decimal", nullable=True),
            "other_decimal": ibis.dtype("decimal", nullable=True),
            "_dlt_load_id": ibis.dtype("string", nullable=False),
            "_dlt_id": ibis.dtype("string", nullable=False),
        }
    )

    table = backend.table("items")

    assert isinstance(table, ir.Table)
    assert table.schema().equals(expected_schema)


@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_execute_expression(populated_pipeline: dlt.Pipeline):
    backend = _DltBackend.from_dataset(populated_pipeline.dataset())
    expected_schema = ibis.Schema(
        {
            "_dlt_id": ibis.dtype("string", nullable=False),
            "id": ibis.dtype("int64", nullable=True),
        }
    )

    table = backend.table("items")
    expr = table.select("_dlt_id", "id")
    table2 = backend.execute(expr)

    assert isinstance(table2, pd.DataFrame)
    assert expr.schema().equals(expected_schema)
    assert set(table2.columns) == set(expected_schema.names)


@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_user_workflow(populated_pipeline: dlt.Pipeline):
    expected_columns = ["_dlt_id", "id"]

    dataset = populated_pipeline.dataset()
    # close conn to data source to release duckdb database file
    con = dataset.ibis()
    try:
        table = con.table("items")
        result = table.select("_dlt_id", "id").execute()

        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == set(expected_columns)
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_table_to_pandas(populated_pipeline: dlt.Pipeline):
    expected_columns = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]

    dataset = populated_pipeline.dataset()
    con = dataset.ibis()
    try:
        result = con.table("items").to_pandas()

        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == set(expected_columns)
    finally:
        con.disconnect()


@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_table_to_pyarrow(populated_pipeline: dlt.Pipeline):
    expected_columns = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]

    dataset = populated_pipeline.dataset()
    con = dataset.ibis()
    try:
        result = con.table("items").to_pyarrow()

        assert isinstance(result, pa.Table)
        assert set(result.column_names) == set(expected_columns)
    finally:
        con.disconnect()
