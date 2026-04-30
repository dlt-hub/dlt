from typing import cast

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

from tests.load.lance_utils import (
    module_lance_rest_server,  # consumed by `populated_pipeline` fixture
)
from tests.load.test_read_interfaces import (
    populated_pipeline,
    preserve_module_environ_per_destination_config,
)
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.utils import (
    auto_module_test_run_context,
    auto_module_test_storage,
)


# NOTE: this fixture is consumed by `populated_pipeline`, and overrides the `destination_config`
# fixture from `test_read_interfaces.py` to limit to `duckdb` destination for this module
@pytest.fixture(
    scope="module",
    params=destinations_configs(
        default_sql_configs=True,
        subset=["duckdb"],
        file_format=None,
    ),
    ids=lambda x: x.name,
)
def destination_config(request: pytest.FixtureRequest) -> DestinationTestConfiguration:
    return cast(DestinationTestConfiguration, request.param)


def test_instantiate_backend():
    _DltBackend()


def test_connect_to_backend(populated_pipeline: dlt.Pipeline):
    backend = _DltBackend.from_dataset(populated_pipeline.dataset())
    assert isinstance(backend, _DltBackend)


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


def test_get_schema(populated_pipeline: dlt.Pipeline):
    backend = _DltBackend.from_dataset(populated_pipeline.dataset())
    expected_schema = ibis.Schema(
        {
            "id": ibis.dtype("int64", nullable=True),
            "decimal": ibis.dtype("decimal", nullable=True),
            "other_decimal": ibis.dtype("decimal", nullable=True),
            "created_at": ibis.dtype("timestamp", nullable=True),
            "_dlt_load_id": ibis.dtype("string", nullable=False),
            "_dlt_id": ibis.dtype("string", nullable=False),
        }
    )

    ibis_schema = backend.get_schema("items")

    assert expected_schema.equals(ibis_schema)


def test_get_bound_table(populated_pipeline: dlt.Pipeline):
    backend = _DltBackend.from_dataset(populated_pipeline.dataset())
    expected_schema = ibis.Schema(
        {
            "id": ibis.dtype("int64", nullable=True),
            "decimal": ibis.dtype("decimal", nullable=True),
            "other_decimal": ibis.dtype("decimal", nullable=True),
            "created_at": ibis.dtype("timestamp", nullable=True),
            "_dlt_load_id": ibis.dtype("string", nullable=False),
            "_dlt_id": ibis.dtype("string", nullable=False),
        }
    )

    table = backend.table("items")

    assert isinstance(table, ir.Table)
    assert table.schema().equals(expected_schema)


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


def test_table_to_pandas(populated_pipeline: dlt.Pipeline):
    expected_columns = ["id", "decimal", "other_decimal", "created_at", "_dlt_load_id", "_dlt_id"]

    dataset = populated_pipeline.dataset()
    con = dataset.ibis()
    try:
        result = con.table("items").to_pandas()

        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == set(expected_columns)
    finally:
        con.disconnect()


def test_table_to_pyarrow(populated_pipeline: dlt.Pipeline):
    expected_columns = ["id", "decimal", "other_decimal", "created_at", "_dlt_load_id", "_dlt_id"]

    dataset = populated_pipeline.dataset()
    con = dataset.ibis()
    try:
        result = con.table("items").to_pyarrow()

        assert isinstance(result, pa.Table)
        assert set(result.column_names) == set(expected_columns)
    finally:
        con.disconnect()
