"""End-to-end tests for `Relation.join()` across destinations."""

import os
from typing import Any, Tuple

import pytest

import dlt
from dlt import Pipeline
from dlt.common.destination import Destination
from dlt.dataset.relation import TJoinType

from tests.dataset.utils import (
    crm,
    inventory,
    relational_tables,
)
from tests.load.read_dataset_fixtures import (
    destination_config,
    preserve_module_environ_per_destination_config,
    skip_if_unsupported_filesystem_format,
)
from tests.load.utils import (
    DestinationTestConfiguration,
    drop_pipeline_data,
)
from tests.utils import (
    auto_module_test_run_context,
    auto_module_test_storage,
    get_test_storage_root,
)


def _skip_unsupported(destination_config: DestinationTestConfiguration) -> None:
    skip_if_unsupported_filesystem_format(destination_config)
    # TODO: remove once https://github.com/dlt-hub/dlt/pull/4011 is merged
    if destination_config.destination_type == "databricks":
        pytest.skip("databricks foreign-key emission breaks this fixture. see dlt-hub/dlt#4011")


@pytest.fixture(scope="module")
def relational_pipeline(
    destination_config: DestinationTestConfiguration,
    auto_module_test_storage: Any,
    preserve_module_environ_per_destination_config: Any,
    auto_module_test_run_context: Any,
) -> Any:
    _skip_unsupported(destination_config)
    pipeline = destination_config.setup_pipeline(
        "join_relational_pipeline", dataset_name="join_relational", dev_mode=True
    )
    pipeline.run(relational_tables(), **destination_config.run_kwargs)
    try:
        yield pipeline
    finally:
        drop_pipeline_data(pipeline)


@pytest.fixture(scope="module")
def crm_pipeline(
    destination_config: DestinationTestConfiguration,
    auto_module_test_storage: Any,
    preserve_module_environ_per_destination_config: Any,
    auto_module_test_run_context: Any,
) -> Any:
    _skip_unsupported(destination_config)
    pipeline = destination_config.setup_pipeline(
        "join_crm_pipeline", dataset_name="join_crm", dev_mode=True
    )
    source = crm(0)
    source.root_key = True
    pipeline.run(source, **destination_config.run_kwargs)
    pipeline.run(inventory(), **destination_config.run_kwargs)
    try:
        yield pipeline
    finally:
        drop_pipeline_data(pipeline)


@pytest.fixture(scope="module")
def cross_dataset_pipelines(
    destination_config: DestinationTestConfiguration,
    auto_module_test_storage: Any,
    preserve_module_environ_per_destination_config: Any,
    auto_module_test_run_context: Any,
) -> Any:
    """Two pipelines on the same physical destination, distinct dataset names."""
    _skip_unsupported(destination_config)
    if destination_config.destination_type in ("filesystem", "lance", "lancedb"):
        pytest.skip(
            "cross-dataset joins are not supported on filesystem destinations"
            " (see dlt/dataset/relation.py:_resolve_join_target)"
        )
    if destination_config.destination_name == "sqlalchemy_sqlite":
        # TODO: remove when we attach foreign datasets in sqlite
        pytest.skip("sqlite cross-dataset joins require ATTACH DATABASE for both datasets")
    destination: Destination[Any, Any]
    if destination_config.destination_type == "duckdb":
        destination_config.setup()
        # explicitly shared path to ensure the two pipelines see each other's datasets
        shared_db = os.path.join(get_test_storage_root(), "cross_ds.duckdb")
        destination = dlt.destinations.duckdb(shared_db)
    else:
        # assume that shared credentials + dataset_name differentiation are enough
        destination = destination_config.destination_factory()

    pipeline_crm = destination_config.setup_pipeline(
        "cross_crm_pipeline",
        dataset_name="cross_crm",
        dev_mode=True,
        destination=destination,
    )
    source = crm(0)
    source.root_key = True
    pipeline_crm.run(source, **destination_config.run_kwargs)

    pipeline_inv = destination_config.setup_pipeline(
        "cross_inv_pipeline",
        dataset_name="cross_inv",
        dev_mode=True,
        destination=destination,
    )
    pipeline_inv.run(inventory(), **destination_config.run_kwargs)

    try:
        yield pipeline_crm, pipeline_inv
    finally:
        drop_pipeline_data(pipeline_crm)
        drop_pipeline_data(pipeline_inv)


@pytest.mark.essential
def test_magic_join_child_to_parent(crm_pipeline: Pipeline) -> None:
    dataset = crm_pipeline.dataset()
    df = dataset.table("users__orders").join("users").df()

    assert df is not None
    assert len(df) == 3
    assert "users__name" in df.columns
    assert sorted(df["users__name"].tolist()) == ["Alice", "Alice", "Bob"]


@pytest.mark.essential
def test_magic_join_multi_hop_to_root_via_root_key(crm_pipeline: Pipeline) -> None:
    dataset = crm_pipeline.dataset()
    df = dataset.table("users__orders__items").join("users").df()

    assert df is not None
    assert len(df) == 4
    assert "users__name" in df.columns
    assert sorted(df["users__name"].tolist()) == ["Alice", "Alice", "Alice", "Bob"]


@pytest.mark.essential
def test_explicit_on_basic(relational_pipeline: Pipeline) -> None:
    dataset = relational_pipeline.dataset()
    df = (
        dataset.table("customers")
        .join("orders", on="customers.customer_id = orders.customer_id")
        .order_by("orders__order_id")
        .df()
    )

    assert df is not None
    assert len(df) == 4
    assert "orders__amount" in df.columns
    assert [float(x) for x in df["orders__amount"]] == [50.0, 75.0, 200.0, 30.0]


@pytest.mark.parametrize(
    "kind,expected_rows",
    [
        pytest.param("inner", 3, id="inner"),
        pytest.param("left", 3, id="left"),
        pytest.param("right", 4, id="right"),
        pytest.param("full", 4, id="full"),
    ],
)
def test_join_kind_matrix(
    relational_pipeline: Pipeline,
    destination_config: DestinationTestConfiguration,
    kind: TJoinType,
    expected_rows: int,
) -> None:
    if kind == "full" and destination_config.destination_name == "sqlalchemy_mysql":
        pytest.skip("MySQL does not support FULL JOIN")
    dataset = relational_pipeline.dataset()
    df = (
        dataset.table("customers")
        .join(
            "countries",
            on="customers.country_code = countries.code",
            kind=kind,
        )
        .df()
    )

    assert df is not None
    assert len(df) == expected_rows


def test_chained_three_table_join(relational_pipeline: Pipeline) -> None:
    dataset = relational_pipeline.dataset()
    df = (
        dataset.table("customers")
        .join("orders", on="customers.customer_id = orders.customer_id")
        .join("countries", on="customers.country_code = countries.code")
        .order_by("orders__order_id")
        .df()
    )

    assert df is not None
    assert len(df) == 4
    assert "name" in df.columns  # left base column
    assert "orders__amount" in df.columns  # first join
    assert "countries__name" in df.columns  # second join
    assert [float(x) for x in df["orders__amount"]] == [50.0, 75.0, 200.0, 30.0]
    assert list(df["countries__name"]) == ["Germany", "Germany", "France", "Germany"]


def test_join_with_filtered_lhs(relational_pipeline: Pipeline) -> None:
    dataset = relational_pipeline.dataset()
    df = (
        dataset.table("customers")
        .where("country_code", "eq", "DE")
        .join("orders", on="customers.customer_id = orders.customer_id")
        .order_by("orders__order_id")
        .df()
    )

    assert df is not None
    assert len(df) == 3
    assert list(df["name"]) == ["Alice", "Alice", "Charlie"]
    assert [float(x) for x in df["orders__amount"]] == [50.0, 75.0, 30.0]


def test_join_alias_prefix_in_output_columns(relational_pipeline: Pipeline) -> None:
    dataset = relational_pipeline.dataset()
    joined = dataset.table("customers").join(
        "orders", on="customers.customer_id = orders.customer_id", alias="o"
    )
    df = joined.df()

    assert df is not None
    o_cols = {c for c in df.columns if c.startswith("o__")}
    assert o_cols, f"no `o__`-prefixed columns in {list(df.columns)}"

    expected = {f"o__{col}" for col in dataset.schema.tables["orders"]["columns"].keys()}
    assert o_cols == expected
    # the default prefix (table name) must not appear when `alias=` overrides it
    assert not any(c.startswith("orders__") for c in df.columns)


def test_cross_dataset_explicit_join(
    cross_dataset_pipelines: Tuple[Pipeline, Pipeline],
) -> None:
    pipeline_a, pipeline_b = cross_dataset_pipelines
    ds_a = pipeline_a.dataset()
    ds_b = pipeline_b.dataset()

    joined = ds_a.table("users").join(ds_b.table("purchases"), on="users.id = purchases.user_id")

    sql = joined.to_sql()
    # the binder emits the physical (normalized) dataset name, casefolded per the
    # destination (e.g. Snowflake uppercases), not the logical `dataset_name`
    casefold = ds_a.sql_client.capabilities.casefold_identifier
    assert casefold(ds_a.sql_client.dataset_name) in sql, sql
    assert casefold(ds_b.sql_client.dataset_name) in sql, sql

    df = joined.order_by("purchases__purchase_id").df()
    assert df is not None
    # orphan user_id=99 dropped by INNER
    assert len(df) == 3
    assert "purchases__sku" in df.columns
    assert "name" in df.columns
    assert list(df["name"]) == ["Alice", "Alice", "Bob"]
    assert list(df["purchases__sku"]) == ["W-001", "G-001", "W-001"]
