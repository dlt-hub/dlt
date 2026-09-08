"""Tests of the declarative SQL database source: `sql_database_source` and `sql_database_resources`.

The declarative layer only compiles a config into `sql_table` resources, so the tests compare
it against the equivalent imperative code instead of re-testing table loading. They run against
a local sqlite database so they need no external services.
"""

import copy
import os
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import sqlalchemy as sa

import dlt
import dlt.sources.sql_database as sql_database_module
from dlt.common.schema.typing import TMergeDispositionDict, TWriteDisposition
from dlt.common.exceptions import DictValidationException
from dlt.common.incremental.typing import IncrementalArgs
from dlt.common.typing import get_type_hints
from dlt.extract import DltResource
from dlt.sources.sql_database import (
    BaseTableLoader,
    sql_database,
    sql_database_resources,
    sql_database_source,
    sql_table,
)
from dlt.sources.sql_database.config_setup import (
    RESOURCE_ARGS,
    SQL_TABLE_ARGS,
    TABLE_HINT_ARGS,
    merge_table_defaults,
    split_table_config,
)
from dlt.sources.sql_database.typing import (
    SqlDatabaseConfig,
    SqlTableResource,
    SqlTableResourceBase,
)

from tests.common.configuration.utils import toml_providers
from tests.pipeline.utils import assert_load_info, load_table_counts
from tests.utils import get_test_storage_root


def create_sqlite_db(items: str, orders: str) -> str:
    """Creates an on-disk sqlite db with an `items` and an `orders` table filled with the given
    rows and returns its connection url."""
    test_dir = Path(get_test_storage_root()) / f"sqlite_{uuid.uuid4().hex}"
    test_dir.mkdir(parents=True, exist_ok=True)
    credentials = f"sqlite:///{test_dir / 'test.db'}"
    engine = sa.create_engine(credentials)
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, secret TEXT,"
                    " updated_at TEXT)"
                )
            )
            conn.execute(sa.text(f"INSERT INTO items VALUES {items}"))
            conn.execute(sa.text("CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER)"))
            conn.execute(sa.text(f"INSERT INTO orders VALUES {orders}"))
    finally:
        engine.dispose()
    return credentials


@pytest.fixture
def credentials() -> str:
    return create_sqlite_db(
        items="(1, 'a', 'x', '2024-01-01'), (2, 'b', 'y', '2024-06-01')",
        orders="(1, 1), (2, 2), (3, 2)",
    )


def assert_same_resource(declared: DltResource, expected: DltResource) -> None:
    """Asserts both resources are named the same and compute the same table schema and data."""
    assert declared.name == expected.name
    assert declared.compute_table_schema() == expected.compute_table_schema()
    assert list(declared) == list(expected)


def test_declared_tables_equal_sql_table_resources(credentials: str) -> None:
    """A table config compiles to the `sql_table` resource with the same arguments."""
    incremental: IncrementalArgs = {
        "cursor_path": "updated_at",
        "initial_value": "2024-03-01",
    }
    recent_items: SqlTableResource = {
        "name": "recent_items",
        "table": "items",
        "table_name": "items_dest",
        "excluded_columns": ["secret"],
        "write_disposition": "merge",
        "primary_key": "id",
        "incremental": incremental,
    }
    expected_orders_resource = sql_table(credentials=credentials, table="orders")
    expected_items_resource = (
        sql_table(
            credentials=credentials,
            table="items",
            excluded_columns=["secret"],
            write_disposition="merge",
            primary_key="id",
            incremental=dlt.sources.incremental(**incremental),
        )
        .with_name("recent_items")
        .apply_hints(table_name="items_dest")
    )

    resources = sql_database_resources(
        {"credentials": credentials, "tables": ["orders", recent_items]}
    )

    assert_same_resource(resources[0], expected_orders_resource)
    assert_same_resource(resources[1], expected_items_resource)


def test_table_defaults_are_overridden_by_table(credentials: str) -> None:
    """`table_defaults` apply to all tables and are overridden by the table's own settings."""
    expected_items_resource = sql_table(
        credentials=credentials,
        table="items",
        write_disposition="replace",
        excluded_columns=["secret"],
    )
    expected_orders_resource = sql_table(
        credentials=credentials,
        table="orders",
        write_disposition="append",
        excluded_columns=["secret"],
    )

    resources = sql_database_resources(
        {
            "credentials": credentials,
            "table_defaults": {
                "write_disposition": "replace",
                "excluded_columns": ["secret"],
            },
            "tables": ["items", {"name": "orders", "write_disposition": "append"}],
        }
    )

    assert_same_resource(resources[0], expected_items_resource)
    assert_same_resource(resources[1], expected_orders_resource)


def test_name_and_table_default_to_each_other(credentials: str) -> None:
    """`name` and `table` each default to the other so only one of them is required."""
    expected_names = ["items", "orders"]

    resources = sql_database_resources(
        {"credentials": credentials, "tables": [{"name": "items"}, {"table": "orders"}]}
    )
    resource_names = [resource.name for resource in resources]
    table_schema_names = [resource.compute_table_schema()["name"] for resource in resources]

    assert resource_names == expected_names
    assert table_schema_names == expected_names


def test_discovered_tables_equal_sql_database(credentials: str) -> None:
    """Tables are discovered when none are declared, like in `sql_database`, and inherit
    `table_defaults`. An empty list declares no tables."""
    expected_source = sql_database(credentials=credentials)
    expected_names = list(expected_source.resources.keys())

    resources = sql_database_resources({"credentials": credentials})
    resource_names = [resource.name for resource in resources]

    assert resource_names == expected_names
    for resource in resources:
        assert_same_resource(resource, expected_source.resources[resource.name])


def test_discovered_tables_inherit_table_defaults(credentials: str) -> None:
    default_write_disposition: TWriteDisposition = "replace"
    table_defaults: SqlTableResourceBase = {"write_disposition": default_write_disposition}
    config: SqlDatabaseConfig = {
        "credentials": credentials,
        "table_defaults": table_defaults,
    }

    with_defaults = sql_database_resources(config)

    assert all(
        resource.write_disposition == default_write_disposition for resource in with_defaults
    )


def test_sql_database_source_empty_table_list_produces_no_resources(credentials: str) -> None:
    empty_resources = sql_database_resources({"credentials": credentials, "tables": []})
    assert empty_resources == []


def test_sql_database_resources_applies_resource_settings(credentials: str) -> None:
    """Settings of the resource itself are applied to the created resource."""
    expected_orders_data = list(sql_table(credentials=credentials, table="orders"))

    resources = sql_database_resources(
        {
            "credentials": credentials,
            "tables": [
                {"name": "items", "selected": False, "max_table_nesting": 1},
                {"name": "orders", "parallelized": True},
            ],
        }
    )
    # the parallelized resource is wrapped and yields the same data
    orders_data = list(resources[1])

    assert resources[0].selected is False
    assert resources[0].max_table_nesting == 1
    assert orders_data == expected_orders_data


def test_sql_database_resources_can_accept_sql_table_resource(credentials: str) -> None:
    """`tables` can accept instances of `sql_table`"""
    expected_orders_resource = sql_table(credentials=credentials, table="orders")
    expected_names = ["orders", "items"]

    resources = sql_database_resources(
        {"credentials": credentials, "tables": [expected_orders_resource, "items"]}
    )
    resource_names = [resource.name for resource in resources]

    assert resource_names == expected_names
    # order is preserved
    assert resources[0] is expected_orders_resource


def test_engine_kwargs_reach_create_engine(credentials: str) -> None:
    """`engine_kwargs` are forwarded to `sqlalchemy.create_engine()`, which validates them."""
    with pytest.raises(TypeError):
        sql_database_resources(
            {
                "credentials": credentials,
                "engine_kwargs": {"this_is_an_invalid_argument_name": True},
                "tables": ["items"],
            }
        )


def test_engine_adapter_callback_replaces_shared_engine(credentials: str) -> None:
    """The callback is called once and the engine it returns is used by all tables."""
    expected_items_ids = [3]
    expected_orders_ids = [4]
    other_credentials = create_sqlite_db(items="(3, 'c', 'z', '2025-01-01')", orders="(4, 3)")
    adapted: list[sa.engine.Engine] = []

    def engine_adapter_callback(engine: sa.engine.Engine) -> sa.engine.Engine:
        """Mock callback that catches the passed value"""
        adapted.append(engine)
        return sa.create_engine(other_credentials)

    resources = sql_database_resources(
        {
            "credentials": credentials,
            "engine_adapter_callback": engine_adapter_callback,
            "tables": ["items", "orders"],
        }
    )
    items_ids = [row["id"] for row in resources[0]]
    orders_ids = [row["id"] for row in resources[1]]

    assert len(adapted) == 1
    assert items_ids == expected_items_ids
    assert orders_ids == expected_orders_ids


def test_source_is_configured_like_sql_database(credentials: str) -> None:
    """The source shares name, section and config sections with `sql_database` so credentials
    and other arguments resolve from the same places."""
    expected_name = "sql_database"
    expected_section = "sql_database"
    expected_items_resource = sql_table(credentials=credentials, table="items")

    os.environ["SOURCES__SQL_DATABASE__CREDENTIALS"] = credentials
    try:
        source = sql_database_source({"tables": ["items"]})
    finally:
        del os.environ["SOURCES__SQL_DATABASE__CREDENTIALS"]

    assert source.name == expected_name
    assert source.section == expected_section
    assert_same_resource(source.resources["items"], expected_items_resource)


def test_resources_require_credentials() -> None:
    with pytest.raises(ValueError, match="sql_database_source"):
        sql_database_resources({"tables": ["items"]})


@pytest.mark.parametrize(
    "config,error",
    (
        ({"tables": ["items"], "unknown": 1}, "unexpected fields"),
        ({"tables": [{"name": "items", "unknown": 1}]}, "unexpected fields"),
        ({"tables": [{"write_disposition": "replace"}]}, "must define `name`"),
        ({"tables": [{"name": "items", "backend": "polars"}]}, "is not one of"),
        ({"tables": [{"name": "items", "chunk_size": "100"}]}, "invalid type"),
        ({"tables": [{"name": "items", "table_loader_class": object}]}, "expects a subclass"),
        ({"table_defaults": {"name": "items"}, "tables": []}, "unexpected fields"),
        (
            {"tables": [{"name": "items", "incremental": {"unknown_key": 1}}]},
            "unexpected fields",
        ),
    ),
)
def test_invalid_config(config: dict[str, Any], error: str) -> None:
    with pytest.raises(DictValidationException, match=error):
        sql_database_source(config)  # type: ignore[arg-type]


def test_credentials_are_excluded_from_validation_errors() -> None:
    with pytest.raises(DictValidationException) as exc_info:
        sql_database_source({"credentials": 1234, "tables": ["items"]})  # type: ignore[typeddict-item]

    error_message = str(exc_info.value)

    assert "1234" not in error_message
    assert "credentials" in error_message


def test_table_loader_class_is_accepted(credentials: str) -> None:
    class MyLoader(BaseTableLoader):
        pass

    # config is valid, the loader is only instantiated when data is extracted
    sql_database_source(
        {
            "credentials": credentials,
            "tables": [{"name": "items", "table_loader_class": MyLoader}],
        }
    )


def test_config_class_covers_all_arguments_of_sql_table() -> None:
    """Each field of `SqlTableResource` is either passed to `sql_table`, applied as a table hint
    or applied to the resource. A new field must be routed to one of them.
    """
    expected_routed_fields = SQL_TABLE_ARGS | TABLE_HINT_ARGS | RESOURCE_ARGS | {"name"}
    routed_fields = set(get_type_hints(SqlTableResource))
    assert routed_fields == expected_routed_fields


def test_merge_table_defaults_is_shallow() -> None:
    """Unlike `rest_api`'s endpoint merge, `table_defaults` are merged shallowly: a dict or list
    value on the table fully replaces the corresponding default instead of being merged into it.
    """
    expected_backend_kwargs = {"b": 2}
    expected_excluded_columns = ["other"]

    merged = merge_table_defaults(
        {"backend_kwargs": {"a": 1}, "excluded_columns": ["secret"]},
        SqlTableResource(name="items", backend_kwargs={"b": 2}, excluded_columns=["other"]),
    )

    assert merged.get("backend_kwargs") == expected_backend_kwargs
    assert merged.get("excluded_columns") == expected_excluded_columns


def test_explicit_none_argument_overrides_table_defaults() -> None:
    """Explicit `None` argument overrides `table_defaults`"""
    table_defaults: SqlTableResource = {"write_disposition": "replace"}
    table: SqlTableResource = {"name": "items", "write_disposition": None}

    merged = merge_table_defaults(table_defaults=table_defaults, table=table)
    table_args, _, _ = split_table_config(merged)

    assert merged["write_disposition"] is None
    assert table_args["write_disposition"] is None


def test_split_table_config_produces_tuple_without_overlap() -> None:
    expected_table_args = {"table": "items", "chunk_size": 100}
    expected_hints = {"table_name": "items_dest"}
    expected_resource_args = {"selected": False}
    table_config = SqlTableResource(
        name="items",
        table="items",
        chunk_size=100,
        table_name="items_dest",
        selected=False,
    )

    table_args, hints, resource_args = split_table_config(table_config)

    assert table_args == expected_table_args
    assert hints == expected_hints
    assert resource_args == expected_resource_args


def test_sql_table_called_with_expected_kwargs(credentials: str) -> None:
    """Check how `sql_database_resources()` calls `sql_table()` by mocking the object

    `sql_table` is called with the compiled table args and the instantiate sqlalchemy engine,
    not the raw credentials, and with no `metadata` when tables are declared.
    """
    incremental: IncrementalArgs = {"cursor_path": "updated_at", "initial_value": "2024-03-01"}
    expected_table = "items"
    expected_chunk_size = 100

    with patch.object(sql_database_module, "sql_table", wraps=sql_table) as mock_sql_table:
        sql_database_resources(
            {
                "credentials": credentials,
                "tables": [
                    {
                        "name": "recent_items",
                        "table": "items",
                        "chunk_size": 100,
                        "incremental": incremental,
                    }
                ],
            }
        )

    _, kwargs = mock_sql_table.call_args

    assert mock_sql_table.call_count == 1
    assert kwargs["table"] == expected_table
    assert kwargs["chunk_size"] == expected_chunk_size
    assert kwargs["metadata"] is None
    # the shared engine is forwarded, not the raw connection string
    assert isinstance(kwargs["credentials"], sa.engine.Engine)
    assert kwargs["incremental"].cursor_path == incremental["cursor_path"]
    assert kwargs["incremental"].initial_value == incremental["initial_value"]


def test_apply_hints_called_with_expected_kwargs(credentials: str) -> None:
    """Check how `sql_database_resources()` uses `.apply_hints()`

    Table hints are applied via a single `apply_hints()` call with exactly the hint fields
    computed by `split_table_config`, mirroring how `rest_api_resources` hints reach
    `dlt.resource()`.
    """
    expected_kwargs = {"table_name": "items_dest"}

    with patch.object(
        DltResource, "apply_hints", autospec=True, side_effect=DltResource.apply_hints
    ) as mock_apply_hints:
        sql_database_resources(
            {
                "credentials": credentials,
                "tables": [{"name": "items", "table_name": "items_dest"}],
            }
        )
    _, kwargs = mock_apply_hints.call_args

    assert mock_apply_hints.call_count == 1
    assert kwargs == expected_kwargs


@pytest.mark.parametrize("include_views", (False, True))
def test_include_views_matches_sql_database(credentials: str, include_views: bool) -> None:
    """Discovery includes/excludes views exactly like `sql_database(include_views=...)`; this
    only checks the `include_views` flag is forwarded, not view reflection itself.
    """
    engine = sa.create_engine(credentials)
    try:
        with engine.begin() as conn:
            conn.execute(sa.text("CREATE VIEW items_view AS SELECT id, name FROM items"))
    finally:
        engine.dispose()

    # the `expected_source` and `expected_names` need to be evaluated after the view is created.
    expected_source = sql_database(credentials=credentials, include_views=include_views)
    expected_names = list(expected_source.resources.keys())

    resources = sql_database_resources({"credentials": credentials, "include_views": include_views})
    resource_names = [resource.name for resource in resources]

    assert resource_names == expected_names


def test_engine_credentials_equal_sql_table(credentials: str) -> None:
    """`credentials` may be a live `Engine` instance, forwarded to `sql_table` like it would be
    in the imperative API."""
    engine = sa.create_engine(credentials)
    try:
        expected_items_resource = sql_table(credentials=engine, table="items")

        resources = sql_database_resources({"credentials": engine, "tables": ["items"]})

        assert_same_resource(resources[0], expected_items_resource)
    finally:
        engine.dispose()


def test_complex_write_disposition_passes_through(credentials: str) -> None:
    """Non-primitive hint values (ie. a merge/scd2 write disposition) reach the resource intact,
    like the plain string form already covered elsewhere.
    """
    write_disposition: TMergeDispositionDict = {"disposition": "merge", "strategy": "scd2"}
    expected_items_resource = sql_table(
        credentials=credentials,
        table="items",
        write_disposition=write_disposition,
        primary_key="id",
    )

    resources = sql_database_resources(
        {
            "credentials": credentials,
            "tables": [
                {"name": "items", "write_disposition": write_disposition, "primary_key": "id"}
            ],
        }
    )

    assert_same_resource(resources[0], expected_items_resource)


def test_config_resolved_entirely_from_providers(toml_providers: Any, credentials: str) -> None:
    """Not just `credentials`, but the whole config (ie. `tables`) is resolvable from dlt config
    providers, the same way `rest_api_source` resolves its whole config from `dlt.config`.
    """
    expected_items_resource = sql_table(credentials=credentials, table="items")

    dlt.secrets["sources.sql_database.credentials"] = credentials
    dlt.config["sources.sql_database.tables"] = ["items"]
    # NOTE passing an empty `dict` is a bit odd, but matches `rest_api_source()`
    source = sql_database_source({})

    assert_same_resource(source.resources["items"], expected_items_resource)


def test_input_config_is_not_mutated(credentials: str) -> None:
    """Compiling the config must not mutate the caller's dict, the same guarantee
    `rest_api_source` gives (`test_configurations_dict_is_not_modified_in_place`)."""
    config: SqlDatabaseConfig = {
        "credentials": credentials,
        "table_defaults": {"write_disposition": "replace"},
        "tables": ["orders", {"name": "items", "excluded_columns": ["secret"]}],
    }
    expected_config = copy.deepcopy(config)

    sql_database_resources(config)

    assert config == expected_config


def test_table_defaults_dont_apply_to_ready_resource(credentials: str) -> None:
    """`table_defaults` only apply to declared tables, not to ready resources passed in `tables`,
    mirroring `test_resource_defaults_dont_apply_to_DltResource` for `rest_api_source`."""
    default_write_disposition: TWriteDisposition = "replace"
    expected_orders_resource = sql_table(credentials=credentials, table="orders")
    table_defaults: SqlTableResourceBase = {"write_disposition": default_write_disposition}
    config: SqlDatabaseConfig = {
        "credentials": credentials,
        "table_defaults": table_defaults,
        "tables": [expected_orders_resource, "items"],
    }

    resources = sql_database_resources(config)

    orders_write_disposition = resources[0].write_disposition
    items_write_disposition = resources[1].write_disposition

    assert resources[0] is expected_orders_resource
    assert orders_write_disposition != default_write_disposition
    assert items_write_disposition == default_write_disposition


def test_sql_database_source_runs_in_pipeline(credentials: str) -> None:
    """A single smoke test proving the `sql_database_source()` wiring (name/section/
    schema_contract/parallelized forwarded via `.clone()`) produces a source `pipeline.run()`
    accepts. This intentionally does not assert row-level data correctness, which is `sql_table`'s
    responsibility and is already tested elsewhere.
    """
    expected_table_counts = {"items": 2, "orders": 3}

    source = sql_database_source(
        {"credentials": credentials, "tables": ["items", "orders"]},
        schema_contract="evolve",
        parallelized=True,
    )
    pipeline = dlt.pipeline(
        pipeline_name="sql_database_declarative",
        destination="duckdb",
        dataset_name="sql_database_data",
        dev_mode=True,
    )

    load_info = pipeline.run(source)
    table_counts = load_table_counts(pipeline, "items", "orders")

    assert_load_info(load_info)
    assert table_counts == expected_table_counts
