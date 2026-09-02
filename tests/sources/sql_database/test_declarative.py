"""Tests of the declarative SQL database source: `sql_database_source` and `sql_database_resources`.

The declarative layer only compiles a config into `sql_table` resources, so the tests compare
it against the equivalent imperative code instead of re-testing table loading. They run against
a local sqlite database so they need no external services.
"""

import os
import uuid
from pathlib import Path
from typing import Any, Dict

import pytest
import sqlalchemy as sa

import dlt
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
)
from dlt.sources.sql_database.typing import SqlTableResource

from tests.utils import get_test_storage_root


@pytest.fixture
def credentials() -> str:
    """Creates an on-disk sqlite db with two tables and returns its connection url."""
    test_dir = Path(get_test_storage_root()) / f"sqlite_{uuid.uuid4().hex}"
    test_dir.mkdir(parents=True, exist_ok=True)
    engine = sa.create_engine(f"sqlite:///{test_dir / 'test.db'}")
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, secret TEXT,"
                    " updated_at TEXT)"
                )
            )
            conn.execute(
                sa.text(
                    "INSERT INTO items VALUES (1, 'a', 'x', '2024-01-01'), (2, 'b', 'y',"
                    " '2024-06-01')"
                )
            )
            conn.execute(sa.text("CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER)"))
            conn.execute(sa.text("INSERT INTO orders VALUES (1, 1), (2, 2), (3, 2)"))
    finally:
        engine.dispose()
    return f"sqlite:///{test_dir / 'test.db'}"


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
    resources = sql_database_resources(
        {"credentials": credentials, "tables": ["orders", recent_items]}
    )

    assert_same_resource(resources[0], sql_table(credentials=credentials, table="orders"))
    assert_same_resource(
        resources[1],
        sql_table(
            credentials=credentials,
            table="items",
            excluded_columns=["secret"],
            write_disposition="merge",
            primary_key="id",
            incremental=dlt.sources.incremental(**incremental),
        )
        .with_name("recent_items")
        .apply_hints(table_name="items_dest"),
    )


def test_table_defaults_are_overridden_by_table(credentials: str) -> None:
    """`table_defaults` apply to all tables and are overridden by the table's own settings."""
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

    assert_same_resource(
        resources[0],
        sql_table(
            credentials=credentials,
            table="items",
            write_disposition="replace",
            excluded_columns=["secret"],
        ),
    )
    assert_same_resource(
        resources[1],
        sql_table(
            credentials=credentials,
            table="orders",
            write_disposition="append",
            excluded_columns=["secret"],
        ),
    )


def test_name_and_table_default_to_each_other(credentials: str) -> None:
    """`name` and `table` each default to the other so only one of them is required."""
    resources = sql_database_resources(
        {"credentials": credentials, "tables": [{"name": "items"}, {"table": "orders"}]}
    )

    assert [resource.name for resource in resources] == ["items", "orders"]
    assert [resource.compute_table_schema()["name"] for resource in resources] == [
        "items",
        "orders",
    ]


def test_discovered_tables_equal_sql_database(credentials: str) -> None:
    """Tables are discovered when none are declared, like in `sql_database`, and inherit
    `table_defaults`. An empty list declares no tables."""
    expected = sql_database(credentials=credentials)
    resources = sql_database_resources({"credentials": credentials})

    assert [resource.name for resource in resources] == list(expected.resources.keys())
    for resource in resources:
        assert_same_resource(resource, expected.resources[resource.name])

    # discovered tables inherit `table_defaults`
    with_defaults = sql_database_resources(
        {"credentials": credentials, "table_defaults": {"write_disposition": "replace"}}
    )
    assert all(resource.write_disposition == "replace" for resource in with_defaults)

    assert sql_database_resources({"credentials": credentials, "tables": []}) == []


def test_resource_settings(credentials: str) -> None:
    """Settings of the resource itself are applied to the created resource."""
    resources = sql_database_resources(
        {
            "credentials": credentials,
            "tables": [
                {"name": "items", "selected": False, "max_table_nesting": 1},
                {"name": "orders", "parallelized": True},
            ],
        }
    )

    assert resources[0].selected is False
    assert resources[0].max_table_nesting == 1
    # the parallelized resource is wrapped and yields the same data
    assert list(resources[1]) == list(sql_table(credentials=credentials, table="orders"))


def test_ready_resources_are_passed_through(credentials: str) -> None:
    """`tables` accepts ready resources ie. from `sql_table`, keeping the declared order."""
    orders = sql_table(credentials=credentials, table="orders")
    resources = sql_database_resources({"credentials": credentials, "tables": [orders, "items"]})

    assert [resource.name for resource in resources] == ["orders", "items"]
    assert resources[0] is orders


def test_source_is_configured_like_sql_database(credentials: str) -> None:
    """The source shares name, section and config sections with `sql_database` so credentials
    and other arguments resolve from the same places."""
    os.environ["SOURCES__SQL_DATABASE__CREDENTIALS"] = credentials
    try:
        source = sql_database_source({"tables": ["items"]})
    finally:
        del os.environ["SOURCES__SQL_DATABASE__CREDENTIALS"]

    assert source.name == "sql_database"
    assert source.section == "sql_database"
    assert_same_resource(
        source.resources["items"], sql_table(credentials=credentials, table="items")
    )


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
    ),
)
def test_invalid_config(config: Dict[str, Any], error: str) -> None:
    with pytest.raises(DictValidationException, match=error):
        sql_database_source(config)  # type: ignore[arg-type]


def test_credentials_are_not_included_in_validation_errors() -> None:
    with pytest.raises(DictValidationException) as exc_info:
        sql_database_source({"credentials": 1234, "tables": ["items"]})  # type: ignore[typeddict-item]

    assert "1234" not in str(exc_info.value)
    assert "credentials" in str(exc_info.value)


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


def test_every_table_setting_is_routed() -> None:
    """Each field of `SqlTableResource` is either passed to `sql_table`, applied as a table hint
    or applied to the resource. A new field must be routed to one of them.
    """
    routed = SQL_TABLE_ARGS | TABLE_HINT_ARGS | RESOURCE_ARGS | {"name"}
    assert set(get_type_hints(SqlTableResource)) == routed
