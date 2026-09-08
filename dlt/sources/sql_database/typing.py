"""Typed configuration for the declarative SQL database source.

The config is a plain dictionary so it can be written in Python, TOML, YAML or generated
by an agent. Keys mirror the arguments of `sql_table` and the table hints of `dlt.resource`
so there's a single set of names to learn. Callables and class arguments (adapters, custom
table loaders, `Engine` instances) are accepted as escape hatches but keep the config
non-serializable, so use them only when needed.
"""

from collections.abc import Callable
from typing import Any

from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.incremental.typing import IncrementalArgs
from dlt.common.libs.sql_alchemy import Engine
from dlt.common.typing import TypedDict
from dlt.extract.hints import TResourceHintsBase
from dlt.extract.resource import DltResource

from .helpers import BaseTableLoader, TableBackend, TQueryAdapter, TTableAdapter
from .schema_types import ReflectionLevel, TTypeAdapter


class SqlTableResourceBase(TResourceHintsBase, total=False):
    """Settings of a single table that may also be used as `table_defaults` for all tables.

    Table hints (`write_disposition`, `primary_key`, `merge_key`, `columns`, `table_name`,
    `references`, ...) are inherited from `TResourceHintsBase` and behave like the
    corresponding `dlt.resource` arguments.
    """

    schema: str | None
    """Name of the database schema the table belongs to. Defaults to the connection's default schema."""
    incremental: IncrementalArgs | None
    """Incremental loading settings ie. `{"cursor_path": "updated_at", "initial_value": "2024-01-01"}`."""
    chunk_size: int | None
    backend: TableBackend | None
    backend_kwargs: dict[str, Any] | None
    reflection_level: ReflectionLevel | None
    defer_table_reflect: bool | None
    included_columns: list[str] | None
    excluded_columns: list[str] | None
    resolve_foreign_keys: bool | None
    max_table_nesting: int | None
    selected: bool | None
    parallelized: bool | None
    table_adapter_callback: TTableAdapter | None
    type_adapter_callback: TTypeAdapter | None
    query_adapter_callback: TQueryAdapter | None
    table_loader_class: type[BaseTableLoader] | None


class SqlTableResource(SqlTableResourceBase, total=False):
    """Settings of a single table. At least one of `name` or `table` must be present."""

    name: str | None
    """Name of the dlt resource. Defaults to `table`."""
    table: str | None
    """Name of the table (or view) in the database. Defaults to `name`."""


class SqlDatabaseConfig(TypedDict, total=False):
    tables: list[str | SqlTableResource | DltResource] | None
    """Tables to load: a table name, a table config or a ready `DltResource` ie. from `sql_table`.
    When omitted, all tables in the database schema are discovered and loaded with
    `table_defaults`, like in `sql_database`. An empty list loads no tables."""
    include_views: bool | None
    """Discover views as well as tables. Declared views are always loaded."""
    credentials: ConnectionStringCredentials | Engine | str | None
    """Database credentials or an `Engine` instance. `sql_database_source` resolves this from
    dlt config providers (ie. `secrets.toml`) when omitted."""
    engine_kwargs: dict[str, Any] | None
    """Keyword arguments passed to `sqlalchemy.create_engine()`."""
    engine_adapter_callback: Callable[[Engine], Engine] | None
    """Callback to configure or replace the `Engine` shared by all tables."""
    table_defaults: SqlTableResourceBase | None
    """Settings applied to every table in `tables`, overridden by the table's own settings."""
