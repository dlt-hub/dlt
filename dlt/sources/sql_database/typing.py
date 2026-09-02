"""Typed configuration for the declarative SQL database source.

The config is a plain dictionary so it can be written in Python, TOML, YAML or generated
by an agent. Keys mirror the arguments of `sql_table` and the table hints of `dlt.resource`
so there's a single set of names to learn. Callables and class arguments (adapters, custom
table loaders, `Engine` instances) are accepted as escape hatches but keep the config
non-serializable, so use them only when needed.
"""

from typing import Any, Callable, Dict, List, Optional, Type, Union

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

    schema: Optional[str]
    """Name of the database schema the table belongs to. Defaults to the connection's default schema."""
    incremental: Optional[IncrementalArgs]
    """Incremental loading settings ie. `{"cursor_path": "updated_at", "initial_value": "2024-01-01"}`."""
    chunk_size: Optional[int]
    backend: Optional[TableBackend]
    backend_kwargs: Optional[Dict[str, Any]]
    reflection_level: Optional[ReflectionLevel]
    defer_table_reflect: Optional[bool]
    included_columns: Optional[List[str]]
    excluded_columns: Optional[List[str]]
    resolve_foreign_keys: Optional[bool]
    max_table_nesting: Optional[int]
    selected: Optional[bool]
    parallelized: Optional[bool]
    table_adapter_callback: Optional[TTableAdapter]
    type_adapter_callback: Optional[TTypeAdapter]
    query_adapter_callback: Optional[TQueryAdapter]
    table_loader_class: Optional[Type[BaseTableLoader]]


class SqlTableResource(SqlTableResourceBase, total=False):
    """Settings of a single table. At least one of `name` or `table` must be present."""

    name: Optional[str]
    """Name of the dlt resource. Defaults to `table`."""
    table: Optional[str]
    """Name of the table (or view) in the database. Defaults to `name`."""


class SqlDatabaseConfig(TypedDict, total=False):
    tables: Optional[List[Union[str, SqlTableResource, DltResource]]]
    """Tables to load: a table name, a table config or a ready `DltResource` ie. from `sql_table`.
    When omitted, all tables in the database schema are discovered and loaded with
    `table_defaults`, like in `sql_database`. An empty list loads no tables."""
    include_views: Optional[bool]
    """Discover views as well as tables. Declared views are always loaded."""
    credentials: Optional[Union[ConnectionStringCredentials, Engine, str]]
    """Database credentials or an `Engine` instance. `sql_database_source` resolves this from
    dlt config providers (ie. `secrets.toml`) when omitted."""
    engine_kwargs: Optional[Dict[str, Any]]
    """Keyword arguments passed to `sqlalchemy.create_engine()`."""
    engine_adapter_callback: Optional[Callable[[Engine], Engine]]
    """Callback to configure or replace the `Engine` shared by all tables."""
    table_defaults: Optional[SqlTableResourceBase]
    """Settings applied to every table in `tables`, overridden by the table's own settings."""
