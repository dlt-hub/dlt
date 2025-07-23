"""Source that loads tables form any SQLAlchemy supported database, supports batching requests and incremental loads."""

from typing import Callable, Dict, List, Optional, Union, Iterable, Any

import dlt
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.schema.typing import TWriteDispositionConfig
from dlt.common.libs.sql_alchemy import MetaData, Table, Engine

from dlt.common.typing import TColumnNames
from dlt.extract import DltResource, Incremental, decorators

from .helpers import (
    _execute_table_adapter,
    table_rows,
    engine_from_credentials,
    remove_nullability_adapter,
    TableBackend,
    SqlTableResourceConfiguration,
    _detect_precision_hints_deprecated,
    TQueryAdapter,
    TTableAdapter,
)
from .schema_types import (
    table_to_resource_hints,
    ReflectionLevel,
    TTypeAdapter,
)


@decorators.source
def sql_database(
    credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    table_names: Optional[List[str]] = dlt.config.value,
    chunk_size: int = 50000,
    backend: TableBackend = "sqlalchemy",
    detect_precision_hints: Optional[bool] = False,
    reflection_level: Optional[ReflectionLevel] = "full",
    defer_table_reflect: Optional[bool] = None,
    table_adapter_callback: Optional[TTableAdapter] = None,
    backend_kwargs: Dict[str, Any] = None,
    include_views: bool = False,
    type_adapter_callback: Optional[TTypeAdapter] = None,
    query_adapter_callback: Optional[TQueryAdapter] = None,
    resolve_foreign_keys: bool = False,
    engine_adapter_callback: Optional[Callable[[Engine], Engine]] = None,
) -> Iterable[DltResource]:
    """
    A dlt source which loads data from an SQL database using SQLAlchemy.
    Resources are automatically created for each table in the schema or from the given list of tables.

    Args:
        credentials (Union[ConnectionStringCredentials, Engine, str]): Database credentials or an `sqlalchemy.Engine` instance.

        schema (Optional[str]): Name of the database schema to load (if different from default).

        metadata (Optional[MetaData]): Optional `sqlalchemy.MetaData` instance. `schema` argument is ignored when this is used.

        table_names (Optional[List[str]]): A list of table names to load. By default, all tables in the schema are loaded.

        chunk_size (int): Number of rows yielded in one batch. SQL Alchemy will create additional internal rows buffer twice the chunk size.

        backend (TableBackend): Type of backend to generate table data. One of: "sqlalchemy", "pyarrow", "pandas" and "connectorx".
            "sqlalchemy" yields batches as lists of Python dictionaries, "pyarrow" and "connectorx" yield batches as arrow tables, "pandas" yields panda frames.
            "sqlalchemy" is the default and does not require additional dependencies, "pyarrow" creates stable destination schemas with correct data types,
            "connectorx" is typically the fastest but ignores the "chunk_size" so you must deal with large tables yourself.

        detect_precision_hints (Optional[bool]): Deprecated. Use `reflection_level`. Set column precision and scale hints for supported data types in the target schema based on the columns in the source tables.
            This is disabled by default.

        reflection_level (Optional[ReflectionLevel]): Specifies how much information should be reflected from the source database schema.
            "minimal": Only table names, nullability and primary keys are reflected. Data types are inferred from the data.
            "full" (default): Data types will be reflected on top of "minimal". `dlt` will coerce the data into reflected types if necessary.
            "full_with_precision": Sets precision and scale on supported data types (ie. decimal, text, binary). Creates big and regular integer types.

        defer_table_reflect (Optional[bool]): Will connect and reflect table schema only when yielding data. Requires table_names to be explicitly passed.
            Enable this option when running on Airflow and other orchestrators that create execution DAGs.

        table_adapter_callback (Optional[TTableAdapter]): Receives each reflected table. May be used to modify the list of columns that will be selected.

        backend_kwargs (Dict[str, Any]): kwargs passed to table backend ie. "conn" is used to pass specialized connection string to connectorx.

        include_views (bool): Reflect views as well as tables. Note view names included in `table_names` are always included regardless of this setting.

        type_adapter_callback (Optional[TTypeAdapter]): Callable to override type inference when reflecting columns.
            Argument is a single sqlalchemy data type (`TypeEngine` instance) and it should return another sqlalchemy data type, or `None` (type will be inferred from data)

        query_adapter_callback (Optional[TQueryAdapter]): Callable to override the SELECT query used to fetch data from the table.
            The callback receives the sqlalchemy `Select` and corresponding `Table`, 'Incremental` and `Engine` objects and should return the modified `Select` or `Text`.

        resolve_foreign_keys (bool): Translate foreign keys in the same schema to `references` table hints.
            May incur additional database calls as all referenced tables are reflected.

        engine_adapter_callback (Optional[Callable[[Engine], Engine]]): Callback to configure, modify and Engine instance that will be used to open a connection ie. to
            set transaction isolation level.

    Yields:
        DltResource: DLT resources for each table to be loaded.
    """
    # detect precision hints is deprecated
    _detect_precision_hints_deprecated(detect_precision_hints)

    if detect_precision_hints:
        reflection_level = "full_with_precision"
    else:
        reflection_level = reflection_level or "minimal"

    # set up alchemy engine
    engine = engine_from_credentials(credentials)
    engine.execution_options(stream_results=True, max_row_buffer=2 * chunk_size)
    if engine_adapter_callback:
        engine = engine_adapter_callback(engine)
    metadata = metadata or MetaData(schema=schema)

    if defer_table_reflect:
        if not table_names:
            raise ValueError("You must pass `table_names` to defer table reflection")
        table_infos = [(schema, table) for table in table_names]
    else:
        # reflect tables
        metadata.reflect(
            bind=engine,
            views=include_views or bool(table_names),  # Specified view names are always reflected
            only=table_names if table_names else None,
            resolve_fks=resolve_foreign_keys,
        )
        tables = list(metadata.tables.values())
        # Some extra tables may be reflected in metadata due to foreign keys
        table_infos = [
            (table.schema, table.name)
            for table in tables
            if table_names is None or table.name in table_names
        ]

    for table_schema, table_name in table_infos:
        yield sql_table(
            credentials=credentials,
            table=table_name,
            schema=table_schema,
            metadata=metadata,
            chunk_size=chunk_size,
            backend=backend,
            reflection_level=reflection_level,
            defer_table_reflect=defer_table_reflect,
            table_adapter_callback=table_adapter_callback,
            backend_kwargs=backend_kwargs,
            type_adapter_callback=type_adapter_callback,
            query_adapter_callback=query_adapter_callback,
            resolve_foreign_keys=resolve_foreign_keys,
            engine_adapter_callback=engine_adapter_callback,
        )


@decorators.resource(name=lambda args: args["table"], spec=SqlTableResourceConfiguration)
def sql_table(
    credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value,
    table: str = dlt.config.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    incremental: Optional[Incremental[Any]] = None,
    chunk_size: int = 50000,
    backend: TableBackend = "sqlalchemy",
    detect_precision_hints: Optional[bool] = None,
    reflection_level: Optional[ReflectionLevel] = "full",
    defer_table_reflect: Optional[bool] = None,
    table_adapter_callback: Optional[TTableAdapter] = None,
    backend_kwargs: Dict[str, Any] = None,
    type_adapter_callback: Optional[TTypeAdapter] = None,
    included_columns: Optional[List[str]] = None,
    query_adapter_callback: Optional[TQueryAdapter] = None,
    resolve_foreign_keys: bool = False,
    engine_adapter_callback: Callable[[Engine], Engine] = None,
    write_disposition: TWriteDispositionConfig = "append",
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
) -> DltResource:
    """
    A dlt resource which loads data from an SQL database table using SQLAlchemy.

    Args:
        credentials (Union[ConnectionStringCredentials, Engine, str]): Database credentials or an `Engine` instance representing the database connection.

        table (str): Name of the table or view to load.

        schema (Optional[str]): Optional name of the schema the table belongs to.

        metadata (Optional[MetaData]): Optional `sqlalchemy.MetaData` instance. If provided, the `schema` argument is ignored.

        incremental (Optional[Incremental[Any]]): Option to enable incremental loading for the table.
            E.g., `incremental=dlt.sources.incremental('updated_at', pendulum.parse('2022-01-01T00:00:00Z'))`

        chunk_size (int): Number of rows yielded in one batch. SQL Alchemy will create additional internal rows buffer twice the chunk size.

        backend (TableBackend): Type of backend to generate table data. One of: "sqlalchemy", "pyarrow", "pandas" and "connectorx".
            "sqlalchemy" yields batches as lists of Python dictionaries, "pyarrow" and "connectorx" yield batches as arrow tables, "pandas" yields panda frames.
            "sqlalchemy" is the default and does not require additional dependencies, "pyarrow" creates stable destination schemas with correct data types,
            "connectorx" is typically the fastest but ignores the "chunk_size" so you must deal with large tables yourself.

        detect_precision_hints (Optional[bool]): Deprecated. Use `reflection_level`. Set column precision and scale hints for supported data types in the target schema based on the columns in the source tables.
            This is disabled by default.

        reflection_level (Optional[ReflectionLevel]): Specifies how much information should be reflected from the source database schema.
            "minimal": Only table names, nullability and primary keys are reflected. Data types are inferred from the data.
            "full" (default): Data types will be reflected on top of "minimal". `dlt` will coerce the data into reflected types if necessary.
            "full_with_precision": Sets precision and scale on supported data types (ie. decimal, text, binary). Creates big and regular integer types.

        defer_table_reflect (Optional[bool]): Will connect and reflect table schema only when yielding data.
            Enable this option when running on Airflow and other orchestrators that create execution DAGs.

        table_adapter_callback (Optional[TTableAdapter]): Receives each reflected table. May be used to modify the list of columns that will be selected.

        backend_kwargs (Dict[str, Any], optional): kwargs passed to table backend ie. "conn" is used to pass specialized connection string to connectorx.

        type_adapter_callback (Optional[TTypeAdapter]): Callable to override type inference when reflecting columns.
            Argument is a single sqlalchemy data type (`TypeEngine` instance) and it should return another sqlalchemy data type, or `None` (type will be inferred from data)

        included_columns (Optional[List[str]]): List of column names to select from the table. If not provided, all columns are loaded.

        query_adapter_callback (Optional[TQueryAdapter]): Callable to override the SELECT query used to fetch data from the table.
            The callback receives the sqlalchemy `Select` and corresponding `Table`, 'Incremental` and `Engine` objects and should return the modified `Select` or `Text`.

        resolve_foreign_keys (bool): Translate foreign keys in the same schema to `references` table hints.
            May incur additional database calls as all referenced tables are reflected.

        engine_adapter_callback (Callable[[Engine], Engine]): Callback to configure, modify and Engine instance that will be used to open a connection ie. to
            set transaction isolation level.

        write_disposition (TWriteDispositionConfig): write disposition of the table resource, defaults to `append`.
        primary_key (TColumnNames): A list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
        merge_key (TColumnNames): A list of column names that define a merge key. Typically used with "merge" write disposition to remove overlapping data ranges ie. to
            keep a single record for a given day.

    Returns:
        DltResource: The dlt resource for loading data from the SQL database table.
    """
    # In case we get None from the config, we want to default to "append"
    write_disposition = write_disposition if write_disposition else "append"

    _detect_precision_hints_deprecated(detect_precision_hints)

    if detect_precision_hints:
        reflection_level = "full_with_precision"
    else:
        reflection_level = reflection_level or "minimal"

    engine = engine_from_credentials(credentials, may_dispose_after_use=True)
    engine.execution_options(stream_results=True, max_row_buffer=2 * chunk_size)
    if engine_adapter_callback:
        engine = engine_adapter_callback(engine)
    metadata = metadata or MetaData(schema=schema)

    # Table object is only created when reflecting, we don't want empty tables in metadata
    # as it breaks foreign key resolution
    table_obj = metadata.tables.get(table)
    if table_obj is None and not defer_table_reflect:
        table_obj = Table(table, metadata, autoload_with=engine, resolve_fks=resolve_foreign_keys)

    if table_obj is not None:
        if not defer_table_reflect:
            table_obj = _execute_table_adapter(table_obj, table_adapter_callback, included_columns)
        skip_nested_on_minimal = backend == "sqlalchemy"
        hints = table_to_resource_hints(
            table_obj,
            reflection_level,
            type_adapter_callback,
            skip_nested_on_minimal,
            resolve_foreign_keys=resolve_foreign_keys,
        )
    else:
        hints = {}

    if primary_key:
        # may be from what is found in the reflection, so it is set explicitly
        hints["primary_key"] = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    return decorators.resource(
        table_rows,
        name=str(table),
        write_disposition=write_disposition,
        merge_key=merge_key,
        **hints,
    )(
        engine,
        table_obj if table_obj is not None else table,  # Pass table name if reflection deferred
        metadata,
        chunk_size,
        backend,
        incremental=incremental,
        reflection_level=reflection_level,
        table_adapter_callback=table_adapter_callback,
        backend_kwargs=backend_kwargs,
        type_adapter_callback=type_adapter_callback,
        included_columns=included_columns,
        query_adapter_callback=query_adapter_callback,
        resolve_foreign_keys=resolve_foreign_keys,
    )


__all__ = [
    "sql_database",
    "sql_table",
    "ReflectionLevel",
    "TTypeAdapter",
    "engine_from_credentials",
    "remove_nullability_adapter",
    "TableBackend",
    "TQueryAdapter",
    "TTableAdapter",
]
