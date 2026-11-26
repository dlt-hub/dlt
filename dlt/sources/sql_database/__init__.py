"""Source that loads tables form any SQLAlchemy supported database, supports batching requests and incremental loads."""

from typing import Callable, Dict, List, Optional, Type, Union, Iterable, Any

import dlt
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.schema.typing import TSchemaContract, TWriteDispositionConfig
from dlt.common.libs.sql_alchemy import MetaData, Table, Engine
from dlt.common.typing import TColumnNames
from dlt.extract import DltResource, DltSource, Incremental, decorators
from dlt.sources.sql_database.config_setup import (
    merge_table_defaults,
    split_table_config,
    validate_config,
)
from dlt.sources.sql_database.typing import (
    SqlDatabaseConfig,
    SqlTableResource,
    SqlTableResourceBase,
)

from .helpers import (
    _execute_table_adapter,
    default_engine_adapter_callback,
    record_table_input,
    table_rows,
    engine_from_credentials,
    remove_nullability_adapter,
    BaseTableLoader,
    TableLoader,
    ConnectorXTableLoader,
    register_table_loader_backend,
    get_table_loader_class,
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
    engine_kwargs: Optional[Dict[str, Any]] = None,
    table_loader_class: Optional[Type[BaseTableLoader]] = None,
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
            "sqlalchemy" yields batches as lists of Python dictionaries, "pyarrow" and "connectorx" yield batches as arrow tables, "pandas" yields pandas DataFrames.
            "sqlalchemy" is the default and does not require additional dependencies, "pyarrow" creates stable destination schemas with correct data types,
            "connectorx" is typically the fastest but ignores the "chunk_size" so you must deal with large tables yourself.

        detect_precision_hints (Optional[bool]): Deprecated. Use `reflection_level`. Set column precision and scale hints for supported data types in the target schema based on the columns in the source tables.
            This is disabled by default.

        reflection_level (Optional[ReflectionLevel]): Specifies how much information should be reflected from the source database schema.
            "minimal": Only table names, nullability and primary keys are reflected. Data types are inferred from the data.
            "full" (default): Data types will be reflected on top of "minimal". `dlt` will coerce the data into reflected types if necessary.
            "full_with_precision": Sets precision and scale on supported data types (ie. decimal, text, binary). Creates big and regular integer types.

        defer_table_reflect (Optional[bool]): Will connect and reflect table schema only when yielding data. Requires `table_names` to be explicitly passed.
            Enable this option when running on Airflow and other orchestrators that create execution DAGs. When True, schema is decided during execution,
            which may override `query_adapter_callback` modifications or `apply_hints`.

        table_adapter_callback (Optional[TTableAdapter]): Receives each reflected table. May be used to modify the list of columns that will be selected.

        backend_kwargs (Dict[str, Any]): kwargs passed to table backend ie. "conn" is used to pass specialized connection string to connectorx.

        include_views (bool): Reflect views as well as tables. Note view names included in `table_names` are always included regardless of this setting.

        type_adapter_callback (Optional[TTypeAdapter]): Callable to override type inference when reflecting columns.
            Argument is a single sqlalchemy data type (`TypeEngine` instance) and it should return another sqlalchemy data type, or `None` (type will be inferred from data)

        query_adapter_callback (Optional[TQueryAdapter]): Callable to override the SELECT query used to fetch data from the table.
            The callback receives the sqlalchemy `Select` and corresponding `Table`, 'Incremental` and `Engine` objects and should return the modified `Select` or `Text`.

        resolve_foreign_keys (bool): Translate foreign keys in the same schema to `references` table hints.
            May incur additional database calls as all referenced tables are reflected.

        engine_adapter_callback (Optional[Callable[[Engine], Engine]]): Callback to configure, modify an Engine instance that will be used to open a connection ie. to
            set transaction isolation level.

        engine_kwargs (Optional[Dict[str, Any]]): Optional SQLAlchemy engine keyword arguments passed directly to `sqlalchemy.create_engine()`.
            They always affect table reflection and also data loading if SQLAlchemy backend is used (default). If other backend is used, pass equivalent idiomatic params to backend_kwargs.

        table_loader_class (Optional[Type[BaseTableLoader]]): Custom table loader class to use for loading data.
            Subclass `TableLoader` to customize row loading within the SQLAlchemy ecosystem (e.g. pagination,
            retry logic), or subclass `BaseTableLoader` for entirely different backends. When not provided,
            the default loader is selected based on the `backend` parameter.

    Yields:
        DltResource: DLT resources for each table to be loaded.
    """
    # detect precision hints is deprecated
    _detect_precision_hints_deprecated(detect_precision_hints)

    if detect_precision_hints:
        reflection_level = "full_with_precision"
    else:
        reflection_level = reflection_level or "minimal"

    engine_kwargs = engine_kwargs or {}
    engine = engine_from_credentials(
        credentials,
        may_dispose_after_use=False,
        **engine_kwargs,
    )
    engine.execution_options(stream_results=True, max_row_buffer=2 * chunk_size)
    if engine_adapter_callback:
        engine = engine_adapter_callback(engine)
    metadata = metadata or MetaData(schema=schema)
    default_engine_adapter_callback(engine, metadata)

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

    # dispose the reflection engine — MetaData no longer needs it and each
    # sql_table() resource creates its own engine from credentials.
    # skipped for externally-provided Engine instances (user manages lifecycle).
    if not isinstance(credentials, Engine):
        engine.dispose()

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
            engine_kwargs=engine_kwargs,
            table_loader_class=table_loader_class,
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
    excluded_columns: Optional[List[str]] = None,
    query_adapter_callback: Optional[TQueryAdapter] = None,
    resolve_foreign_keys: bool = False,
    engine_adapter_callback: Callable[[Engine], Engine] = None,
    write_disposition: TWriteDispositionConfig = "append",
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    engine_kwargs: Optional[Dict[str, Any]] = None,
    table_loader_class: Optional[Type[BaseTableLoader]] = None,
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
            "sqlalchemy" yields batches as lists of Python dictionaries, "pyarrow" and "connectorx" yield batches as arrow tables, "pandas" yields pandas DataFrames.
            "sqlalchemy" is the default and does not require additional dependencies, "pyarrow" creates stable destination schemas with correct data types,
            "connectorx" is typically the fastest but ignores the "chunk_size" so you must deal with large tables yourself.

        detect_precision_hints (Optional[bool]): Deprecated. Use `reflection_level`. Set column precision and scale hints for supported data types in the target schema based on the columns in the source tables.
            This is disabled by default.

        reflection_level (Optional[ReflectionLevel]): Specifies how much information should be reflected from the source database schema.
            "minimal": Only table names, nullability and primary keys are reflected. Data types are inferred from the data.
            "full" (default): Data types will be reflected on top of "minimal". `dlt` will coerce the data into reflected types if necessary.
            "full_with_precision": Sets precision and scale on supported data types (ie. decimal, text, binary). Creates big and regular integer types.

        defer_table_reflect (Optional[bool]): Will connect and reflect table schema only when yielding data. Requires `table_names` to be explicitly passed.
            Enable this option when running on Airflow and other orchestrators that create execution DAGs. When True, schema is decided during execution,
            which may override `query_adapter_callback` modifications or `apply_hints`.

        table_adapter_callback (Optional[TTableAdapter]): Receives each reflected table. May be used to modify the list of columns that will be selected.

        backend_kwargs (Dict[str, Any], optional): kwargs passed to table backend ie. "conn" is used to pass specialized connection string to connectorx.

        type_adapter_callback (Optional[TTypeAdapter]): Callable to override type inference when reflecting columns.
            Argument is a single sqlalchemy data type (`TypeEngine` instance) and it should return another sqlalchemy data type, or `None` (type will be inferred from data)

        included_columns (Optional[List[str]]): List of column names to select from the table. If not provided, all columns are loaded.

        excluded_columns (Optional[List[str]]): List of column names to exclude from select. If not provided, all columns are loaded.

        query_adapter_callback (Optional[TQueryAdapter]): Callable to override the SELECT query used to fetch data from the table.
            The callback receives the sqlalchemy `Select` and corresponding `Table`, 'Incremental` and `Engine` objects and should return the modified `Select` or `Text`.

        resolve_foreign_keys (bool): Translate foreign keys in the same schema to `references` table hints.
            May incur additional database calls as all referenced tables are reflected.

        engine_adapter_callback (Callable[[Engine], Engine]): Callback to configure, modify an Engine instance that will be used to open a connection ie. to
            set transaction isolation level.

        write_disposition (TWriteDispositionConfig): write disposition of the table resource, defaults to `append`.
        primary_key (TColumnNames): A list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
        merge_key (TColumnNames): A list of column names that define a merge key. Typically used with "merge" write disposition to remove overlapping data ranges ie. to
            keep a single record for a given day.

        engine_kwargs (Optional[Dict[str, Any]]): Optional SQLAlchemy engine keyword arguments passed directly to `sqlalchemy.create_engine()`.
            They always affect table reflection and also data loading if SQLAlchemy backend is used (default). If other backend is used, pass equivalent idiomatic params to backend_kwargs.

        table_loader_class (Optional[Type[BaseTableLoader]]): Custom table loader class to use for loading data.
            Subclass `TableLoader` to customize row loading within the SQLAlchemy ecosystem (e.g. pagination,
            retry logic), or subclass `BaseTableLoader` for entirely different backends. When not provided,
            the default loader is selected based on the `backend` parameter.

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

    engine_kwargs = engine_kwargs or {}
    engine = engine_from_credentials(
        credentials,
        may_dispose_after_use=True,
        **engine_kwargs,
    )
    engine.execution_options(stream_results=True, max_row_buffer=2 * chunk_size)
    if engine_adapter_callback:
        engine = engine_adapter_callback(engine)
    metadata = metadata or MetaData(schema=schema)
    default_engine_adapter_callback(engine, metadata)

    # look up by the schema-qualified key so a reused metadata is honored as a reflection cache
    table_key = f"{metadata.schema}.{table}" if metadata.schema else table
    table_obj = metadata.tables.get(table_key)
    if table_obj is None and not defer_table_reflect:
        # Table object is only created when reflecting, we don't want empty tables in metadata
        # as it breaks foreign key resolution
        table_obj = Table(table, metadata, autoload_with=engine, resolve_fks=resolve_foreign_keys)

    if table_obj is not None:
        if not defer_table_reflect:
            table_obj = _execute_table_adapter(
                table_obj, table_adapter_callback, included_columns, excluded_columns
            )
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

    resource = decorators.resource(
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
        excluded_columns=excluded_columns,
        query_adapter_callback=query_adapter_callback,
        resolve_foreign_keys=resolve_foreign_keys,
        table_loader_class=table_loader_class,
    )
    record_table_input(resource, credentials, schema, str(table))
    return resource


@decorators.source(name="sql_database", section="sql_database")
def _declarative_sql_database(
    tables: list[str | SqlTableResource | DltResource] | None = None,
    credentials: ConnectionStringCredentials | Engine | str = dlt.secrets.value,
    table_defaults: SqlTableResourceBase | None = None,
    include_views: bool | None = None,
    engine_kwargs: dict[str, Any] | None = None,
    engine_adapter_callback: Callable[[Engine], Engine] | None = None,
) -> list[DltResource]:
    """Declarative SQL database source.

    Arguments not passed explicitly are resolved from dlt config providers.

    NOTE. This source factory isn't meant to be used directly. It is used
    by `sql_database_source()`
    """
    return sql_database_resources(
        SqlDatabaseConfig(
            tables=tables,
            credentials=credentials,
            table_defaults=table_defaults,
            include_views=include_views,
            engine_kwargs=engine_kwargs,
            engine_adapter_callback=engine_adapter_callback,
        )
    )


def sql_database_source(
    config: SqlDatabaseConfig,
    name: str = None,
    section: str = None,
    max_table_nesting: int = None,
    root_key: bool = None,
    schema: dlt.Schema = None,
    schema_contract: TSchemaContract = None,
    parallelized: bool = False,
) -> DltSource:
    """Creates a SQL database source from a declarative configuration.

    Tables that are not declared in `config["tables"]` are discovered from the database, like
    in the imperative `sql_database()` source.

    Compared to `sql_database()`, `sql_database_source()` can be configured more extensively directly
    from `config.toml` and other config providers.

    Args:
        config (SqlDatabaseConfig): Configuration of the connection and the loaded tables.
        name (str, optional): Name of the source.
        section (str, optional): Section of the configuration file.
        max_table_nesting (int, optional): Maximum depth of nested table above which
            the remaining nodes are loaded as structs or JSON.
        root_key (bool, optional): Enables merging on all resources by propagating
            root foreign key to child tables. Defaults to False.
        schema (dlt.Schema, optional): An explicit dlt `Schema` instance to be associated with the
            source. Not to be confused with the database schema which is set per table or in
            `config["table_defaults"]`.
        schema_contract (TSchemaContract, optional): Schema contract settings
            that will be applied to this source.
        parallelized (bool, optional): If `True`, resource generators will be extracted in
            parallel. Defaults to `False` which preserves resource settings.

    Returns:
        DltSource: A configured dlt source.

    Example:

        ```python
        db_source = sql_database_source({
            "credentials": "postgresql://loader@localhost/dvdrental",
            "table_defaults": {"schema": "public", "reflection_level": "full"},
            "tables": [
                "customer",
                {
                    "name": "items",
                    "table": "inventory_items",
                    "included_columns": ["id", "name", "updated_at"],
                },
                {
                    "name": "orders",
                    "write_disposition": "merge",
                    "primary_key": "id",
                    "incremental": {
                        "cursor_path": "created_at",
                        "initial_value": "2024-01-25T00:00:00Z",
                    },
                },
            ],
        })
        ```

    """

    validate_config(config)
    decorated = _declarative_sql_database.clone(
        name=name,
        section=section,
        max_table_nesting=max_table_nesting,
        root_key=root_key,
        schema=schema,
        schema_contract=schema_contract,
        parallelized=parallelized,
    )
    return decorated(**config)


def sql_database_resources(config: SqlDatabaseConfig) -> list[DltResource]:
    """Creates a list of resources from a declarative SQL database configuration.

    Resources may be used to create a custom source or passed to `pipeline.run` directly.
    `config["credentials"]` is required here: use `sql_database_source` to resolve credentials
    from dlt config providers.

    Args:
        config (SqlDatabaseConfig): Configuration of the connection and the loaded tables.

    Returns:
        list[DltResource]: A resource per table, in the order the tables are declared.
    """
    validate_config(config)
    credentials = config.get("credentials")
    if credentials is None:
        raise ValueError(
            "`credentials` are required in the config passed to `sql_database_resources`. Use"
            " `sql_database_source` to resolve them from dlt config providers i.e., secrets.toml."
        )

    # all tables share a single engine
    engine = engine_from_credentials(
        credentials, may_dispose_after_use=False, **(config.get("engine_kwargs", {}) or {})
    )
    if engine_adapter_callback := config.get("engine_adapter_callback"):
        engine = engine_adapter_callback(engine)

    table_defaults = config.get("table_defaults") or {}
    tables = config.get("tables")
    metadata: MetaData | None = None
    if tables is None:
        # if `tables=None`, discover tables in source (matches `sql_database())
        tables, metadata = _discover_tables(engine, table_defaults, config.get("include_views"))

    resources: list[DltResource] = []
    for table in tables:
        if isinstance(table, DltResource):
            resources.append(table)
            continue

        table_config = merge_table_defaults(table_defaults, table)
        table_args, hints, resource_args = split_table_config(table_config)
        resource = sql_table(credentials=engine, metadata=metadata, **table_args)

        if table_config["name"] != table_config["table"]:
            resource = resource.with_name(table_config["name"])

        if hints:
            resource.apply_hints(**hints)

        if (max_table_nesting := resource_args.get("max_table_nesting")) is not None:
            resource.max_table_nesting = max_table_nesting

        if (selected := resource_args.get("selected")) is not None:
            resource.selected = selected

        if resource_args.get("parallelized"):
            resource.parallelize()

        resources.append(resource)

    return resources


def _discover_tables(
    engine: Engine, table_defaults: SqlTableResourceBase, include_views: bool | None
) -> tuple[list[str | SqlTableResource | DltResource], MetaData]:
    """Reflects all tables in the database schema of `table_defaults` and returns their names
    together with the `MetaData` that `sql_table` reuses as a reflection cache.
    """
    metadata = MetaData(schema=table_defaults.get("schema"))
    default_engine_adapter_callback(engine, metadata)
    metadata.reflect(
        bind=engine,
        views=bool(include_views),
        resolve_fks=bool(table_defaults.get("resolve_foreign_keys")),
    )
    return [table.name for table in metadata.tables.values()], metadata


__all__ = [
    "BaseTableLoader",
    "ReflectionLevel",
    "SqlDatabaseConfig",
    "SqlTableResource",
    "SqlTableResourceBase",
    "TQueryAdapter",
    "TTableAdapter",
    "TTypeAdapter",
    "TableBackend",
    "TableLoader",
    "engine_from_credentials",
    "get_table_loader_class",
    "register_table_loader_backend",
    "remove_nullability_adapter",
    "sql_database",
    "sql_database_resources",
    "sql_database_source",
    "sql_table",
]
