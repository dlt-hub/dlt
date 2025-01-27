---
sidebar_label: sql_database
title: sources.sql_database
---

Source that loads tables form any SQLAlchemy supported database, supports batching requests and incremental loads.

## sql\_database

```python
@decorators.source
def sql_database(
    credentials: Union[ConnectionStringCredentials, Engine,
                       str] = dlt.secrets.value,
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
    engine_adapter_callback: Callable[[Engine], Engine] = None
) -> Iterable[DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/__init__.py#L31)

A dlt source which loads data from an SQL database using SQLAlchemy.
Resources are automatically created for each table in the schema or from the given list of tables.

**Arguments**:

- `credentials` _Union[ConnectionStringCredentials, Engine, str]_ - Database credentials or an `sqlalchemy.Engine` instance.
- `schema` _Optional[str]_ - Name of the database schema to load (if different from default).
- `metadata` _Optional[MetaData]_ - Optional `sqlalchemy.MetaData` instance. `schema` argument is ignored when this is used.
- `table_names` _Optional[List[str]]_ - A list of table names to load. By default, all tables in the schema are loaded.
- `chunk_size` _int_ - Number of rows yielded in one batch. SQL Alchemy will create additional internal rows buffer twice the chunk size.
- `backend` _TableBackend_ - Type of backend to generate table data. One of: "sqlalchemy", "pyarrow", "pandas" and "connectorx".
  "sqlalchemy" yields batches as lists of Python dictionaries, "pyarrow" and "connectorx" yield batches as arrow tables, "pandas" yields panda frames.
  "sqlalchemy" is the default and does not require additional dependencies, "pyarrow" creates stable destination schemas with correct data types,
  "connectorx" is typically the fastest but ignores the "chunk_size" so you must deal with large tables yourself.
- `detect_precision_hints` _bool_ - Deprecated. Use `reflection_level`. Set column precision and scale hints for supported data types in the target schema based on the columns in the source tables.
  This is disabled by default.
- `reflection_level` - (ReflectionLevel): Specifies how much information should be reflected from the source database schema.
- `"minimal"` - Only table names, nullability and primary keys are reflected. Data types are inferred from the data. This is the default option.
- `"full"` - Data types will be reflected on top of "minimal". `dlt` will coerce the data into reflected types if necessary.
- `"full_with_precision"` - Sets precision and scale on supported data types (ie. decimal, text, binary). Creates big and regular integer types.
- `defer_table_reflect` _bool_ - Will connect and reflect table schema only when yielding data. Requires table_names to be explicitly passed.
  Enable this option when running on Airflow. Available on dlt 0.4.4 and later.
- `table_adapter_callback` - (Callable): Receives each reflected table. May be used to modify the list of columns that will be selected.
- `backend_kwargs` _**kwargs_ - kwargs passed to table backend ie. "conn" is used to pass specialized connection string to connectorx.
- `include_views` _bool_ - Reflect views as well as tables. Note view names included in `table_names` are always included regardless of this setting.
- `type_adapter_callback(Optional[Callable])` - Callable to override type inference when reflecting columns.
  Argument is a single sqlalchemy data type (`TypeEngine` instance) and it should return another sqlalchemy data type, or `None` (type will be inferred from data)
  query_adapter_callback(Optional[Callable[Select, Table], Select]): Callable to override the SELECT query used to fetch data from the table.
  The callback receives the sqlalchemy `Select` and corresponding `Table`, 'Incremental` and `Engine` objects and should return the modified `Select` or `Text`.
- `resolve_foreign_keys` _bool_ - Translate foreign keys in the same schema to `references` table hints.
  May incur additional database calls as all referenced tables are reflected.
- `engine_adapter_callback` _Callable[[Engine], Engine]_ - Callback to configure, modify and Engine instance that will be used to open a connection ie. to
  set transaction isolation level.
  

**Returns**:

- `Iterable[DltResource]` - A list of DLT resources for each table to be loaded.

## sql\_table

```python
@decorators.resource(name=lambda args: args["table"],
                     standalone=True,
                     spec=SqlTableResourceConfiguration)
def sql_table(
        credentials: Union[ConnectionStringCredentials, Engine,
                           str] = dlt.secrets.value,
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
        write_disposition: TWriteDispositionConfig = "append") -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/__init__.py#L143)

A dlt resource which loads data from an SQL database table using SQLAlchemy.

**Arguments**:

- `credentials` _Union[ConnectionStringCredentials, Engine, str]_ - Database credentials or an `Engine` instance representing the database connection.
- `table` _str_ - Name of the table or view to load.
- `schema` _Optional[str]_ - Optional name of the schema the table belongs to.
- `metadata` _Optional[MetaData]_ - Optional `sqlalchemy.MetaData` instance. If provided, the `schema` argument is ignored.
- `incremental` _Optional[dlt.sources.incremental[Any]]_ - Option to enable incremental loading for the table.
  E.g., `incremental=dlt.sources.incremental('updated_at', pendulum.parse('2022-01-01T00:00:00Z'))`
- `chunk_size` _int_ - Number of rows yielded in one batch. SQL Alchemy will create additional internal rows buffer twice the chunk size.
- `backend` _TableBackend_ - Type of backend to generate table data. One of: "sqlalchemy", "pyarrow", "pandas" and "connectorx".
  "sqlalchemy" yields batches as lists of Python dictionaries, "pyarrow" and "connectorx" yield batches as arrow tables, "pandas" yields panda frames.
  "sqlalchemy" is the default and does not require additional dependencies, "pyarrow" creates stable destination schemas with correct data types,
  "connectorx" is typically the fastest but ignores the "chunk_size" so you must deal with large tables yourself.
- `reflection_level` - (ReflectionLevel): Specifies how much information should be reflected from the source database schema.
- `"minimal"` - Only table names, nullability and primary keys are reflected. Data types are inferred from the data. This is the default option.
- `"full"` - Data types will be reflected on top of "minimal". `dlt` will coerce the data into reflected types if necessary.
- `"full_with_precision"` - Sets precision and scale on supported data types (ie. decimal, text, binary). Creates big and regular integer types.
- `detect_precision_hints` _bool_ - Deprecated. Use `reflection_level`. Set column precision and scale hints for supported data types in the target schema based on the columns in the source tables.
  This is disabled by default.
- `defer_table_reflect` _bool_ - Will connect and reflect table schema only when yielding data. Enable this option when running on Airflow. Available
  on dlt 0.4.4 and later
- `table_adapter_callback` - (Callable): Receives each reflected table. May be used to modify the list of columns that will be selected.
- `backend_kwargs` _**kwargs_ - kwargs passed to table backend ie. "conn" is used to pass specialized connection string to connectorx.
- `type_adapter_callback(Optional[Callable])` - Callable to override type inference when reflecting columns.
  Argument is a single sqlalchemy data type (`TypeEngine` instance) and it should return another sqlalchemy data type, or `None` (type will be inferred from data)
- `included_columns` _Optional[List[str]_ - List of column names to select from the table. If not provided, all columns are loaded.
  query_adapter_callback(Optional[Callable[Select, Table], Select]): Callable to override the SELECT query used to fetch data from the table.
  The callback receives the sqlalchemy `Select` and corresponding `Table`, 'Incremental` and `Engine` objects and should return the modified `Select` or `Text`.
- `resolve_foreign_keys` _bool_ - Translate foreign keys in the same schema to `references` table hints.
  May incur additional database calls as all referenced tables are reflected.
- `engine_adapter_callback` _Callable[[Engine], Engine]_ - Callback to configure, modify and Engine instance that will be used to open a connection ie. to
  set transaction isolation level.
- `write_disposition` _TWriteDispositionConfig_ - write disposition of the table resource, defaults to `append`.
  

**Returns**:

- `DltResource` - The dlt resource for loading data from the SQL database table.

