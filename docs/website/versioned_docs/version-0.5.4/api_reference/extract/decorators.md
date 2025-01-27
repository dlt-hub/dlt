---
sidebar_label: decorators
title: extract.decorators
---

## SourceSchemaInjectableContext Objects

```python
@configspec
class SourceSchemaInjectableContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/decorators.py#L71)

A context containing the source schema, present when dlt.source/resource decorated function is executed

## SourceInjectableContext Objects

```python
@configspec
class SourceInjectableContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/decorators.py#L80)

A context containing the source schema, present when dlt.resource decorated function is executed

## source

```python
def source(func: Optional[AnyFun] = None,
           name: str = None,
           section: str = None,
           max_table_nesting: int = None,
           root_key: bool = False,
           schema: Schema = None,
           schema_contract: TSchemaContract = None,
           spec: Type[BaseConfiguration] = None,
           _impl_cls: Type[TDltSourceImpl] = DltSource) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/decorators.py#L123)

A decorator that transforms a function returning one or more `dlt resources` into a `dlt source` in order to load it with `dlt`.

**Notes**:

  A `dlt source` is a logical grouping of resources that are often extracted and loaded together. A source is associated with a schema, which describes the structure of the loaded data and provides instructions how to load it.
  Such schema contains table schemas that describe the structure of the data coming from the resources.
  
  Please refer to https://dlthub.com/docs/general-usage/source for a complete documentation.
  
  Credentials:
  Another important function of the source decorator is to provide credentials and other configuration to the code that extracts data. The decorator may automatically bind the source function arguments to the secret and config values.
```py
@dlt.source
def chess(username, chess_url: str = dlt.config.value, api_secret = dlt.secrets.value, title: str = "GM"):
    return user_profile(username, chess_url, api_secret), user_games(username, chess_url, api_secret, with_titles=title)

list(chess("magnuscarlsen"))
```
  
  Here `username` is a required, explicit python argument, `chess_url` is a required argument, that if not explicitly passed will be taken from configuration ie. `config.toml`, `api_secret` is a required argument, that if not explicitly passed will be taken from dlt secrets ie. `secrets.toml`.
  See https://dlthub.com/docs/general-usage/credentials for details.
  

**Arguments**:

- `func` - A function that returns a dlt resource or a list of those or a list of any data items that can be loaded by `dlt`.
  
- `name` _str, optional_ - A name of the source which is also the name of the associated schema. If not present, the function name will be used.
  
- `section` _str, optional_ - A name of configuration. If not present, the current python module name will be used.
  
- `max_table_nesting` _int, optional_ - A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.
  
- `root_key` _bool_ - Enables merging on all resources by propagating root foreign key to child tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge. Defaults to False.
  
- `schema` _Schema, optional_ - An explicit `Schema` instance to be associated with the source. If not present, `dlt` creates a new `Schema` object with provided `name`. If such `Schema` already exists in the same folder as the module containing the decorated function, such schema will be loaded from file.
  
- `schema_contract` _TSchemaContract, optional_ - Schema contract settings that will be applied to this resource.
  
- `spec` _Type[BaseConfiguration], optional_ - A specification of configuration and secret values required by the source.
  
- `_impl_cls` _Type[TDltSourceImpl], optional_ - A custom implementation of DltSource, may be also used to providing just a typing stub
  

**Returns**:

  `DltSource` instance

## resource

```python
def resource(
        data: Optional[Any] = None,
        name: TTableHintTemplate[str] = None,
        table_name: TTableHintTemplate[str] = None,
        max_table_nesting: int = None,
        write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
        table_format: TTableHintTemplate[TTableFormat] = None,
        file_format: TTableHintTemplate[TFileFormat] = None,
        selected: bool = True,
        spec: Type[BaseConfiguration] = None,
        parallelized: bool = False,
        _impl_cls: Type[TDltResourceImpl] = DltResource,
        standalone: bool = False,
        data_from: TUnboundDltResource = None) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/decorators.py#L376)

When used as a decorator, transforms any generator (yielding) function into a `dlt resource`. When used as a function, it transforms data in `data` argument into a `dlt resource`.

**Notes**:

  A `resource`is a location within a `source` that holds the data with specific structure (schema) or coming from specific origin. A resource may be a rest API endpoint, table in the database or a tab in Google Sheets.
  A `dlt resource` is python representation of a `resource` that combines both data and metadata (table schema) that describes the structure and instructs the loading of the data.
  A `dlt resource` is also an `Iterable` and can used like any other iterable object ie. list or tuple.
  
  Please refer to https://dlthub.com/docs/general-usage/resource for a complete documentation.
  
  Credentials:
  If used as a decorator (`data` argument is a `Generator`), it may automatically bind the source function arguments to the secret and config values.
```py
@dlt.resource
def user_games(username, chess_url: str = dlt.config.value, api_secret = dlt.secrets.value):
    return requests.get("%s/games/%s" % (chess_url, username), headers={"Authorization": f"Bearer {api_secret}"})

list(user_games("magnuscarlsen"))
```
  
  Here `username` is a required, explicit python argument, `chess_url` is a required argument, that if not explicitly passed will be taken from configuration ie. `config.toml`, `api_secret` is a required argument, that if not explicitly passed will be taken from dlt secrets ie. `secrets.toml`.
  See https://dlthub.com/docs/general-usage/credentials for details.
  Note that if decorated function is an inner function, passing of the credentials will be disabled.
  

**Arguments**:

- `data` _Callable | Any, optional_ - a function to be decorated or a data compatible with `dlt` `run`.
  
- `name` _str, optional_ - A name of the resource that by default also becomes the name of the table to which the data is loaded.
  If not present, the name of the decorated function will be used.
  
- `table_name` _TTableHintTemplate[str], optional_ - An table name, if different from `name`.
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  
- `max_table_nesting` _int, optional_ - A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.
  
- `write_disposition` _TTableHintTemplate[TWriteDispositionConfig], optional_ - Controls how to write data to a table. Accepts a shorthand string literal or configuration dictionary.
  Allowed shorthand string literals: `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
  Write behaviour can be further customized through a configuration dictionary. For example, to obtain an SCD2 table provide `write_disposition={"disposition": "merge", "strategy": "scd2"}`.
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  
- `columns` _Sequence[TAnySchemaColumns], optional_ - A list, dict or pydantic model of column schemas.
  Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  When the argument is a pydantic model, the model will be used to validate the data yielded by the resource as well.
  
- `primary_key` _str | Sequence[str]_ - A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  
- `merge_key` _str | Sequence[str]_ - A column name or a list of column names that define a merge key. Typically used with "merge" write disposition to remove overlapping data ranges ie. to keep a single record for a given day.
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  
- `schema_contract` _TSchemaContract, optional_ - Schema contract settings that will be applied to all resources of this source (if not overridden in the resource itself)
  
- `table_format` _Literal["iceberg", "delta"], optional_ - Defines the storage format of the table. Currently only "iceberg" is supported on Athena, and "delta" on the filesystem.
  Other destinations ignore this hint.
  
- `file_format` _Literal["preferred", ...], optional_ - Format of the file in which resource data is stored. Useful when importing external files. Use `preferred` to force
  a file format that is preferred by the destination used. This setting superseded the `load_file_format` passed to pipeline `run` method.
  
- `selected` _bool, optional_ - When `True` `dlt pipeline` will extract and load this resource, if `False`, the resource will be ignored.
  
- `spec` _Type[BaseConfiguration], optional_ - A specification of configuration and secret values required by the source.
  
- `standalone` _bool, optional_ - Returns a wrapped decorated function that creates DltResource instance. Must be called before use. Cannot be part of a source.
  
- `data_from` _TUnboundDltResource, optional_ - Allows to pipe data from one resource to another to build multi-step pipelines.
  
- `parallelized` _bool, optional_ - If `True`, the resource generator will be extracted in parallel with other resources. Defaults to `False`.
  
- `_impl_cls` _Type[TDltResourceImpl], optional_ - A custom implementation of DltResource, may be also used to providing just a typing stub
  

**Raises**:

- `ResourceNameMissing` - indicates that name of the resource cannot be inferred from the `data` being passed.
- `InvalidResourceDataType` - indicates that the `data` argument cannot be converted into `dlt resource`
  

**Returns**:

  TDltResourceImpl instance which may be loaded, iterated or combined with other resources into a pipeline.

## transformer

```python
def transformer(
        f: Optional[Callable[Concatenate[TDataItem, TResourceFunParams],
                             Any]] = None,
        data_from: TUnboundDltResource = DltResource.Empty,
        name: TTableHintTemplate[str] = None,
        table_name: TTableHintTemplate[str] = None,
        max_table_nesting: int = None,
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
        table_format: TTableHintTemplate[TTableFormat] = None,
        file_format: TTableHintTemplate[TFileFormat] = None,
        selected: bool = True,
        spec: Type[BaseConfiguration] = None,
        parallelized: bool = False,
        standalone: bool = False,
        _impl_cls: Type[TDltResourceImpl] = DltResource) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/decorators.py#L674)

A form of `dlt resource` that takes input from other resources via `data_from` argument in order to enrich or transform the data.

The decorated function `f` must take at least one argument of type TDataItems (a single item or list of items depending on the resource `data_from`). `dlt` will pass
metadata associated with the data item if argument with name `meta` is present. Otherwise, transformer function may take more arguments and be parametrized
like the resources.

You can bind the transformer early by specifying resource in `data_from` when the transformer is created or create dynamic bindings later with | operator
which is demonstrated in example below:

**Example**:

```py
@dlt.resource
def players(title, chess_url=dlt.config.value):
    r = requests.get(f"{chess_url}titled/{title}")
    yield r.json()["players"]  # returns list of player names

# this resource takes data from players and returns profiles
@dlt.transformer(write_disposition="replace")
def player_profile(player: Any) -> Iterator[TDataItems]:
    r = requests.get(f"{chess_url}player/{player}")
    r.raise_for_status()
    yield r.json()

# pipes the data from players into player profile to produce a list of player profiles
list(players("GM") | player_profile)
```
  

**Arguments**:

- `f` - (Callable): a function taking minimum one argument of TDataItems type which will receive data yielded from `data_from` resource.
  
- `data_from` _Callable | Any, optional_ - a resource that will send data to the decorated function `f`
  
- `name` _str, optional_ - A name of the resource that by default also becomes the name of the table to which the data is loaded.
  If not present, the name of the decorated function will be used.
  
- `table_name` _TTableHintTemplate[str], optional_ - An table name, if different from `name`.
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  
- `max_table_nesting` _int, optional_ - A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.
  
- `write_disposition` _Literal["skip", "append", "replace", "merge"], optional_ - Controls how to write data to a table. `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  
- `columns` _Sequence[TAnySchemaColumns], optional_ - A list, dict or pydantic model of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  
- `primary_key` _str | Sequence[str]_ - A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  
- `merge_key` _str | Sequence[str]_ - A column name or a list of column names that define a merge key. Typically used with "merge" write disposition to remove overlapping data ranges ie. to keep a single record for a given day.
  This argument also accepts a callable that is used to dynamically create tables for stream-like resources yielding many datatypes.
  
- `schema_contract` _TSchemaContract, optional_ - Schema contract settings that will be applied to all resources of this source (if not overridden in the resource itself)
  
- `table_format` _Literal["iceberg", "delta"], optional_ - Defines the storage format of the table. Currently only "iceberg" is supported on Athena, and "delta" on the filesystem.
  Other destinations ignore this hint.
  
- `file_format` _Literal["preferred", ...], optional_ - Format of the file in which resource data is stored. Useful when importing external files. Use `preferred` to force
  a file format that is preferred by the destination used. This setting superseded the `load_file_format` passed to pipeline `run` method.
  
- `selected` _bool, optional_ - When `True` `dlt pipeline` will extract and load this resource, if `False`, the resource will be ignored.
  
- `spec` _Type[BaseConfiguration], optional_ - A specification of configuration and secret values required by the source.
  
- `standalone` _bool, optional_ - Returns a wrapped decorated function that creates DltResource instance. Must be called before use. Cannot be part of a source.
  
- `_impl_cls` _Type[TDltResourceImpl], optional_ - A custom implementation of DltResource, may be also used to providing just a typing stub

## get\_source\_schema

```python
def get_source_schema() -> Schema
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/decorators.py#L812)

When executed from the function decorated with @dlt.source, returns a writable source Schema

## get\_source

```python
def get_source() -> DltSource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/extract/decorators.py#L820)

When executed from the function decorated with @dlt.resource, returns currently extracted source

