---
sidebar_label: capabilities
title: common.destination.capabilities
---

## LoaderFileFormatSelector Objects

```python
class LoaderFileFormatSelector(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L44)

Selects preferred and supported file formats for a given table schema

## MergeStrategySelector Objects

```python
class MergeStrategySelector(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L57)

Selects right set of merge strategies for a given table schema

## DataTypeMapper Objects

```python
class DataTypeMapper(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L69)

### \_\_init\_\_

```python
def __init__(capabilities: "DestinationCapabilitiesContext") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L70)

Maps dlt data types into destination data types

### to\_destination\_type

```python
@abstractmethod
def to_destination_type(column: TColumnSchema,
                        table: PreparedTableSchema) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L75)

Gets destination data type for a particular `column` in prepared `table`

### from\_destination\_type

```python
@abstractmethod
def from_destination_type(db_type: str, precision: Optional[int],
                          scale: Optional[int]) -> TColumnType
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L80)

Gets column type from db type

### ensure\_supported\_type

```python
@abstractmethod
def ensure_supported_type(column: TColumnSchema, table: PreparedTableSchema,
                          loader_file_format: TLoaderFileFormat) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L87)

Makes sure that dlt type in `column` in prepared `table`  is supported by the destination for a given file format

## UnsupportedTypeMapper Objects

```python
class UnsupportedTypeMapper(DataTypeMapper)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L97)

Type Mapper that can't map any type

## DestinationCapabilitiesContext Objects

```python
@configspec
class DestinationCapabilitiesContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L120)

Injectable destination capabilities required for many Pipeline stages ie. normalize

### loader\_file\_format\_selector

Callable that adapts `preferred_loader_file_format` and `supported_loader_file_formats` at runtime.

### recommended\_file\_size

Recommended file size in bytes when writing extract/load files

### escape\_identifier

Escapes table name, column name and other identifiers

### escape\_literal

Escapes string literal

### casefold\_identifier

Casing function applied by destination to represent case insensitive identifiers.

### has\_case\_sensitive\_identifiers

Tells if destination supports case sensitive identifiers

### supports\_clone\_table

Destination supports CREATE TABLE ... CLONE ... statements

### max\_table\_nesting

Allows a destination to overwrite max_table_nesting from source

### max\_parallel\_load\_jobs

The destination can set the maximum amount of parallel load jobs being executed

### loader\_parallelism\_strategy

The destination can override the parallelism strategy

### max\_query\_parameters

The maximum number of parameters that can be supplied in a single parametrized query

### supports\_native\_boolean

The destination supports a native boolean type, otherwise bool columns are usually stored as integers

### generates\_case\_sensitive\_identifiers

```python
def generates_case_sensitive_identifiers() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L186)

Tells if capabilities as currently adjusted, will generate case sensitive identifiers

## merge\_caps\_file\_formats

```python
def merge_caps_file_formats(
    destination: str, staging: str, dest_caps: DestinationCapabilitiesContext,
    stage_caps: DestinationCapabilitiesContext
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/capabilities.py#L234)

Merges preferred and supported file formats from destination and staging.
Returns new preferred file format and all possible formats.

