---
sidebar_label: capabilities
title: common.destination.capabilities
---

## LoaderFileFormatAdapter Objects

```python
class LoaderFileFormatAdapter(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/capabilities.py#L31)

Callback protocol for `loader_file_format_adapter` capability.

## DestinationCapabilitiesContext Objects

```python
@configspec
class DestinationCapabilitiesContext(ContainerInjectableContext)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/capabilities.py#L45)

Injectable destination capabilities required for many Pipeline stages ie. normalize

### loader\_file\_format\_adapter

Callable that adapts `preferred_loader_file_format` and `supported_loader_file_formats` at runtime.

### supported\_table\_formats

type: ignore[name-defined] # noqa: F821

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

### supported\_merge\_strategies

type: ignore[name-defined] # noqa: F821

### max\_parallel\_load\_jobs

The destination can set the maxium amount of parallel load jobs being executed

### loader\_parallelism\_strategy

The destination can override the parallelism strategy

### generates\_case\_sensitive\_identifiers

```python
def generates_case_sensitive_identifiers() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/capabilities.py#L103)

Tells if capabilities as currently adjusted, will generate case sensitive identifiers

## merge\_caps\_file\_formats

```python
def merge_caps_file_formats(
    destination: str, staging: str, dest_caps: DestinationCapabilitiesContext,
    stage_caps: DestinationCapabilitiesContext
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/capabilities.py#L146)

Merges preferred and supported file formats from destination and staging.
Returns new preferred file format and all possible formats.

