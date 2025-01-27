---
sidebar_label: path_utils
title: destinations.path_utils
---

## normalize\_path\_sep

```python
def normalize_path_sep(pathlib: Any, path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/path_utils.py#L78)

Normalizes path in `path` separator to one used by `pathlib`

## check\_layout

```python
def check_layout(
    layout: str,
    extra_placeholders: Optional[Dict[str, Any]] = None,
    standard_placeholders: Optional[Set[str]] = STANDARD_PLACEHOLDERS
) -> Tuple[List[str], List[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/path_utils.py#L176)

Returns a tuple with all valid placeholders and the list of layout placeholders

Raises: InvalidFilesystemLayout

Returns: a pair of lists of valid placeholders and layout placeholders

## create\_path

```python
def create_path(layout: str,
                file_name: str,
                schema_name: str,
                load_id: str,
                load_package_timestamp: Optional[pendulum.DateTime] = None,
                current_datetime: Optional[TCurrentDateTime] = None,
                extra_placeholders: Optional[Dict[str, Any]] = None) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/path_utils.py#L212)

create a filepath from the layout and our default params

## get\_table\_prefix\_layout

```python
def get_table_prefix_layout(
        layout: str,
        supported_prefix_placeholders: Sequence[
            str] = SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS,
        table_needs_own_folder: bool = False) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/path_utils.py#L248)

get layout fragment that defines positions of the table, cutting other placeholders
allowed `supported_prefix_placeholders` that may appear before table.

