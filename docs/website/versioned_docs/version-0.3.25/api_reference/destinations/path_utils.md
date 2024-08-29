---
sidebar_label: path_utils
title: destinations.path_utils
---

## create\_path

```python
def create_path(layout: str, schema_name: str, table_name: str, load_id: str,
                file_id: str, ext: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/path_utils.py#L27)

create a filepath from the layout and our default params

## get\_table\_prefix\_layout

```python
def get_table_prefix_layout(
    layout: str,
    supported_prefix_placeholders: Sequence[
        str] = SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS
) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/path_utils.py#L46)

get layout fragment that defines positions of the table, cutting other placeholders

allowed `supported_prefix_placeholders` that may appear before table.

