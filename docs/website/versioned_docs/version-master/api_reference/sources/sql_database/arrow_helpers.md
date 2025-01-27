---
sidebar_label: arrow_helpers
title: sources.sql_database.arrow_helpers
---

## row\_tuples\_to\_arrow

```python
@with_config
def row_tuples_to_arrow(rows: Sequence[Any],
                        caps: DestinationCapabilitiesContext = None,
                        columns: TTableSchemaColumns = None,
                        tz: str = None) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/sql_database/arrow_helpers.py#L10)

Converts `column_schema` to arrow schema using `caps` and `tz`. `caps` are injected from the container - which
is always the case if run within the pipeline. This will generate arrow schema compatible with the destination.
Otherwise generic capabilities are used

