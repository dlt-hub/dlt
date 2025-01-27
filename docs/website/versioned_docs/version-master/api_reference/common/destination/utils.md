---
sidebar_label: utils
title: common.destination.utils
---

## verify\_schema\_capabilities

```python
def verify_schema_capabilities(schema: Schema,
                               capabilities: DestinationCapabilitiesContext,
                               destination_type: str,
                               warnings: bool = True) -> List[Exception]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/utils.py#L23)

Verifies `load_tables` that have all hints filled by job client before loading against capabilities.
Returns a list of exceptions representing critical problems with the schema.
It will log warnings by default. It is up to the caller to eventually raise exception

* Checks all table and column name lengths against destination capabilities and raises on too long identifiers
* Checks if schema has collisions due to case sensitivity of the identifiers

## column\_type\_to\_str

```python
def column_type_to_str(column: TColumnType) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/utils.py#L119)

Converts column type to db-like type string

## resolve\_merge\_strategy

```python
@with_config
def resolve_merge_strategy(
    tables: TSchemaTables,
    table: TTableSchema,
    destination_capabilities: Optional[
        DestinationCapabilitiesContext] = ConfigValue
) -> Optional[TLoaderMergeStrategy]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/destination/utils.py#L194)

Resolve merge strategy for a table, possibly resolving the 'x-merge-strategy from a table chain. strategies selector in `destination_capabilities`
is used if present. If `table` does not contain strategy hint, a default value will be used which is the first.

`destination_capabilities` are injected from context if not explicitly passed.

Returns None if table write disposition is not merge

