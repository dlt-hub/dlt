---
name: Identifier Normalization Check
description: Ensures all table names, column names, and other identifiers pass through the schema naming convention normalizer — the single most common PR review finding in this repo.
---

# Identifier Normalization Check

## Context

Different destinations have incompatible naming rules (e.g., Athena/S3 Tables forbids leading underscores, Weaviate capitalizes identifiers, BigQuery has length limits). All identifiers must go through the schema's naming normalizer — raw string constants are never acceptable for table or column names in runtime code.

## When to Check

Those rules apply to INTERNAL `dlt` code that is intended to work on ANY destination. In that case raw identifiers (string literals) MUST be normalized.


You must known when you normalize RAW identifier that represents a table or column name vs. when you RE-NORMALIZE already normalized identifier - take it from `Schema` instance, `Dataset` or `Relation` and which comes in a Python variable.

Note: nested tables `user__comments` and nested columns `issue__stat_count` are PATHS containing many ALREADY NORMALIZED identifiers.

When re-normalizing (and WHEN IN DOUBT)
Use 
- `normalize_tables_path`
- `normalize_path`

When normalizing a RAW identifier:

Use 
- `normalize_table_identifier`
- `normalize_identifier`

## What to Check

### 1. Raw String Identifiers

Look for hardcoded table or column name strings used in SQL queries, schema lookups, or API calls without normalization:

```python
# BAD — raw string identifier
table_name = "my_table"
sql = f"SELECT * FROM {table_name}"

# GOOD — normalized through schema naming
table_name = schema.naming.normalize_table_identifier("my_table")
sql = f"SELECT * FROM {sql_client.make_qualified_table_name(table_name)}"
```

Code in `dlt/dataset/` that builds queries or accesses tables must normalize identifiers:

```python
# BAD
dataset["_dlt_loads"]

# GOOD
dataset[schema.naming.normalize_table_identifier("_dlt_loads")]
```

In `dlt/destinations/impl/*/`:

- Table names passed to SQL must use `sql_client.make_qualified_table_name()`
- Column names in SQL must be escaped/normalized
- Internal dlt tables (like `_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state`) must still be normalized

### 2. Re-normalizing identifiers
Use this path when in doubt!
When re-normalizing identifiers (ie. naming convention changed) or when identifier is a variable you MUST use:
* `normalize_tables_path` - for table identifiers
* `normalize_path` - for anything else ie. columns

Example: a pyarrow table in transfer from destination 1 (now a source) to destion 2. Columns are normalized per
destination 1 and we re-normalize for destination 2:

```python
def get_normalized_arrow_fields_mapping(schema: pyarrow.Schema, naming: NamingConvention) -> StrStr:
    """Normalizes schema field names and returns mapping from original to normalized name. Raises on name collisions"""
    # use normalize_path to be compatible with how regular columns are normalized in dlt.Schema
    norm_f = naming.normalize_path
```