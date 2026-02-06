---
name: Identifier Normalization Check
description: Ensures all table names, column names, and other identifiers pass through the schema naming convention normalizer — the single most common PR review finding in this repo.
---

# Identifier Normalization Check

## Context

This is the **#1 most frequently caught issue** in dlt PR reviews. Different destinations have incompatible naming rules (e.g., Athena/S3 Tables forbids leading underscores, Weaviate capitalizes identifiers, BigQuery has length limits). All identifiers must go through the schema's naming normalizer — raw string constants are never acceptable for table or column names in runtime code.

Reviewer quote: "we should not use raw identifiers — all of them should be normalized via `dataset.schema.naming`"

## What to Check

### 1. Raw String Identifiers

Look for hardcoded table or column name strings used in SQL queries, schema lookups, or API calls without normalization:

```python
# BAD — raw string identifier
table_name = "my_table"
sql = f"SELECT * FROM {table_name}"

# GOOD — normalized through schema naming
table_name = schema.naming.normalize_identifier("my_table")
sql = f"SELECT * FROM {sql_client.make_qualified_table_name(table_name)}"
```

### 2. Dataset/Relation Layer

Code in `dlt/dataset/` that builds queries or accesses tables must normalize identifiers:

```python
# BAD
dataset["_dlt_loads"]

# GOOD
dataset[schema.naming.normalize_identifier("_dlt_loads")]
```

### 3. Destination Implementations

In `dlt/destinations/impl/*/`:

- Table names passed to SQL must use `sql_client.make_qualified_table_name()`
- Column names in SQL must be escaped/normalized
- Internal dlt tables (like `_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state`) must still be normalized

### 4. Double Normalization

The opposite mistake also occurs — normalizing an identifier that was already normalized earlier in the call chain. This can cause issues with naming conventions that aren't idempotent. Check that normalization happens exactly once.

### 5. Adapter Functions

In `*_adapter.py` files, column hints (partition, sort, cluster) reference column names that will be normalized later. Ensure hint column names are handled consistently with the normalization pipeline.

## Key Files

- `dlt/common/normalizers/naming/naming.py` — `NamingConvention` base class
- `dlt/common/schema/schema.py` — Schema with `naming` property
- `dlt/destinations/sql_client.py` — `make_qualified_table_name()`
- `dlt/dataset/` — Dataset/Relation query building
