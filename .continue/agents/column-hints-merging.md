---
name: Column Hints Merging Check
description: Ensures adapter functions merge column hints (partition, sort, cluster, codec) correctly instead of overwriting them, and that repeated hint-setting patterns are extracted into helpers.
---

# Column Hints Merging Check

## Context

dlt adapters (`*_adapter.py`) set column-level hints like partition, sort, cluster, and codec. A frequently caught bug is **overwriting hints instead of merging them** — when a column has both a partition hint and a sort hint, naive dict assignment clobbers one.

Reviewer quote: "this pattern of setting column hints repeats 4 times. Let's extract this pattern into a helper function"

## What to Check

### 1. Hint Overwriting

Look for patterns where column hints are set by replacing the entire dict entry:

```python
# BAD — overwrites existing hints on the column
columns[column_name] = {"partition": True}
# If column_name already had {"sort": True}, that's now lost

# GOOD — merges hints
columns.setdefault(column_name, {}).update({"partition": True})
# Or
if column_name not in columns:
    columns[column_name] = {}
columns[column_name]["partition"] = True
```

### 2. Multiple Hints on Same Column

Test that a single column can have multiple hints simultaneously:

```python
# Should work: same column is both partitioned and sorted
adapter(table, partition=["col1"], sort=["col1"])
# col1 should have BOTH partition=True AND sort=True
```

### 3. Unbound Column Detection

When hints reference columns that don't appear in the data:

- "Unbound" columns should be created for sort/partition/codec hints (like how other destinations do it)
- These unbound columns are detected during normalize, providing early warnings
- Set correct nullability as required by the destination

### 4. Column Order for Hints

Most destinations use column order to determine partition/sort order. When multiple columns are specified for a hint, their order should be preserved and respected:

```python
# The order matters: partition by col1 first, then col2
adapter(table, partition=["col1", "col2"])
```

### 5. Repeated Patterns

If the PR contains the same hint-setting logic repeated multiple times, it should be extracted into a helper function (likely in a shared utils module).

## Key Files

- `dlt/destinations/impl/*/adapter.py` — Per-destination adapter functions
- `dlt/destinations/utils.py` — Shared destination utilities
- `dlt/common/schema/typing.py` — Column hint type definitions
