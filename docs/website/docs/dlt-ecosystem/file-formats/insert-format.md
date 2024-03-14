---
title: INSERT
description: The INSERT file format
keywords: [insert values, file formats]
---

# SQL INSERT File Format

This file format contains an INSERT...VALUES statement to be executed on the destination during the `load` stage.

Additional data types are stored as follows:

- `datetime` and `date` are stored as ISO strings;
- `decimal` is stored as a text representation of a decimal number;
- `binary` storage depends on the format accepted by the destination;
- `complex` storage also depends on the format accepted by the destination.

This file format is [compressed](../../reference/performance.md#disabling-and-enabling-file-compression) by default.

## Supported Destinations

This format is used by default by: **DuckDB**, **Postgres**, **Redshift**.

It is also supported by: **filesystem**.

By setting the `loader_file_format` argument to `insert_values` in the run command, the pipeline will store your data in the INSERT format at the destination:

```py
info = pipeline.run(some_source(), loader_file_format="insert_values")
```
