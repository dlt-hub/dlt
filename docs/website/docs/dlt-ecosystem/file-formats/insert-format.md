---
title: INSERT
description: The INSERT file format
keywords: [insert_values, file-formats]
---

# SQL INSERT file format
This file format contains an INSERT...VALUES statement to be executed on the destination during the `load` stage.

Additional data types are stored as follows:
* `datetime` and `date` as ISO strings
* `decimal` as text representation of decimal number
* `binary` depends on the format accepted by the destination
* `complex` depends on the format accepted by the destination

This file format is [compressed](../../reference/performance.md#disabling-and-enabling-file-compression) by default.

## Supported destinations
Used by default by: **duckdb**, **postgres**, **Redshift**

Supported by: **filesystem**

By setting the `loader_file_format` argument to `insert_values` in the run command, the pipeline will store your data in the INSERT format to the destination:

```python
info = pipeline.run(some_source(), loader_file_format="insert_values")
