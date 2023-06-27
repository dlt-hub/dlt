---
title: jsonl
description: The jsonl file format
keywords: [jsonl, file formats]
---

# jsonl - JSON delimited

`JSON delimited` is a file format that stores several `JSON` documents in one file. The `JSON`
documents are separated by a new line.

Additional data types are stored as follows:

- `datetime` and `date` as ISO strings;
- `decimal` as text representation of decimal number;
- `binary` is base64 encoded string;
- `HexBytes` is hex encoded string;
- `complex` is serialized as a string;

This file format is
[compressed](../../reference/performance.md#disabling-and-enabling-file-compression) by default.

## Supported destinations

Used by default by: **BigQuery**, **Snowflake**, **filesystem**.

By setting the `loader_file_format` argument to `jsonl` in the run command, the pipeline will store
your data in the jsonl format to the destination:

```python
info = pipeline.run(some_source(), loader_file_format="jsonl")
```
